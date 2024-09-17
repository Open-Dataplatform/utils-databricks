# File: custom_utils/transformations/dataframe.py

import json
import pyspark.sql.functions as F
from typing import Dict, Tuple, List
from pyspark.sql.types import ArrayType, StructType, StringType, StructField, IntegerType, BooleanType, DoubleType
from pyspark.sql import DataFrame
from custom_utils.config.config import Config
from custom_utils.logging.logger import Logger

class Transformer:
    def __init__(self, config: Config, logger: Logger = None, debug: bool = None):
        """
        Initializes the Transformer.

        Args:
            config (Config): An instance of the Config class containing configuration parameters.
            logger (Logger, optional): An instance of the Logger class for logging. Defaults to None.
            debug (bool, optional): If set to True, overrides config's debug setting. Defaults to None.
        """
        self.config = config
        self.logger = logger if logger else config.logger
        self.debug = debug if debug is not None else config.debug
        self.logger.debug = self.debug

    def _json_schema_to_spark_struct(self, schema_file_path: str) -> Tuple[dict, StructType]:
        """
        Converts a JSON schema file to a PySpark StructType.

        Args:
            schema_file_path (str): The path to the JSON schema file.

        Returns:
            tuple: A tuple containing the original JSON schema as a dictionary and the corresponding PySpark StructType.

        Raises:
            ValueError: If the schema file cannot be loaded or parsed.
        """
        try:
            with open(schema_file_path, "r") as f:
                json_schema = json.load(f)
        except Exception as e:
            raise ValueError(f"Failed to load or parse schema file '{schema_file_path}': {e}")

        def parse_type(field_props):
            if isinstance(field_props, list):
                valid_types = [parse_type(fp) for fp in field_props if fp.get("type") != "null"]
                return valid_types[0] if valid_types else StringType()

            json_type = field_props.get("type")
            if isinstance(json_type, list):
                json_type = next((t for t in json_type if t != "null"), None)

            if json_type == "string":
                return StringType()
            elif json_type == "integer":
                return IntegerType()
            elif json_type == "boolean":
                return BooleanType()
            elif json_type == "number":
                return DoubleType()
            elif json_type == "array":
                items = field_props.get("items")
                return ArrayType(parse_type(items) if items else StringType())
            elif json_type == "object":
                properties = field_props.get("properties", {})
                return StructType([StructField(k, parse_type(v), True) for k, v in properties.items()])
            else:
                return StringType()

        def parse_properties(properties):
            return StructType([StructField(name, parse_type(props), True) for name, props in properties.items()])

        return json_schema, parse_properties(json_schema.get("properties", {}))

    def _get_json_depth(self, json_schema: dict, current_depth=0) -> int:
        """
        Recursively determines the maximum depth of a JSON schema.

        Args:
            json_schema (dict): A JSON schema represented as a dictionary.
            current_depth (int): The current depth level (used internally).

        Returns:
            int: The maximum depth level of the JSON schema.
        """
        def calculate_depth(schema, current_depth):
            if isinstance(schema, dict) and '$ref' in schema:
                return calculate_depth(definitions.get(schema['$ref'].split('/')[-1], {}), current_depth)
            
            max_depth = current_depth
            if 'properties' in schema:
                properties_depth = max(calculate_depth(v, current_depth + 1) for v in schema['properties'].values())
                max_depth = max(max_depth, properties_depth)
            if 'items' in schema:
                items_depth = calculate_depth(schema['items'], current_depth + 1)
                max_depth = max(max_depth, items_depth)

            return max_depth

        definitions = json_schema.get('definitions', {})
        return calculate_depth(json_schema, current_depth)

    def _read_json_from_binary(self, schema: StructType, data_file_path: str) -> DataFrame:
        """
        Reads JSON files as binary, parses the JSON content, and associates the `input_file_name`.

        Args:
            schema (StructType): The schema for parsing the JSON files.
            data_file_path (str): The path to the JSON data files.

        Returns:
            DataFrame: A DataFrame containing parsed JSON content and file metadata.

        Raises:
            RuntimeError: If the binary JSON reading process fails.
        """
        try:
            binary_df = self.config.spark.read.format("binaryFile").load(data_file_path)
            df_with_filename = (
                binary_df.withColumn("json_string", F.col("content").cast("string"))
                .withColumn("input_file_name", F.col("path"))
                .withColumn("id", F.monotonically_increasing_id())
            )

            df_parsed = (
                self.config.spark.read.schema(schema)
                .json(df_with_filename.select("json_string").rdd.map(lambda row: row.json_string))
                .withColumn("id", F.monotonically_increasing_id())
            )

            df_final_with_filename = df_parsed.join(df_with_filename, on="id", how="inner").drop(
                "json_string", "content", "path", "modificationTime", "length", "id"
            )

            columns = ["input_file_name"] + [col for col in df_final_with_filename.columns if col != "input_file_name"]
            return df_final_with_filename.select(columns)
        except Exception as e:
            self.logger.log_message(f"Error processing binary JSON files: {e}", level="error")
            raise RuntimeError(f"Error processing binary JSON files")

    def flatten_df(self, df: DataFrame, depth_level: int, max_depth: int, type_mapping: Dict[str, str] = None) -> DataFrame:
        """
        Flattens complex fields in a DataFrame up to a specified depth level.

        Args:
            df (DataFrame): The DataFrame to flatten.
            depth_level (int): The level of depth up to which flattening should occur.
            max_depth (int): The maximum depth of nested fields in the DataFrame.
            type_mapping (Dict[str, str], optional): Custom type mappings for columns.

        Returns:
            DataFrame: A new DataFrame with nested fields flattened.

        Raises:
            RuntimeError: If the flattening process fails.
        """
        try:
            if type_mapping:
                df = df.select(
                    [F.col(c).cast(type_mapping.get(df.schema[c].dataType.simpleString(), df.schema[c].dataType)) for c in df.columns]
                )

            current_level = 0
            while current_level < depth_level:
                complex_fields = {field.name: field.dataType for field in df.schema.fields if isinstance(field.dataType, (ArrayType, StructType))}

                if not complex_fields:
                    break

                for col_name, data_type in complex_fields.items():
                    if current_level + 1 == depth_level:
                        df = df.withColumn(col_name, F.to_json(F.col(col_name)))
                    else:
                        df = self._flatten_array_column(df, col_name) if isinstance(data_type, ArrayType) else self._flatten_struct_column(df, col_name)

                current_level += 1

            if current_level >= depth_level:
                df = df.select(
                    [F.col(c).cast(StringType()) if isinstance(df.schema[c].dataType, (ArrayType, StructType)) else F.col(c) for c in df.columns]
                )

            for column_name in df.columns:
                new_name = column_name.replace("__", "_").replace(".", "_")
                df = df.withColumnRenamed(column_name, new_name)

            return df
        except Exception as e:
            self.logger.log_message(f"Error flattening DataFrame: {e}", level="error")
            raise RuntimeError(f"Error flattening DataFrame")

    def process_and_flatten_json(self, schema_file_path: str, data_file_path: str, depth_level: int = None, include_schema: bool = False) -> Tuple[DataFrame, DataFrame]:
            """
            Processes and flattens a JSON file based on the provided schema.

            Args:
                schema_file_path (str): Path to the JSON schema file.
                data_file_path (str): Path to the JSON data file.
                depth_level (int, optional): Depth level for flattening. Defaults to None.
                include_schema (bool, optional): If True, includes the schema JSON in logs.

            Returns:
                Tuple[DataFrame, DataFrame]: The original and flattened DataFrames.

            Raises:
                RuntimeError: If processing fails.
            """
            self.logger.log_start("process_and_flatten_json")
            try:
                # Use the internal method to parse the schema file
                schema_json, schema = self._json_schema_to_spark_struct(schema_file_path)
                depth_level_to_use = depth_level if depth_level is not None else 1
                max_depth = self._get_json_depth(schema_json)

                # Log start of processing
                start_lines = [
                    f"Schema file path: {schema_file_path}",
                    f"Data file path: {data_file_path}",
                    f"Flattened depth level of the JSON file: {depth_level_to_use}"
                ]
                self.logger.log_block("Starting Processing", start_lines)

                if include_schema:
                    self.logger.log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

                # Read the JSON data
                df = self._read_json_from_binary(schema, data_file_path)

                if self.debug:
                    self.logger.log_block("Initial DataFrame Info", [])
                    initial_row_count = df.count()
                    self.logger.log_message(f"Initial DataFrame row count: {initial_row_count}", level="info")
                    # Use the built-in printSchema method to log the schema
                    schema_str = df._jdf.schema().treeString()  # Convert schema to a string
                    self.logger.log_message(f"Initial DataFrame schema:\n{schema_str}", level="info")

                # Flatten the DataFrame
                df_flattened = self.flatten_df(df, depth_level=depth_level_to_use, max_depth=max_depth)

                if self.debug:
                    self.logger.log_block("Flattened DataFrame Info", [])
                    flattened_row_count = df_flattened.count()
                    self.logger.log_message(f"Flattened DataFrame row count: {flattened_row_count}", level="info")
                    # Use the built-in printSchema method to log the flattened schema
                    schema_str_flattened = df_flattened._jdf.schema().treeString()  # Convert schema to a string
                    self.logger.log_message(f"Flattened DataFrame schema:\n{schema_str_flattened}", level="info")

                df = df.drop("input_file_name")
                self.logger.log_message("Completed JSON processing and flattening.", level="info")
                self.logger.log_end("process_and_flatten_json", success=True, additional_message="Proceeding with notebook execution.")
                return df, df_flattened

            except Exception as e:
                self.logger.log_message(f"Error during processing and flattening: {str(e)}", level="error")
                self.logger.log_end("process_and_flatten_json", success=False)
                raise RuntimeError(f"Error during processing and flattening: {e}")