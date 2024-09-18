# File: custom_utils/transformations/dataframe.py

import json
import pyspark.sql.functions as F
from typing import Dict, Tuple, List
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from pyspark.sql import SparkSession, DataFrame
from custom_utils.dp_storage import reader, writer
from custom_utils.logging.logger import Logger

class DataFrameTransformer:
    def __init__(self, logger: Logger = None, debug: bool = False):
        """
        Initializes the DataFrameTransformer with logging capabilities.

        Args:
            logger (Logger, optional): An instance of the Logger class for logging. Defaults to None.
            debug (bool): If set to True, enables detailed logging for debugging purposes. Defaults to False.
        """
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug

    def _get_array_and_struct_columns(self, df: DataFrame) -> List[Tuple[str, type]]:
        """
        Returns a list of columns in the DataFrame that are of type ArrayType or StructType.

        Args:
            df (DataFrame): The DataFrame to analyze.

        Returns:
            List[Tuple[str, type]]: A list of tuples containing column names and their data types.
        """
        complex_columns = []
        for field in df.schema.fields:
            data_type = type(field.dataType)
            if data_type in [ArrayType, StructType]:
                complex_columns.append((field.name, field.dataType))
        return complex_columns

    def _json_schema_to_spark_struct(self, schema_file_path: str, definitions: Dict = None) -> Tuple[dict, StructType]:
        """
        Converts a JSON schema file to a PySpark StructType.

        Args:
            schema_file_path (str): The path to the JSON schema file.
            definitions (dict, optional): The schema definitions. Defaults to None.

        Returns:
            tuple: A tuple containing the original JSON schema as a dictionary and the corresponding PySpark StructType.
        """
        try:
            with open(schema_file_path, "r") as f:
                json_schema = json.load(f)
        except Exception as e:
            raise ValueError(f"Failed to load or parse schema file '{schema_file_path}': {e}")

        if definitions is None:
            definitions = json_schema.get("definitions", {})

        def resolve_ref(ref):
            ref_path = ref.split("/")[-1]
            return definitions.get(ref_path, {})

        def parse_type(field_props):
            if isinstance(field_props, list):
                valid_types = [parse_type(fp) for fp in field_props if fp.get("type") != "null"]
                return valid_types[0] if valid_types else StringType()

            if "$ref" in field_props:
                field_props = resolve_ref(field_props["$ref"])

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

    def flatten_array_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """Flatten an array column in the DataFrame and handle nested structs within the array."""
        df = df.withColumn(column_name, F.explode_outer(F.col(column_name)))
        nested_columns = self._get_array_and_struct_columns(df)
        for nested_column_name, data_type in nested_columns:
            if nested_column_name == column_name and isinstance(data_type, StructType):
                df = self.flatten_struct_column(df, column_name, layer_separator)
        return df

    def flatten_struct_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """Flatten a struct column in the DataFrame, including nested structs recursively."""
        expanded_columns = self._get_expanded_columns_with_aliases(df, column_name, layer_separator)
        df = df.select("*", *expanded_columns).drop(column_name)
        complex_columns = self._get_array_and_struct_columns(df)
        for nested_column_name, data_type in complex_columns:
            if nested_column_name.startswith(f"{column_name}{layer_separator}") and isinstance(data_type, StructType):
                df = self.flatten_struct_column(df, nested_column_name, layer_separator)
        return df

    def _get_expanded_columns_with_aliases(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.Column]:
        """Return a list of nested columns in a struct to be used in df.select()."""
        expanded_columns = []
        for nested_column in df.select(f"`{column_name}`.*").columns:
            expanded_column = f"`{column_name}`.`{nested_column}`"
            expanded_column_alias = f"{column_name}{layer_separator}{nested_column}"
            expanded_columns.append(F.col(expanded_column).alias(expanded_column_alias))
        return expanded_columns
    
    def _format_schema(self, schema: StructType, indent_level: int = 0) -> str:
        """
        Formats a given schema to make it more readable.

        Args:
            schema (StructType): The schema to format.
            indent_level (int): The current indentation level for nested fields.

        Returns:
            str: The formatted schema as a string.
        """
        formatted_schema = ""
        indent = " " * (indent_level * 4)  # Adjust the indentation (4 spaces per level)

        for field in schema.fields:
            field_type = field.dataType
            if isinstance(field_type, StructType):
                formatted_schema += f"{indent}|-- {field.name}: struct (nullable = {field.nullable})\n"
                formatted_schema += self._format_schema(field_type, indent_level + 1)
            elif isinstance(field_type, ArrayType):
                element_type = field_type.elementType
                if isinstance(element_type, StructType):
                    formatted_schema += f"{indent}|-- {field.name}: array (nullable = {field.nullable})\n"
                    formatted_schema += f"{indent}    |-- element: struct (containsNull = {field_type.containsNull})\n"
                    formatted_schema += self._format_schema(element_type, indent_level + 2)
                else:
                    formatted_schema += f"{indent}|-- {field.name}: array<{element_type.simpleString()}> (nullable = {field.nullable})\n"
            else:
                formatted_schema += f"{indent}|-- {field.name}: {field_type.simpleString()} (nullable = {field.nullable})\n"

        return formatted_schema

    def flatten_df(self, df: DataFrame, depth_level: int, current_level: int = 0, max_depth: int = 1, type_mapping: Dict[str, str] = None) -> DataFrame:
        """Flatten complex fields in a DataFrame up to a specified depth level."""
        if type_mapping:
            df = df.select([
                F.col(c).cast(type_mapping.get(df.schema[c].dataType.simpleString(), df.schema[c].dataType))
                for c in df.columns
            ])

        while current_level < depth_level:
            complex_fields = {
                field.name: field.dataType
                for field in df.schema.fields
                if isinstance(field.dataType, (ArrayType, StructType))
            }

            if not complex_fields:
                break

            for col_name, data_type in complex_fields.items():
                if current_level + 1 == depth_level:
                    df = df.withColumn(col_name, F.to_json(F.col(col_name)))
                else:
                    if isinstance(data_type, ArrayType):
                        df = self.flatten_array_column(df, col_name, layer_separator="_")
                    elif isinstance(data_type, StructType):
                        df = self.flatten_struct_column(df, col_name, layer_separator="_")

            current_level += 1

        if current_level >= depth_level:
            df = df.select([
                F.col(c).cast(StringType()) if isinstance(df.schema[c].dataType, (ArrayType, StructType)) else F.col(c)
                for c in df.columns
            ])

        for column_name in df.columns:
            new_name = column_name.replace("__", "_").replace(".", "_")
            df = df.withColumnRenamed(column_name, new_name)

        return df

    def read_json_from_binary(self, spark: SparkSession, schema: StructType, data_file_path: str) -> DataFrame:
        """Reads files as binary, parses the JSON content, and associates the `input_file_name`."""
        try:
            binary_df = spark.read.format("binaryFile").load(data_file_path)
            df_with_filename = (
                binary_df.withColumn("json_string", F.col("content").cast("string"))
                .withColumn("input_file_name", F.col("path"))
                .withColumn("id", F.monotonically_increasing_id())
            )

            df_parsed = (
                spark.read.schema(schema)
                .json(df_with_filename.select("json_string").rdd.map(lambda row: row.json_string))
                .withColumn("id", F.monotonically_increasing_id())
            )

            df_final_with_filename = df_parsed.join(df_with_filename, on="id", how="inner")
            df_final_with_filename = df_final_with_filename.drop(
                "json_string", "content", "path", "modificationTime", "length", "id"
            )

            columns = ["input_file_name"] + [col for col in df_final_with_filename.columns if col != "input_file_name"]
            return df_final_with_filename.select(columns)
        except Exception as e:
            self.logger.log_message(f"Error processing binary JSON files: {e}", level="error")
            raise RuntimeError(f"Error processing binary JSON files")

    def process_and_flatten_json(self, schema_file_path: str, data_file_path: str, depth_level: int = None, include_schema: bool = False) -> Tuple[DataFrame, DataFrame]:
        """
        Orchestrates the JSON processing pipeline from schema reading to DataFrame flattening.

        Args:
            schema_file_path (str): Path to the schema file.
            data_file_path (str): Path to the JSON data file.
            depth_level (int, optional): The depth level to flatten the JSON. Defaults to None.
            include_schema (bool, optional): If True, includes the schema in the logs. Defaults to False.

        Returns:
            Tuple[DataFrame, DataFrame]: A tuple containing the original DataFrame and the flattened DataFrame.

        Raises:
            RuntimeError: If the process fails.
        """
        # Log the start of the process
        self.logger.log_start("process_and_flatten_json")
        
        try:
            # Get the active Spark session
            spark = SparkSession.builder.getOrCreate()

            # Reading schema and parsing JSON to Spark StructType
            schema_json, schema = self._json_schema_to_spark_struct(schema_file_path)

            # Determine the depth level to use
            depth_level_to_use = depth_level if depth_level is not None else 1

            # Get the max depth of the JSON schema
            max_depth = reader.get_json_depth(schema_json)

            # Block 1: Schema and file paths
            self.logger.log_block("Schema and File Paths", [
                f"Schema file path: {schema_file_path}",
                f"Data file path: {data_file_path}",
                f"Maximum depth level of the JSON schema: {max_depth}; Flattened depth level: {depth_level_to_use}"
            ])
            
            # Optionally log the schema JSON
            if include_schema:
                self.logger.log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

            # Read the JSON data
            df = self.read_json_from_binary(spark, schema, data_file_path)

            # Block 2: Initial DataFrame info
            initial_row_count = df.count()
            self.logger.log_block("Initial DataFrame Info", [
                f"Initial DataFrame row count: {initial_row_count}"
            ])
            
            # Log the schema using printSchema
            self.logger.log_message("Initial DataFrame schema:", level="info")
            df.printSchema()  # This will print the schema to the console
            
            # Flatten the DataFrame
            df_flattened = self.flatten_df(df, depth_level=depth_level_to_use, max_depth=max_depth, type_mapping=reader.get_type_mapping())

            # Block 3: Flattened DataFrame info
            flattened_row_count = df_flattened.count()
            self.logger.log_block("Flattened DataFrame Info", [
                f"Flattened DataFrame row count: {flattened_row_count}"
            ])
            
            # Log the flattened schema using printSchema
            self.logger.log_message("Flattened DataFrame schema:", level="info")
            df_flattened.printSchema()  # This will print the schema to the console

            # Drop the "input_file_name" column from the original DataFrame
            df = df.drop("input_file_name")

            # Log the end of the process with the additional message
            self.logger.log_end("process_and_flatten_json", success=True, additional_message="Proceeding with notebook execution.")
            
            return df, df_flattened

        except Exception as e:
            # Log the error message
            self.logger.log_message(f"Error during processing and flattening: {str(e)}", level="error")
            
            # Log the end of the process with failure
            self.logger.log_end("process_and_flatten_json", success=False, additional_message="Check error logs for details.")
            
            raise RuntimeError(f"Error during processing and flattening: {e}")