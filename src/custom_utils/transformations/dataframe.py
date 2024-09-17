# File: custom_utils/transformations/transformer.py

import json
import pyspark.sql.functions as F
from typing import Dict, Tuple, List
from pyspark.sql.types import ArrayType, StructType, StringType
from pyspark.sql import DataFrame
from custom_utils.dp_storage import reader, writer
from custom_utils.config.config import Config
from custom_utils.logging.logger import Logger

class Transformer:
    def __init__(self, config: Config, logger: Logger = None, debug: bool = None):
        """
        Initializes the Transformer.

        Args:
            config (Config): An instance of the Config class containing configuration parameters.
            logger (Logger, optional): An instance of Logger for logging. Defaults to None.
            debug (bool, optional): If set, overrides the debug setting from the config. Defaults to None.
        """
        self.config = config
        # Use logger from the config if not provided
        self.logger = logger if logger else config.logger
        # Use the provided debug value or fallback to config's debug
        self.debug = debug if debug is not None else config.debug
        # Update the logger's debug state
        self.logger.debug = self.debug

    def flatten_array_column(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Flattens an array column in the DataFrame using `explode_outer`.

        Args:
            df (DataFrame): The DataFrame containing the array column to be flattened.
            column_name (str): The name of the array column to flatten.

        Returns:
            DataFrame: A new DataFrame with the specified array column flattened.

        Raises:
            RuntimeError: If the flattening process fails.
        """
        try:
            return df.withColumn(column_name, F.explode_outer(F.col(column_name)))
        except Exception as e:
            self.logger.log_message(f"Error flattening array column '{column_name}': {e}", level="error")
            raise RuntimeError(f"Error flattening array column '{column_name}'")

    def flatten_struct_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """
        Flattens a struct column in the DataFrame by expanding its nested fields.

        Args:
            df (DataFrame): The DataFrame containing the struct column to be flattened.
            column_name (str): The name of the struct column to flatten.
            layer_separator (str): Separator to use when naming nested fields. Defaults to "_".

        Returns:
            DataFrame: A new DataFrame with the specified struct column flattened.

        Raises:
            RuntimeError: If the flattening process fails.
        """
        try:
            expanded_columns = self._get_expanded_columns_with_aliases(df, column_name, layer_separator)
            return df.select("*", *expanded_columns).drop(column_name)
        except Exception as e:
            self.logger.log_message(f"Error flattening struct column '{column_name}': {e}", level="error")
            raise RuntimeError(f"Error flattening struct column '{column_name}'")

    def _get_expanded_columns_with_aliases(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.Column]:
        """
        Generates a list of nested columns in a struct, with appropriate aliases for use in `df.select()`.

        Args:
            df (DataFrame): The DataFrame containing the struct column.
            column_name (str): The name of the struct column to expand.
            layer_separator (str): Separator to use when naming nested fields. Defaults to "_".

        Returns:
            List[F.Column]: A list of columns with aliases for flattened struct fields.
        """
        return [
            F.col(f"`{column_name}`.`{nested_column}`").alias(f"{column_name}{layer_separator}{nested_column}")
            for nested_column in df.select(f"`{column_name}`.*").columns
        ]

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
                        df = self.flatten_array_column(df, col_name) if isinstance(data_type, ArrayType) else self.flatten_struct_column(df, col_name)

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

    def read_json_from_binary(self, spark: SparkSession, schema: StructType, data_file_path: str) -> DataFrame:
        """
        Reads JSON files as binary, parses the JSON content, and associates the `input_file_name`.

        Args:
            spark (SparkSession): The active Spark session.
            schema (StructType): The schema for parsing the JSON files.
            data_file_path (str): The path to the JSON data files.

        Returns:
            DataFrame: A DataFrame containing parsed JSON content and file metadata.

        Raises:
            RuntimeError: If the binary JSON reading process fails.
        """
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

            df_final_with_filename = df_parsed.join(df_with_filename, on="id", how="inner").drop(
                "json_string", "content", "path", "modificationTime", "length", "id"
            )

            columns = ["input_file_name"] + [col for col in df_final_with_filename.columns if col != "input_file_name"]
            return df_final_with_filename.select(columns)
        except Exception as e:
            self.logger.log_message(f"Error processing binary JSON files: {e}", level="error")
            raise RuntimeError(f"Error processing binary JSON files")

    def _format_schema(self, schema: StructType, indent_level: int = 0) -> str:
        """
        Formats the schema of a DataFrame for logging.

        Args:
            schema (StructType): The schema to format.
            indent_level (int, optional): The indentation level for nested fields. Defaults to 0.

        Returns:
            str: A formatted string representing the schema.
        """
        formatted_schema = ""
        indent = " " * (indent_level * 2)
        for field in schema.fields:
            field_type = field.dataType
            if isinstance(field_type, StructType):
                formatted_schema += f"{indent}|-- {field.name}: struct (nullable = {field.nullable})\n"
                formatted_schema += self._format_schema(field_type, indent_level + 1)
            elif isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType):
                formatted_schema += f"{indent}|-- {field.name}: array (nullable = {field.nullable})\n"
                formatted_schema += f"{indent}    |-- element: struct (containsNull = {field_type.containsNull})\n"
                formatted_schema += self._format_schema(field_type.elementType, indent_level + 2)
            elif isinstance(field_type, ArrayType):
                formatted_schema += f"{indent}|-- {field.name}: array<{field_type.elementType.simpleString()}> (nullable = {field.nullable})\n"
            else:
                formatted_schema += f"{indent}|-- {field.name}: {field_type.simpleString()} (nullable = {field.nullable})\n"
        return formatted_schema

    def process_and_flatten_json(
        self,
        schema_file_path: str,
        data_file_path: str,
        depth_level: int = None,
        include_schema: bool = False
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Orchestrates the JSON processing pipeline, including schema reading, data flattening, and logging.

        Args:
            schema_file_path (str): The path to the schema file.
            data_file_path (str): The path to the JSON data file.
            depth_level (int, optional): The flattening depth level. Defaults to None.
            include_schema (bool, optional): If True, includes the schema JSON in logs. Defaults to False.

        Returns:
            Tuple[DataFrame, DataFrame]: The original DataFrame and the flattened DataFrame.

        Raises:
            RuntimeError: If the processing and flattening fail.
        """
        try:
            # Get the active Spark session from config
            spark = self.config.spark

            # Reading schema and parsing JSON to Spark StructType
            schema_json, schema = writer.json_schema_to_spark_struct(schema_file_path)

            # Determine the depth level to use
            depth_level_to_use = depth_level if depth_level is not None else 1

            # Get the maximum depth of the JSON schema
            max_depth = reader.get_json_depth(schema_json, logger=self.logger, depth_level=depth_level_to_use)

            # Log the start of the processing
            if self.debug:
                start_lines = [
                    f"Schema file path: {schema_file_path}",
                    f"Data file path: {data_file_path}",
                    f"Maximum depth level of the JSON schema: {max_depth}; Flattened depth level of the JSON file: {depth_level_to_use}"
                ]
                self.logger.log_block("Starting Processing", start_lines)

                if include_schema:
                    self.logger.log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

            # Read the JSON data
            df = self.read_json_from_binary(spark, schema, data_file_path)

            # Log initial DataFrame info
            if self.debug:
                self.logger.log_block("Initial DataFrame Info", [])
                initial_row_count = df.count()
                self.logger.log_message(f"Initial DataFrame row count: {initial_row_count}", level="info")
                initial_schema_str = self._format_schema(df.schema)
                self.logger.log_message(f"Initial DataFrame schema:\nroot\n{initial_schema_str}", level="info")

            # Flatten the DataFrame
            df_flattened = self.flatten_df(df, depth_level=depth_level_to_use, max_depth=max_depth, type_mapping=reader.get_type_mapping())

            # Log flattened DataFrame info
            if self.debug:
                self.logger.log_block("Flattened DataFrame Info", [])
                flattened_row_count = df_flattened.count()
                self.logger.log_message(f"Flattened DataFrame row count: {flattened_row_count}", level="info")
                flattened_schema_str = self._format_schema(df_flattened.schema)
                self.logger.log_message(f"Flattened DataFrame schema:\nroot\n{flattened_schema_str}", level="info")

            # Drop the "input_file_name" column from the original DataFrame
            df = df.drop("input_file_name")

            self.logger.log_message("Completed JSON processing and flattening.", level="info")
            return df, df_flattened

        except Exception as e:
            self.logger.log_message(f"Error during processing and flattening: {str(e)}", level="error")
            raise RuntimeError(f"Error during processing and flattening: {e}")