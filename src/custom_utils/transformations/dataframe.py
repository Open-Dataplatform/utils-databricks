# File: custom_utils/transformations/dataframe.py

import json
import pyspark.sql.functions as F
from typing import Dict, Tuple, List
from pyspark.sql.types import ArrayType, StructType, StringType
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

    def flatten_array_column(self, df: DataFrame, column_name: str) -> DataFrame:
        try:
            return df.withColumn(column_name, F.explode_outer(F.col(column_name)))
        except Exception as e:
            self.logger.log_message(f"Error flattening array column '{column_name}': {e}", level="error")
            raise RuntimeError(f"Error flattening array column '{column_name}'")

    def flatten_struct_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        try:
            expanded_columns = self._get_expanded_columns_with_aliases(df, column_name, layer_separator)
            return df.select("*", *expanded_columns).drop(column_name)
        except Exception as e:
            self.logger.log_message(f"Error flattening struct column '{column_name}': {e}", level="error")
            raise RuntimeError(f"Error flattening struct column '{column_name}'")

    def _get_expanded_columns_with_aliases(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.Column]:
        return [
            F.col(f"`{column_name}`.`{nested_column}`").alias(f"{column_name}{layer_separator}{nested_column}")
            for nested_column in df.select(f"`{column_name}`.*").columns
        ]

    def flatten_df(self, df: DataFrame, depth_level: int, max_depth: int, type_mapping: Dict[str, str] = None) -> DataFrame:
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

    def read_json_from_binary(self, schema: StructType, data_file_path: str) -> DataFrame:
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

    def _format_schema(self, schema: StructType, indent_level: int = 0) -> str:
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

    def process_and_flatten_json(self, schema_file_path: str, data_file_path: str, depth_level: int = None, include_schema: bool = False) -> Tuple[DataFrame, DataFrame]:
        self.logger.log_start("process_and_flatten_json")
        try:
            schema_json, schema = writer.json_schema_to_spark_struct(schema_file_path)
            depth_level_to_use = depth_level if depth_level is not None else 1
            max_depth = reader.get_json_depth(schema_json, logger=None, depth_level=depth_level_to_use)

            start_lines = [
                f"Schema file path: {schema_file_path}",
                f"Data file path: {data_file_path}",
                f"Flattened depth level of the JSON file: {depth_level_to_use}"
            ]
            self.logger.log_block("Starting Processing", start_lines)

            if include_schema:
                self.logger.log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

            df = self.read_json_from_binary(schema, data_file_path)

            if self.debug:
                self.logger.log_block("Initial DataFrame Info", [])
                initial_row_count = df.count()
                self.logger.log_message(f"Initial DataFrame row count: {initial_row_count}", level="info")
                initial_schema_str = self._format_schema(df.schema)
                self.logger.log_message(f"Initial DataFrame schema:\nroot\n{initial_schema_str}", level="info")

            df_flattened = self.flatten_df(df, depth_level=depth_level_to_use, max_depth=max_depth, type_mapping=reader.get_type_mapping())

            if self.debug:
                self.logger.log_block("Flattened DataFrame Info", [])
                flattened_row_count = df_flattened.count()
                self.logger.log_message(f"Flattened DataFrame row count: {flattened_row_count}", level="info")
                flattened_schema_str = self._format_schema(df_flattened.schema)
                self.logger.log_message(f"Flattened DataFrame schema:\nroot\n{flattened_schema_str}", level="info")

            df = df.drop("input_file_name")

            self.logger.log_message("Completed JSON processing and flattening.", level="info")
            self.logger.log_end("process_and_flatten_json", success=True, additional_message="Proceeding with notebook execution.")
            return df, df_flattened

        except Exception as e:
            self.logger.log_message(f"Error during processing and flattening: {str(e)}", level="error")
            self.logger.log_end("process_and_flatten_json", success=False)
            raise RuntimeError(f"Error during processing and flattening: {e}")