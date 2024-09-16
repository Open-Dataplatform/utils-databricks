import json
import pyspark.sql.functions as F
from typing import List, Tuple, Dict
from pyspark.sql.types import ArrayType, StructType, StringType
from pyspark.sql import SparkSession, DataFrame
from custom_utils.dp_storage import reader, writer
from custom_utils.logging.logger import Logger
from custom_utils.config.config import Config

class DataFrameTransformer:
    def __init__(self, logger: Logger = None, debug: bool = False):
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug

    def _log_message(self, message: str, level="info"):
        """Log a message using the logger."""
        if self.debug or level != "info":
            self.logger.log_message(message, level=level)

    def _log_block(self, header, content_lines, level="info"):
        """
        Utility method to log blocks of messages with a header and separators.

        Args:
            header (str): Header of the block.
            content_lines (list): List of lines to include in the block.
            level (str): Log level for the block.
        """
        self.logger.log_message(f"\n=== {header} ===", level=level, single_info_prefix=True)
        print("------------------------------")
        for line in content_lines:
            self.log_message(line, level=level)

    def flatten_array_column(self, df: DataFrame, column_name: str) -> DataFrame:
        """Flatten an array column in the DataFrame."""
        try:
            return df.withColumn(column_name, F.explode_outer(F.col(column_name)))
        except Exception as e:
            self._log_message(f"Error flattening array column '{column_name}': {e}", level="error")
            raise

    def flatten_struct_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """Flatten a struct column in the DataFrame."""
        try:
            expanded_columns = self._get_expanded_columns_with_aliases(df, column_name, layer_separator)
            return df.select("*", *expanded_columns).drop(column_name)
        except Exception as e:
            self._log_message(f"Error flattening struct column '{column_name}': {e}", level="error")
            raise

    def _get_expanded_columns_with_aliases(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.col]:
        """Return a list of nested columns in a struct to be used in df.select()."""
        expanded_columns = []
        for nested_column in df.select(f"`{column_name}`.*").columns:
            expanded_column = f"`{column_name}`.`{nested_column}`"
            expanded_column_alias = f"{column_name}{layer_separator}{nested_column}"
            expanded_columns.append(F.col(expanded_column).alias(expanded_column_alias))
        return expanded_columns

    def flatten_df(self, df: DataFrame, depth_level: int, max_depth: int, type_mapping: Dict[str, str] = None) -> DataFrame:
        """Flatten complex fields in a DataFrame up to a specified depth level."""
        try:
            current_level = 0

            # Apply custom type mappings if provided
            if type_mapping:
                df = df.select(
                    [
                        F.col(c).cast(type_mapping.get(df.schema[c].dataType.simpleString(), df.schema[c].dataType))
                        for c in df.columns
                    ]
                )

            # Flatten complex fields in DataFrame
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
                            df = self.flatten_array_column(df, col_name)
                        elif isinstance(data_type, StructType):
                            df = self.flatten_struct_column(df, col_name)

                current_level += 1

            # Convert remaining complex fields to strings if the depth is reached
            if current_level >= depth_level:
                df = df.select(
                    [
                        F.col(c).cast(StringType()) if isinstance(df.schema[c].dataType, (ArrayType, StructType)) else F.col(c)
                        for c in df.columns
                    ]
                )

            # Rename columns to replace '__' and '.' with '_'
            df = self.rename_columns(df, replacements={"__": "_", ".": "_"})

            return df
        except Exception as e:
            self._log_message(f"Error flattening DataFrame: {e}", level="error")
            raise

    def rename_columns(self, df: DataFrame, replacements: Dict[str, str]) -> DataFrame:
        """Rename DataFrame columns based on a dictionary of replacement rules."""
        for column_name in df.columns:
            new_name = self._string_replace(column_name, replacements)
            df = df.withColumnRenamed(column_name, new_name)
        return df

    def _string_replace(self, s: str, replacements: Dict[str, str]) -> str:
        """Helper function to perform multiple string replacements."""
        for string_before, string_after in replacements.items():
            s = s.replace(string_before, string_after)
        return s
    
    def read_json_from_binary(spark: SparkSession, schema: StructType, data_file_path: str) -> DataFrame:
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
            raise RuntimeError(f"Error processing binary JSON files: {e}")

    def _format_schema(schema, indent_level=0):
        """Helper function to format schema with proper indentation."""
        formatted_schema = ""
        indent = " " * (indent_level * 2)
        for field in schema.fields:
            field_type = field.dataType
            if isinstance(field_type, StructType):
                formatted_schema += f"{indent}|-- {field.name}: struct (nullable = {field.nullable})\n"
                formatted_schema += _format_schema(field_type, indent_level + 1)
            elif isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType):
                formatted_schema += f"{indent}|-- {field.name}: array (nullable = {field.nullable})\n"
                formatted_schema += f"{indent}    |-- element: struct (containsNull = {field_type.containsNull})\n"
                formatted_schema += _format_schema(field_type.elementType, indent_level + 2)
            elif isinstance(field_type, ArrayType):
                formatted_schema += f"{indent}|-- {field.name}: array<{field_type.elementType.simpleString()}> (nullable = {field.nullable})\n"
            else:
                formatted_schema += f"{indent}|-- {field.name}: {field_type.simpleString()} (nullable = {field.nullable})\n"
        return formatted_schema

    def process_and_flatten_json(
        self,
        schema_file_path: str,
        data_file_path: str,
        logger: Logger = None,
        depth_level: int = None,
        debug: bool = False,
        include_schema: bool = False
    ) -> Tuple[DataFrame, DataFrame]:
        """Orchestrates the JSON processing pipeline from schema reading to DataFrame flattening."""
        transformer = DataFrameTransformer(logger=logger, debug=debug)

        try:
            # Get the active Spark session
            spark = SparkSession.builder.getOrCreate()

            # Reading schema and parsing JSON to Spark StructType
            schema_json, schema = writer.json_schema_to_spark_struct(schema_file_path)

            if logger:
                transformer._log_message(f"Schema file path: {schema_file_path}", level="info")
                transformer._log_message(f"Data file path: {data_file_path}", level="info")

                # Only log the schema JSON if include_schema is True
                if include_schema:
                    transformer._log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

            # Read the JSON data
            df = read_json_from_binary(spark, schema, data_file_path)

            if logger:
                # Log initial DataFrame info block
                transformer._log_block("Initial DataFrame Info", [])
                
                # Format and log the schema
                initial_schema_str = _format_schema(df.schema)
                transformer._log_message(f"Initial DataFrame schema:\nroot\n{initial_schema_str}", level="info")

                initial_row_count = df.count()
                transformer._log_message(f"Initial DataFrame row count: {initial_row_count}", level="info")

            # Determine the depth level to use
            depth_level_to_use = depth_level if depth_level is not None else config.depth_level

            # Get the max depth and flatten the DataFrame
            max_depth = reader.get_json_depth(schema_json, logger=logger if debug else None, depth_level=depth_level_to_use)
            df_flattened = transformer.flatten_df(df, depth_level=depth_level_to_use, max_depth=max_depth, type_mapping=reader.get_type_mapping())

            if logger:
                # Log flattened DataFrame info block
                transformer._log_block("Flattened DataFrame Info", [])
                
                # Format and log the flattened schema
                flattened_schema_str = _format_schema(df_flattened.schema)
                transformer._log_message(f"Flattened DataFrame schema:\nroot\n{flattened_schema_str}", level="info")

                flattened_row_count = df_flattened.count()
                transformer._log_message(f"Flattened DataFrame row count: {flattened_row_count}", level="info")

            # Drop the "input_file_name" column from the original DataFrame
            df = df.drop("input_file_name")

            transformer._log_message("Completed JSON processing and flattening.", level="info")

            return df, df_flattened

        except Exception as e:
            if logger:
                transformer._log_message(f"Error during processing and flattening: {str(e)}", level="error")
            raise