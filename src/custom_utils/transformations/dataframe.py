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

        # Simplified parsing to StructType
        def parse_type(field_props):
            json_type = field_props.get("type")
            if json_type == "string":
                return StringType()
            elif json_type == "array":
                items = field_props.get("items")
                return ArrayType(parse_type(items) if items else StringType())
            elif json_type == "object":
                properties = field_props.get("properties", {})
                return StructType([StructField(k, parse_type(v), True) for k, v in properties.items()])
            else:
                return StringType()  # Default to StringType for unsupported types

        def parse_properties(properties):
            return StructType([StructField(name, parse_type(props), True) for name, props in properties.items()])

        return json_schema, parse_properties(json_schema.get("properties", {}))

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
            if data_type == ArrayType or data_type == StructType:
                complex_columns.append((field.name, data_type))
        return complex_columns

    def _get_expanded_columns_with_aliases(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.Column]:
        """
        Expands nested columns in a struct column for use in DataFrame.select().

        Args:
            df (DataFrame): The DataFrame containing the struct column.
            column_name (str): The name of the struct column to expand.
            layer_separator (str): Separator to use when naming nested fields.

        Returns:
            List[F.Column]: A list of Columns representing the expanded fields.
        """
        expanded_columns = []
        for nested_column in df.select(f"`{column_name}`.*").columns:
            expanded_column = f"`{column_name}`.`{nested_column}`"
            expanded_column_alias = f"{column_name}{layer_separator}{nested_column}"
            expanded_columns.append(F.col(expanded_column).alias(expanded_column_alias))
        return expanded_columns

    def _flatten_array_column(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Flattens an array column in the DataFrame using `explode_outer`.

        Args:
            df (DataFrame): The DataFrame containing the array column.
            column_name (str): The name of the array column to flatten.

        Returns:
            DataFrame: A DataFrame with the specified array column flattened.
        """
        return df.withColumn(column_name, F.explode_outer(F.col(column_name)))

    def _flatten_struct_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """
        Flattens a struct column in the DataFrame by expanding its nested fields.

        Args:
            df (DataFrame): The DataFrame containing the struct column.
            column_name (str): The name of the struct column to flatten.
            layer_separator (str): Separator to use when naming nested fields.

        Returns:
            DataFrame: A DataFrame with the specified struct column flattened.
        """
        expanded_columns = self._get_expanded_columns_with_aliases(df, column_name, layer_separator)
        return df.select("*", *expanded_columns).drop(column_name)

    def flatten_df(self, df: DataFrame, depth_level: int = None, current_level: int = 0, max_depth: int = 1) -> DataFrame:
        """
        Flattens complex fields in a DataFrame up to a specified depth level.

        Args:
            df (DataFrame): A PySpark DataFrame containing nested structures.
            depth_level (int, optional): The maximum depth level to flatten. If None, max_depth is used.
            current_level (int): The current depth level (used internally during recursion).
            max_depth (int): The maximum depth calculated from the schema.

        Returns:
            DataFrame: A flattened DataFrame.
        """
        depth_level = max_depth if depth_level is None else depth_level

        if current_level >= depth_level:
            return df

        complex_columns = self._get_array_and_struct_columns(df)

        while complex_columns and current_level < depth_level:
            for column_name, data_type in complex_columns:
                if current_level + 1 == depth_level:
                    df = df.withColumn(column_name, F.to_json(F.col(column_name)))
                else:
                    if data_type == ArrayType:
                        df = self._flatten_array_column(df, column_name)
                    elif data_type == StructType:
                        df = self._flatten_struct_column(df, column_name)

            complex_columns = self._get_array_and_struct_columns(df)
            current_level += 1

        return df

    def process_and_flatten_json(self, schema_file_path: str, data_file_path: str, depth_level: int = None, include_schema: bool = False) -> Tuple[DataFrame, DataFrame]:
        """
        Orchestrates the JSON processing pipeline, including schema reading, data flattening, and logging.

        Args:
            schema_file_path (str): The path to the schema file.
            data_file_path (str): The path to the JSON data file.
            depth_level (int, optional): The flattening depth level. Defaults to None.
            include_schema (bool, optional): If True, includes the schema JSON in logs. Defaults to False.

        Returns:
            Tuple[DataFrame, DataFrame]: The original DataFrame and the flattened DataFrame.
        """
        self.logger.log_start("process_and_flatten_json")
        try:
            schema_json, schema = self._json_schema_to_spark_struct(schema_file_path)
            max_depth = self._get_json_depth(schema_json)
            depth_level_to_use = depth_level if depth_level is not None else max_depth

            self.logger.log_block("Starting Processing", [
                f"Schema file path: {schema_file_path}",
                f"Data file path: {data_file_path}",
                f"Maximum depth level of the JSON schema: {max_depth}",
                f"Flattened depth level of the JSON file: {depth_level_to_use}"
            ])

            if include_schema:
                self.logger.log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

            df = self._read_json_from_binary(schema, data_file_path)

            if self.debug:
                self.logger.log_block("Initial DataFrame Info", [])
                self.logger.log_message(f"Initial DataFrame row count: {df.count()}", level="info")
                self.logger.log_message("Initial DataFrame schema:", level="info")
                df.printSchema()

            df_flattened = self.flatten_df(df, depth_level=depth_level_to_use)

            if self.debug:
                self.logger.log_block("Flattened DataFrame Info", [])
                self.logger.log_message(f"Flattened DataFrame row count: {df_flattened.count()}", level="info")
                self.logger.log_message("Flattened DataFrame schema:", level="info")
                df_flattened.printSchema()

            df = df.drop("input_file_name")

            self.logger.log_message("Completed JSON processing and flattening.", level="info")
            self.logger.log_end("process_and_flatten_json", success=True, additional_message="Proceeding with notebook execution.")
            return df, df_flattened

        except Exception as e:
            self.logger.log_message(f"Error during processing and flattening: {str(e)}", level="error")
            self.logger.log_end("process_and_flatten_json", success=False)
            raise RuntimeError(f"Error during processing and flattening: {e}")