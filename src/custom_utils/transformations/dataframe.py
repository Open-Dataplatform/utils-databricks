# File: custom_utils/transformations/dataframe.py

import json
import pyspark.sql.functions as F
from typing import Dict, Tuple, List
from pyspark.sql.types import (
    ArrayType,
    StructField,
    StringType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
    DecimalType,
    DateType,
    BinaryType,
    StructType,
    FloatType,
)
from pyspark.sql import SparkSession, DataFrame
from custom_utils.logging.logger import Logger
from custom_utils.config.config import Config  # Assuming there is a config module with a Config class

class DataFrameTransformer:
    def __init__(self, config: Config = None, logger: Logger = None, debug: bool = None):
        """
        Initializes the DataFrameTransformer with logging capabilities.

        Args:
            config (Config, optional): An instance of the Config class containing configuration parameters.
            logger (Logger, optional): An instance of the Logger class for logging. Defaults to None.
            debug (bool, optional): If set, it overrides the config's debug setting.
        """
        self.config = config
        self.debug = debug if debug is not None else (config.debug if config else False)
        self.logger = logger if logger else (config.logger if config else Logger(debug=self.debug))
        
        # Update the logger's debug flag to match the one passed to DataFrameTransformer
        self.logger.debug = self.debug

    def _log_message(self, message: str, level="info"):
        """Logs a message using the logger."""
        if self.debug or level != "info":
            self.logger.log_message(message, level=level)

    def _log_block(self, header: str, content_lines: List[str], level="info"):
        """Logs a block of messages with a header and separators."""
        if self.debug or level != "info":
            self.logger.log_block(header, content_lines, level=level)

    def _get_json_depth(self, json_schema, current_depth=0, definitions=None, logger=None, depth_level=None) -> int:
        """
        Recursively determines the maximum depth of a JSON schema, including handling references and mixed structures.

        Args:
            json_schema (dict): A JSON schema represented as a dictionary.
            current_depth (int): The current depth level (used internally).
            definitions (dict, optional): Definitions from the JSON schema to resolve $ref references. Defaults to None.
            logger (Logger, optional): Logger object used for logging. If provided, logs the maximum depth.
            depth_level (int, optional): The specified flattening depth level for comparison in the log message.

        Returns:
            int: The maximum depth level of the JSON schema.
        """
        def calculate_depth(schema, current_depth, definitions):
            # Handle $ref references
            if isinstance(schema, dict) and '$ref' in schema:
                ref_key = schema['$ref'].split('/')[-1]
                ref_schema = definitions.get(ref_key, {})
                if ref_schema:
                    return calculate_depth(ref_schema, current_depth, definitions)
                else:
                    raise ValueError(f"Reference '{ref_key}' not found in definitions.")

            # Initialize the max depth as the current depth
            max_depth = current_depth

            if isinstance(schema, dict):
                # Handle properties (objects)
                if 'properties' in schema:
                    properties_depth = max(
                        calculate_depth(v, current_depth + 1, definitions)
                        for v in schema['properties'].values()
                    )
                    max_depth = max(max_depth, properties_depth)

                # Handle items (arrays)
                if 'items' in schema and isinstance(schema['items'], dict):
                    items_depth = calculate_depth(schema['items'], current_depth + 1, definitions)
                    max_depth = max(max_depth, items_depth)

            if isinstance(schema, list):
                # Handle cases where items is a list of objects
                list_depths = [
                    calculate_depth(item, current_depth + 1, definitions)
                    for item in schema
                ]
                max_depth = max(max_depth, *list_depths)

            return max_depth

        # Calculate the depth
        max_depth = calculate_depth(json_schema, current_depth, definitions or json_schema.get('definitions', {}))

        # Log the depth using the logger, if provided
        if logger:
            logger.log_message(f"Maximum depth level of the JSON schema: {max_depth}; Flattened depth level of the JSON file: {depth_level}")

        return max_depth

    def _get_type_mapping(self) -> dict:
            """
            Returns a dictionary that maps JSON data types to corresponding PySpark SQL types.

            The mappings are useful when converting JSON schemas into Spark DataFrame schemas,
            ensuring correct data types are assigned during parsing.

            Returns:
                dict: A dictionary where:
                    - Keys are JSON data types as strings (e.g., "string", "integer").
                    - Values are corresponding PySpark SQL data types (e.g., StringType(), IntegerType()).
            """
            return {
                "string": StringType(),       # Maps JSON "string" to PySpark's StringType
                "boolean": BooleanType(),     # Maps JSON "boolean" to PySpark's BooleanType
                "number": FloatType(),        # Maps JSON "number" to PySpark's FloatType (default for numbers)
                "integer": IntegerType(),     # Maps JSON "integer" to PySpark's IntegerType
                "long": LongType(),           # Maps JSON "long" (if specified) to PySpark's LongType
                "double": FloatType(),        # Maps JSON "double" to PySpark's FloatType
                "array": StringType(),        # Treats arrays as strings (flattening scenarios)
                "object": StringType(),       # Treats objects as strings (flattening scenarios)
                "datetime": TimestampType(),  # Maps JSON "datetime" to PySpark's TimestampType
                "decimal": FloatType(),       # Maps JSON "decimal" to PySpark's FloatType
                "date": DateType(),           # Maps JSON "date" to PySpark's DateType
                "time": StringType(),         # Treats time as a string (time-only types)
                "binary": BinaryType(),       # Maps binary data to PySpark's BinaryType
            }

    def _get_array_and_struct_columns(self, df: DataFrame) -> List[Tuple[str, type]]:
        """Returns a list of columns in the DataFrame that are of type ArrayType or StructType."""
        complex_columns = []
        for field in df.schema.fields:
            data_type = type(field.dataType)
            if data_type in [ArrayType, StructType]:
                complex_columns.append((field.name, field.dataType))
        return complex_columns

    def _json_schema_to_spark_struct(self, schema_file_path: str, definitions: Dict = None) -> Tuple[dict, StructType]:
        """Converts a JSON schema file to a PySpark StructType."""
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

    def _flatten_array_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """Flatten an array column in the DataFrame and handle nested structs within the array."""
        df = df.withColumn(column_name, F.explode_outer(F.col(column_name)))
        nested_columns = self._get_array_and_struct_columns(df)
        for nested_column_name, data_type in nested_columns:
            if nested_column_name == column_name and isinstance(data_type, StructType):
                df = self._flatten_struct_column(df, column_name, layer_separator)
        return df

    def _flatten_struct_column(self, df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """Flatten a struct column in the DataFrame, including nested structs recursively."""
        expanded_columns = self._get_expanded_columns_with_aliases(df, column_name, layer_separator)
        df = df.select("*", *expanded_columns).drop(column_name)
        complex_columns = self._get_array_and_struct_columns(df)
        for nested_column_name, data_type in complex_columns:
            if nested_column_name.startswith(f"{column_name}{layer_separator}") and isinstance(data_type, StructType):
                df = self._flatten_struct_column(df, nested_column_name, layer_separator)
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
        """Formats a given schema to make it more readable."""
        formatted_schema = ""
        indent = " " * (indent_level * 4)
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

    def flatten_df(self, df: DataFrame, depth_level: int, current_level: int = 0, max_depth: int = 1, type_mapping: Dict[str, str] = None) -> Tuple[DataFrame, List[Tuple[str, str, str]]]:
        """
        Flatten complex fields in a DataFrame up to a specified depth level.

        Args:
            df (DataFrame): The DataFrame to flatten.
            depth_level (int): The depth level to flatten.
            current_level (int): The current depth level (used internally).
            max_depth (int): The maximum depth level of the JSON schema.
            type_mapping (Dict[str, str], optional): Mapping of data types for conversion.

        Returns:
            Tuple[DataFrame, List[Tuple[str, str, str]]]: The flattened DataFrame and a list of converted columns.
        """
        # Use the internal _get_type_mapping method if no external type_mapping is provided
        type_mapping = type_mapping if type_mapping is not None else self._get_type_mapping()

        # Track columns that were converted
        converted_columns = []

        if type_mapping:
            # Apply type conversion to the DataFrame columns
            new_columns = []
            for c in df.columns:
                target_type = type_mapping.get(df.schema[c].dataType.simpleString(), df.schema[c].dataType)
                if target_type != df.schema[c].dataType:
                    converted_columns.append((c, df.schema[c].dataType.simpleString(), target_type.simpleString()))
                new_columns.append(F.col(c).cast(target_type))
            df = df.select(new_columns)

        # Flatten the DataFrame as usual (rest of the flattening logic)
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
                        df = self._flatten_array_column(df, col_name, layer_separator="_")
                    elif isinstance(data_type, StructType):
                        df = self._flatten_struct_column(df, col_name, layer_separator="_")

            current_level += 1

        if current_level >= depth_level:
            df = df.select([
                F.col(c).cast(StringType()) if isinstance(df.schema[c].dataType, (ArrayType, StructType)) else F.col(c)
                for c in df.columns
            ])

        for column_name in df.columns:
            new_name = column_name.replace("__", "_").replace(".", "_")
            df = df.withColumnRenamed(column_name, new_name)

        return df, converted_columns

    def _read_json_from_binary(self, spark: SparkSession, schema: StructType, data_file_path: str) -> DataFrame:
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
            self._log_message(f"Error processing binary JSON files: {e}", level="error")
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
            max_depth = self._get_json_depth(schema_json)

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
            df = self._read_json_from_binary(spark, schema, data_file_path)

            # Block 2: Initial DataFrame info
            initial_row_count = df.count()
            self.logger.log_block("Initial DataFrame Info", [
                f"Initial DataFrame row count: {initial_row_count}"
            ])
            
            # Log the schema if debug is enabled
            if self.debug:
                self.logger.log_message("Initial DataFrame schema:", level="info")
                df.printSchema()  # This will print the schema to the console

            # Flatten the DataFrame
            df_flattened, converted_columns = self.flatten_df(df, depth_level=depth_level_to_use, max_depth=max_depth, type_mapping=self._get_type_mapping())

            # Block 3: Flattened DataFrame info
            flattened_row_count = df_flattened.count()
            self.logger.log_block("Flattened DataFrame Info", [
                f"Flattened DataFrame row count: {flattened_row_count}"
            ])

            # Block 4: Columns converted by _get_type_mapping
            if converted_columns:
                converted_column_lines = [f"  - {col_name}: {original_type} -> {converted_type}" for col_name, original_type, converted_type in converted_columns]
                self.logger.log_block("Columns converted by _get_type_mapping", converted_column_lines)
            
            # Log the flattened schema if debug is enabled
            if self.debug:
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