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
from pyspark.sql.types import DataType

class DataFrameTransformer:
    def __init__(self, config: Config = None, logger: Logger = None, dbutils=None, debug: bool = None):
        """
        Initializes the DataFrameTransformer with logging capabilities and optional dbutils.

        Args:
            config (Config, optional): An instance of the Config class containing configuration parameters.
            logger (Logger, optional): An instance of the Logger class for logging. Defaults to None.
            debug (bool, optional): If set, it overrides the config's debug setting.
            dbutils (optional): dbutils object for accessing DBFS and other utilities.
        """
        self.config = config
        self.debug = debug if debug is not None else (config.debug if config else False)
        self.logger = logger if logger else (config.logger if config else Logger(debug=self.debug))
        self.dbutils = dbutils if dbutils else config.dbutils  # Use dbutils from config if available
        
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
        Recursively determines the maximum depth of a JSON schema, counting only arrays for depth, not structs,
        and correctly handling $ref references.

        Args:
            json_schema (dict): A JSON schema represented as a dictionary.
            current_depth (int): The current depth level (used internally).
            definitions (dict, optional): Definitions from the JSON schema to resolve $ref references. Defaults to None.
            logger (Logger, optional): Logger object used for logging. If provided, logs the maximum depth.
            depth_level (int, optional): The specified flattening depth level for comparison in the log message.

        Returns:
            int: The maximum depth level of the JSON schema, excluding structs.
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
                # Handle properties (objects) without counting them as depth
                if 'properties' in schema:
                    for prop_name, prop_schema in schema['properties'].items():
                        max_depth = max(max_depth, calculate_depth(prop_schema, current_depth, definitions))

                # Traverse array "items" and increase depth for arrays
                if 'items' in schema:
                    max_depth = max(max_depth, calculate_depth(schema['items'], current_depth + 1, definitions))

            elif isinstance(schema, list):
                # Handle cases where items is a list of objects or arrays
                max_depth = max(calculate_depth(item, current_depth + 1, definitions) for item in schema)

            # Base case: for primitive types, return the current depth
            return max_depth

        # Calculate the depth, passing definitions if they exist
        max_depth = calculate_depth(json_schema, current_depth, definitions or json_schema.get('definitions', {}))

        # Log the depth using the logger, if provided
        if logger:
            logger.log_message(f"Maximum depth level of the JSON schema: {max_depth}; Flattened depth level: {depth_level}")

        return max_depth

    def _infer_max_depth_from_df(self, df: DataFrame) -> int:
        """
        Infers the maximum depth of a DataFrame's schema by traversing nested structs and arrays.
        Only arrays contribute to depth, and structs do not increase depth unless they contain arrays.

        Args:
            df (DataFrame): The DataFrame for which to infer the max depth.

        Returns:
            int: The maximum depth of the DataFrame's schema, excluding structs.
        """
        def calculate_depth(data_type, current_depth):
            # If it's an array, increase the depth level and recurse into the element type
            if isinstance(data_type, ArrayType):
                return calculate_depth(data_type.elementType, current_depth + 1)
            # If it's a struct, recurse into the struct's fields but don't increase depth
            elif isinstance(data_type, StructType):
                return max([calculate_depth(f.dataType, current_depth) for f in data_type.fields], default=current_depth)
            # For primitive types, return the current depth
            else:
                return current_depth

        # Start calculating depth for each field in the schema
        max_depth = max([calculate_depth(f.dataType, 1) for f in df.schema.fields], default=1)
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

    def _strip_dbfs_prefix(self, path: str) -> str:
        """
        Remove the '/dbfs' prefix from a path to make it compatible with dbutils.fs functions.

        Args:
            path (str): The path to strip.

        Returns:
            str: The path without the '/dbfs' prefix.
        """
        if path and path.startswith('/dbfs'):
            return path[5:]  # Strip '/dbfs' from the path
        return path

    def _apply_nested_type_mapping(self, column_name, data_type, type_mapping):
        """
        Recursively applies the type mapping to nested structures (arrays or structs).
        
        Args:
            column_name (str): The name of the column to apply the type mapping.
            data_type (DataType): The data type of the column.
            type_mapping (dict): A dictionary that maps original data types to target types.
        
        Returns:
            Column: The transformed column with applied type mapping.
        """
        if isinstance(data_type, ArrayType):
            element_type = data_type.elementType
            # Recursively apply the type mapping to the elements of the array
            transformed_element_type = self._apply_nested_type_mapping(column_name, element_type, type_mapping)
            return ArrayType(transformed_element_type)  # Apply to the ArrayType itself
        
        elif isinstance(data_type, StructType):
            # Apply the type mapping to each field in the struct
            fields = []
            for field in data_type.fields:
                transformed_field_type = self._apply_nested_type_mapping(f"{column_name}.{field.name}", field.dataType, type_mapping)
                fields.append(StructField(field.name, transformed_field_type, field.nullable))
            return StructType(fields)
        
        # For basic types, apply the conversion from the type_mapping
        original_type_str = data_type.simpleString()
        target_type = type_mapping.get(original_type_str, data_type)
        
        if isinstance(target_type, DataType):
            return target_type  # If it's a DataType, return the correct type directly
        else:
            return data_type  # Fallback to the original type if no mapping exists
        
    def _format_converted_columns(self, converted_columns: List[Tuple[str, str, str]], df_flattened: DataFrame) -> List[str]:
        """
        Format the converted columns for display. This method unfolds arrays and structs and shows individual field changes,
        ensuring that the columns exist in the flattened DataFrame before logging.
        
        Args:
            converted_columns (List[Tuple[str, str, str]]): List of converted columns with (column name, original type, target type).
            df_flattened (DataFrame): The flattened DataFrame to check if columns exist before logging.

        Returns:
            List[str]: Formatted list of changes to be logged.
        """
        formatted_conversions = []
        flattened_columns = df_flattened.columns  # Get the list of columns in the flattened DataFrame

        def unfold_struct_type(column_name, original_type_str, target_type_str):
            """
            Unfold a struct type and display individual fields one by one, ensuring the fields actually differ.
            """
            unfolded = []

            if original_type_str.startswith("array<struct<") and target_type_str.startswith("array<struct<"):
                # Extract fields in the struct within the array
                original_fields = original_type_str[13:-2].split(",")
                target_fields = target_type_str[13:-2].split(",")

                for orig, targ in zip(original_fields, target_fields):
                    try:
                        # Handle cases where there may be more than one ":" in the string
                        orig_parts = orig.split(":")
                        targ_parts = targ.split(":")

                        if len(orig_parts) == 2 and len(targ_parts) == 2:
                            orig_field_name, orig_type = orig_parts
                            targ_field_name, targ_type = targ_parts
                            # Log each field conversion separately if there's a difference in types
                            if orig_type != targ_type:
                                unfolded.append(f"  - {column_name}.{orig_field_name}: {orig_type} -> {targ_type}")
                        else:
                            unfolded.append(f"  - {column_name}: {orig} -> {targ}")  # If format isn't as expected, log the whole string
                    except ValueError:
                        unfolded.append(f"  - {column_name}: {orig} -> {targ}")  # Catch any unexpected errors in splitting

            return unfolded if unfolded else [f"  - {column_name}: {original_type_str} -> {target_type_str}"]

        for col_name, original_type_str, target_type_str in converted_columns:
            # Check if the column or its flattened equivalent exists in the flattened DataFrame
            if col_name in flattened_columns or any(flattened_column.startswith(f"{col_name}_") for flattened_column in flattened_columns):
                # Skip the columns that did not change
                if original_type_str != target_type_str:
                    # Handle complex types (arrays and structs) by unfolding them if necessary
                    if "struct" in original_type_str or "array" in original_type_str:
                        unfolded_columns = unfold_struct_type(col_name, original_type_str, target_type_str)
                        if unfolded_columns:
                            formatted_conversions.extend(unfolded_columns)
                    else:
                        # For simple types, display directly
                        formatted_conversions.append(f"  - {col_name}: {original_type_str} -> {target_type_str}")

        return formatted_conversions

    def _filter_converted_columns(self, converted_columns: List[Tuple[str, str, str]], df_flattened: DataFrame) -> List[Tuple[str, str, str]]:
        """
        Filters the converted columns to include only those present in the flattened DataFrame.
        
        Args:
            converted_columns (List[Tuple[str, str, str]]): A list of converted columns in the format (column_name, original_type, new_type).
            df_flattened (DataFrame): The flattened DataFrame.

        Returns:
            List[Tuple[str, str, str]]: Filtered list of converted columns that exist in df_flattened.
        """
        flattened_columns = df_flattened.columns

        filtered_conversions = []

        for column_name, original_type, new_type in converted_columns:
            # Check if the column is in the flattened DataFrame directly or has been flattened into subfields
            if column_name in flattened_columns:
                filtered_conversions.append((column_name, original_type, new_type))
            else:
                # Check if any of the flattened columns match the prefix of this column (e.g., "data" -> "data_reference_incident")
                if any(flattened_column.startswith(f"{column_name}_") for flattened_column in flattened_columns):
                    filtered_conversions.append((column_name, original_type, new_type))

        return filtered_conversions

    def _get_array_and_struct_columns(self, df: DataFrame) -> List[Tuple[str, type]]:
        """Returns a list of columns in the DataFrame that are of type ArrayType or StructType."""
        complex_columns = []
        for field in df.schema.fields:
            data_type = type(field.dataType)
            if data_type in [ArrayType, StructType]:
                complex_columns.append((field.name, field.dataType))
        return complex_columns

    def _json_schema_to_spark_struct(self, schema_file_path: str, definitions: Dict = None) -> Tuple[dict, StructType]:
        """
        Converts a JSON schema file to a PySpark StructType.
        This method reads the file from DBFS using dbutils.
        """
        try:
            # Use dbutils to read the file as a string
            schema_content = self.dbutils.fs.head(schema_file_path)
            json_schema = json.loads(schema_content)  # Parse the JSON schema
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
            depth_level (int): The depth level to flatten (if None, use max depth).
            current_level (int): The current depth level (used internally).
            max_depth (int): The maximum depth level of the JSON schema.
            type_mapping (Dict[str, str], optional): Mapping of data types for conversion.

        Returns:
            Tuple[DataFrame, List[Tuple[str, str, str]]]: The flattened DataFrame and a list of converted columns.
        """
        # If depth_level is None, flatten all levels (use max depth)
        if depth_level is None:
            depth_level = max_depth

        type_mapping = type_mapping if type_mapping is not None else self._get_type_mapping()
        converted_columns = []

        # Apply type conversions if type_mapping is provided
        if type_mapping:
            new_columns = []
            for c in df.columns:
                original_type = df.schema[c].dataType
                new_column = self._apply_nested_type_mapping(c, original_type, type_mapping)
                new_columns.append(F.col(c).cast(new_column))  # Cast column to the new type
                if original_type.simpleString() != new_column.simpleString():
                    converted_columns.append((c, original_type.simpleString(), new_column.simpleString()))

            df = df.select(new_columns)

        # Perform the flattening process
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
                        df = self._flatten_array_column(df, col_name)
                    elif isinstance(data_type, StructType):
                        df = self._flatten_struct_column(df, col_name)

            current_level += 1

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
            schema_file_path (str): Path to the schema file. If None, schema validation will be skipped.
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

            # Correct the file paths by stripping any leading '/dbfs' if it exists
            schema_file_path = self._strip_dbfs_prefix(schema_file_path)
            data_file_path = self._strip_dbfs_prefix(data_file_path)

            schema = None  # Initialize schema to None
            max_depth = None  # Default max depth is None, it will be inferred later if needed

            # Check if schema is being used
            if self.config.use_schema:
                # Load schema from schema file
                schema_json, schema = self._json_schema_to_spark_struct(schema_file_path)
                max_depth = self._get_json_depth(schema_json)  # Get max depth from the schema

                # Log schema-related info
                self.logger.log_block("Schema and File Paths", [
                    f"Schema file path: {schema_file_path}",
                    f"Data file path: {data_file_path}",
                    f"Maximum depth level of the JSON schema: {max_depth}"
                ])
                
                if include_schema:
                    self.logger.log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

            else:
                # Log that schema validation is skipped
                self.logger.log_message(f"No schema provided. Skipping schema validation.", level="warning")
                self.logger.log_block("File Paths", [
                    f"Data file path: {data_file_path}"
                ])

                # Infer schema dynamically and keep input_file_name
                df = spark.read.option("multiline", "true").json(data_file_path)
                df = df.withColumn("input_file_name", F.input_file_name())  # Add the file name to the DataFrame

                # Log the inferred schema as the initial DataFrame schema
                self.logger.log_message(f"Using dynamically inferred schema for initial DataFrame.", level="info")

                # Infer the max depth from the DataFrame if no schema is provided
                max_depth = self._infer_max_depth_from_df(df.drop("input_file_name"))
                self.logger.log_message(f"Max depth inferred from initial DataFrame: {max_depth}", level="info")

            # If depth_level is None, use the inferred max_depth
            if depth_level is None:
                depth_level_to_use = max_depth
                self.logger.log_message(f"DepthLevel is not provided. Using max_depth: {max_depth} as DepthLevel.", level="info")
            else:
                depth_level_to_use = depth_level
                self.logger.log_message(f"Using provided DepthLevel: {depth_level_to_use}", level="info")

            # Read the JSON data. If schema is provided, use it; otherwise, infer the schema dynamically
            if self.config.use_schema:
                df = self._read_json_from_binary(spark, schema, data_file_path)
            else:
                # Schema was already dynamically inferred above when max_depth was determined
                pass

            # Block 2: Initial DataFrame info
            initial_row_count = df.count()
            self.logger.log_block("Initial DataFrame Info", [
                f"Initial DataFrame row count: {initial_row_count}"
            ])

            # Log the schema of the initial DataFrame without "input_file_name"
            if self.debug:
                self.logger.log_message("Initial DataFrame schema (without input_file_name):", level="info")
                df.drop("input_file_name").printSchema()  # This will print the schema without "input_file_name"

            # Flatten the DataFrame using the determined depth level
            df_flattened, converted_columns = self.flatten_df(df, depth_level=depth_level_to_use, max_depth=max_depth, type_mapping=self._get_type_mapping())

            # Block 3: Flattened DataFrame info
            flattened_row_count = df_flattened.count()
            self.logger.log_block("Flattened DataFrame Info", [
                f"Flattened DataFrame row count: {flattened_row_count}",
                f"Depth level used: {depth_level_to_use}"  # Log the depth level used, whether it was provided or inferred
            ])

            # **Filter converted columns based on the columns in the flattened DataFrame**
            final_converted_columns = self._filter_converted_columns(converted_columns, df_flattened)

            # Block 4: Columns converted by _get_type_mapping (filtered based on flattened_df)
            if final_converted_columns:
                formatted_conversions = self._format_converted_columns(final_converted_columns, df_flattened)
                # Log only if there are columns to be displayed
                if formatted_conversions:
                    self.logger.log_block("Columns converted by _get_type_mapping", formatted_conversions)
            
            # Log the flattened schema if debug is enabled
            if self.debug:
                self.logger.log_message("Flattened DataFrame schema:", level="info")
                df_flattened.printSchema()  # This will print the schema to the console

            # Ensure `input_file_name` is the first column in both DataFrames
            columns_with_input_file = ["input_file_name"] + [col for col in df.columns if col != "input_file_name"]
            df = df.select(columns_with_input_file)

            columns_with_input_file_flattened = ["input_file_name"] + [col for col in df_flattened.columns if col != "input_file_name"]
            df_flattened = df_flattened.select(columns_with_input_file_flattened)

            # Log the end of the process with the additional message
            self.logger.log_end("process_and_flatten_json", success=True, additional_message="Proceeding with notebook execution.")
            
            return df, df_flattened

        except Exception as e:
            # Log the error message
            self.logger.log_message(f"Error during processing and flattening: {str(e)}", level="error")
            
            # Log the end of the process with failure
            self.logger.log_end("process_and_flatten_json", success=False, additional_message="Check error logs for details.")
            
            raise RuntimeError(f"Error during processing and flattening: {e}")