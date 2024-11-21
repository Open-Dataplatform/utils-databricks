import os
import json
import pandas as pd
from io import BytesIO
from typing import Dict, Tuple, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType, StructField, StringType, BooleanType, DoubleType, IntegerType, LongType,
    TimestampType, DecimalType, DateType, BinaryType, StructType, FloatType, DataType
)
from custom_utils.logging.logger import Logger
from custom_utils.config.config import Config

class DataFrameTransformer:
    """
    A transformer class for processing and flattening complex nested data structures 
    (such as JSON or XLSX) in Spark DataFrames, including type conversions and flattening 
    arrays and structs to specified depth levels.
    """
    def __init__(self, config: Config = None, logger: Logger = None, dbutils=None, debug: bool = None):
        # Initialize configuration, logger, and dbutils
        self.config = config
        self.debug = debug if debug is not None else (config.debug if config else False)
        self.logger = logger if logger else (config.logger if config else Logger(debug=self.debug))
        self.dbutils = dbutils if dbutils else config.dbutils if config else None
        self.logger.debug = self.debug

    def _log_message(self, message: str, level="info"):
        """Logs a message using the logger."""
        if self.debug or level != "info":
            self.logger.log_message(message, level=level)

    def _log_block(self, header: str, content_lines: List[str], level="info"):
        """Logs a block of messages with a header and separators."""
        if self.debug or level != "info":
            self.logger.log_block(header, content_lines, level=level)

    def _strip_dbfs_prefix(self, path: str) -> str:
        """Remove the '/dbfs' prefix from a path to make it compatible with dbutils.fs functions."""
        return path[5:] if path and path.startswith('/dbfs') else path

    def _calculate_schema_depth(self, schema, current_depth=0, definitions=None) -> int:
        """Recursively determines the maximum depth of a schema (JSON or XML), counting only arrays."""
        def calculate_depth(schema, current_depth, definitions):
            if isinstance(schema, dict) and '$ref' in schema:
                ref_key = schema['$ref'].split('/')[-1]
                ref_schema = definitions.get(ref_key, {})
                return calculate_depth(ref_schema, current_depth, definitions) if ref_schema else current_depth

            max_depth = current_depth
            if isinstance(schema, dict):
                if 'properties' in schema:
                    for prop_schema in schema['properties'].values():
                        max_depth = max(max_depth, calculate_depth(prop_schema, current_depth, definitions))
                if 'items' in schema:
                    max_depth = max(max_depth, calculate_depth(schema['items'], current_depth + 1, definitions))
            elif isinstance(schema, list):
                for item in schema:
                    max_depth = max(max_depth, calculate_depth(item, current_depth + 1, definitions))
            return max_depth

        return calculate_depth(schema, current_depth, definitions or schema.get('definitions', {}))

    def _infer_max_depth_from_df(self, df_initial: DataFrame) -> int:
        """Infers the maximum depth of a DataFrame's schema by traversing nested structs and arrays."""
        def calculate_depth(data_type, current_depth):
            if isinstance(data_type, ArrayType):
                return calculate_depth(data_type.elementType, current_depth + 1)
            elif isinstance(data_type, StructType):
                return max([calculate_depth(f.dataType, current_depth) for f in data_type.fields], default=current_depth)
            return current_depth

        return max([calculate_depth(f.dataType, 1) for f in df_initial.schema.fields], default=1)

    def _flatten_array_column(self, df_initial: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """Flattens an array column in the DataFrame, handling nested structs within the array."""
        df_initial = df_initial.withColumn(column_name, F.explode_outer(F.col(column_name)))
        nested_columns = self._get_array_and_struct_columns(df_initial)
        for nested_column_name, data_type in nested_columns:
            if nested_column_name == column_name and isinstance(data_type, StructType):
                df_initial = self._flatten_struct_column(df_initial, column_name, layer_separator)
        return df_initial

    def _flatten_struct_column(self, df_initial: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """Flattens a struct column in the DataFrame, recursively processing nested structs."""
        expanded_columns = self._get_expanded_columns_with_aliases(df_initial, column_name, layer_separator)
        df_initial = df_initial.select("*", *expanded_columns).drop(column_name)
        complex_columns = self._get_array_and_struct_columns(df_initial)
        for nested_column_name, data_type in complex_columns:
            if nested_column_name.startswith(f"{column_name}{layer_separator}") and isinstance(data_type, StructType):
                df_initial = self._flatten_struct_column(df_initial, nested_column_name, layer_separator)
        return df_initial

    def _get_expanded_columns_with_aliases(self, df_initial: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.Column]:
        """Returns a list of expanded columns with aliases for nested structs."""
        return [F.col(f"`{column_name}`.`{nested_column}`").alias(f"{column_name}{layer_separator}{nested_column}") 
                for nested_column in df_initial.select(f"`{column_name}`.*").columns]

    def _get_array_and_struct_columns(self, df_initial: DataFrame) -> List[Tuple[str, type]]:
        """Returns a list of array or struct columns in the DataFrame."""
        return [(field.name, field.dataType) for field in df_initial.schema.fields if isinstance(field.dataType, (ArrayType, StructType))]

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
        """Recursively applies type mapping to nested arrays and structs."""
        if isinstance(data_type, ArrayType):
            element_type = data_type.elementType
            transformed_element_type = self._apply_nested_type_mapping(column_name, element_type, type_mapping)
            return ArrayType(transformed_element_type)
        elif isinstance(data_type, StructType):
            fields = [StructField(f.name, self._apply_nested_type_mapping(f"{column_name}.{f.name}", f.dataType, type_mapping), f.nullable) for f in data_type.fields]
            return StructType(fields)
        else:
            original_type_str = data_type.simpleString()
            target_type = type_mapping.get(original_type_str, data_type)
            return target_type if isinstance(target_type, DataType) else data_type

    def _format_converted_columns(self, converted_columns: List[Tuple[str, str, str]], df_flattened: DataFrame) -> List[str]:
        """Formats converted columns for logging, only logging those present in the flattened DataFrame."""
        formatted_conversions = []
        flattened_columns = df_flattened.columns
        for col_name, original_type_str, target_type_str in converted_columns:
            flattened_col_name = col_name.replace('.', '_')
            if flattened_col_name in flattened_columns and original_type_str != target_type_str:
                formatted_conversions.append(f"  - {flattened_col_name}: {original_type_str} -> {target_type_str}")
        return formatted_conversions

    def _get_type_mapping(self) -> dict:
        """Returns a dictionary mapping JSON types to PySpark types."""
        return {
            "string": StringType(),
            "boolean": BooleanType(),
            "number": FloatType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": FloatType(),
            "array": StringType(),
            "object": StringType(),
            "datetime": TimestampType(),
            "decimal": FloatType(),
            "date": DateType(),
            "binary": BinaryType(),
        }

    def _read_binary_file(self, file_path: str) -> DataFrame:
        """Reads binary data from a file path."""
        spark = SparkSession.builder.getOrCreate()
        return spark.read.format("binaryFile").load(file_path)

    def _json_schema_to_spark_struct(self, schema_file_path: str, definitions: Dict = None) -> Tuple[dict, StructType]:
        """
        Converts a JSON schema file to a PySpark StructType.
        This method reads the file from DBFS using dbutils.
        """
        try:
            schema_content = self.dbutils.fs.head(schema_file_path)
            json_schema = json.loads(schema_content)
        except Exception as e:
            raise ValueError(f"Failed to load or parse schema file '{schema_file_path}': {e}")

        if definitions is None:
            definitions = json_schema.get("definitions", {})

        def resolve_ref(ref):
            ref_path = ref.split("/")[-1]
            return definitions.get(ref_path, {})

        def parse_type(field_props):
            if field_props is None:
                return StringType()
            if isinstance(field_props, list):
                valid_types = [parse_type(fp) for fp in field_props if fp.get("type") != "null"]
                return valid_types[0] if valid_types else StringType()
            if "$ref" in field_props:
                field_props = resolve_ref(field_props["$ref"])
            json_type = field_props.get("type")
            if isinstance(json_type, list):
                json_type = next((t for t in json_type if t != "null"), None)
            if json_type == "string": return StringType()
            elif json_type == "integer": return IntegerType()
            elif json_type == "boolean": return BooleanType()
            elif json_type == "number": return DoubleType()
            elif json_type == "array":
                items = field_props.get("items")
                return ArrayType(parse_type(items) if items else StringType())
            elif json_type == "object":
                properties = field_props.get("properties", {})
                return StructType([StructField(k, parse_type(v), True) for k, v in properties.items()])
            return StringType()

        return json_schema, StructType([StructField(name, parse_type(props), True) for name, props in json_schema.get("properties", {}).items()])

    def _process_xlsx(self, df_binary: DataFrame, sheet_name: str) -> DataFrame:
        """Processes binary XLSX files and returns them as a PySpark DataFrame."""
        content = df_binary.select("content").collect()[0][0]
        pdf = pd.read_excel(BytesIO(content), engine='openpyxl', sheet_name=sheet_name)
        pdf.columns = pdf.columns.str.replace(" ", "_")
        df_initial = SparkSession.builder.getOrCreate().createDataFrame(pdf)
        return df_initial

    def _process_json(self, df_binary: DataFrame, schema: StructType = None) -> DataFrame:
        """Processes binary JSON files into a PySpark DataFrame."""
        df_with_filename = df_binary.withColumn("json_string", F.col("content").cast("string")).withColumn("input_file_name", F.col("path")).withColumn("id", F.monotonically_increasing_id())
        if schema:
            df_parsed = SparkSession.builder.getOrCreate().read.schema(schema).json(df_with_filename.select("json_string").rdd.map(lambda row: row.json_string)).withColumn("id", F.monotonically_increasing_id())
        else:
            df_parsed = SparkSession.builder.getOrCreate().read.json(df_with_filename.select("json_string").rdd.map(lambda row: row.json_string)).withColumn("id", F.monotonically_increasing_id())
        df_final = df_parsed.join(df_with_filename, on="id").drop("json_string", "content", "path", "modificationTime", "length", "id")
        return df_final

    def flatten_df(self, df_initial: DataFrame, depth_level: int = None, current_level: int = 0, max_depth: int = 1, type_mapping: Dict[str, str] = None) -> Tuple[DataFrame, List[Tuple[str, str, str]]]:
        """
        Flattens complex fields in a DataFrame up to a specified depth level. 
        If depth_level is None, it flattens all levels.
        """
        type_mapping = type_mapping or self._get_type_mapping()
        converted_columns = []

        def apply_type_mapping_recursively(column_name, data_type):
            if isinstance(data_type, ArrayType):
                # If depth_level is None, continue recursion; otherwise, check depth limit
                if depth_level is not None and current_level + 1 >= depth_level:
                    return StringType()
                else:
                    return ArrayType(apply_type_mapping_recursively(column_name, data_type.elementType))
            elif isinstance(data_type, StructType):
                fields = [StructField(f.name, apply_type_mapping_recursively(f"{column_name}.{f.name}", f.dataType), f.nullable) for f in data_type.fields]
                return StructType(fields)
            original_type_str = data_type.simpleString()
            new_type = type_mapping.get(original_type_str, data_type)
            if original_type_str != new_type.simpleString():
                converted_columns.append((column_name, original_type_str, new_type.simpleString()))
            return new_type

        # Apply the type mapping recursively
        new_columns = [F.col(c).cast(apply_type_mapping_recursively(c, df_initial.schema[c].dataType)) for c in df_initial.columns]
        df_flattened = df_initial.select(new_columns)

        # Flatten the DataFrame by iterating through complex fields
        while depth_level is None or current_level < depth_level:
            complex_fields = {field.name: field.dataType for field in df_flattened.schema.fields if isinstance(field.dataType, (ArrayType, StructType))}
            if not complex_fields:
                break
            for col_name, data_type in complex_fields.items():
                if depth_level is not None and current_level + 1 == depth_level:
                    df_flattened = df_flattened.withColumn(col_name, F.to_json(F.col(col_name)))
                else:
                    if isinstance(data_type, ArrayType):
                        df_flattened = self._flatten_array_column(df_flattened, col_name)
                    elif isinstance(data_type, StructType):
                        df_flattened = self._flatten_struct_column(df_flattened, col_name)
            current_level += 1

        return df_flattened, converted_columns

    def _reorder_columns(self, df: DataFrame) -> DataFrame:
        """
        Reorders the columns of the DataFrame to place 'input_file_name' as the first column.
        """
        if 'input_file_name' in df.columns:
            columns = ['input_file_name'] + [col for col in df.columns if col != 'input_file_name']
            df = df.select(columns)
        return df

    def process_and_flatten_data(
            self,
            schema_file_path: str,
            data_file_path: str,
            file_type: str,
            matched_data_files: List[str] = None,
            depth_level: int = None,
            sheet_name=None,
            include_schema: bool = False
        ) -> Tuple[DataFrame, DataFrame]:
        """
        Main function to process and flatten JSON or XLSX data, with dynamic logging and support for schemas.
        Processes all files in the folder if matched_data_files is not provided.

        Args:
            schema_file_path (str): Path to the schema file.
            data_file_path (str): Path to the data files.
            file_type (str): Type of the files ('json' or 'xlsx').
            matched_data_files (List[str], optional): List of matched data file names.
            depth_level (int, optional): Depth level for flattening nested structures.
            sheet_name (str, optional): Sheet name for XLSX files.
            include_schema (bool, optional): Flag to include schema details in logs.

        Returns:
            Tuple[DataFrame, DataFrame]: A tuple containing the initial and flattened DataFrames.

        Raises:
            RuntimeError: If processing fails or unsupported file type is provided.
        """
        # Log the start of the process
        self.logger.log_start("process_and_flatten_data")

        try:
            # Strip the prefix (if necessary) but only for actual processing
            stripped_schema_file_path = self._strip_dbfs_prefix(schema_file_path)
            stripped_data_file_path = self._strip_dbfs_prefix(data_file_path)

            # Log the schema and file paths at the beginning
            self.logger.log_block("Schema and File Paths", [
                f"Schema file path: {schema_file_path}",
                f"Data file path: {data_file_path}"
            ])

            # If matched_data_files is not provided, retrieve all files from the folder
            if not matched_data_files:
                self.logger.log_message("No matched_data_files provided. Fetching all files from the folder.", level="info")
                all_files = self.dbutils.fs.ls(stripped_data_file_path)
                matched_data_files = [file.name for file in all_files if not file.name.startswith("_")]

                if not matched_data_files:
                    raise ValueError(f"No files found in the folder: {data_file_path}")

            # Correctly strip the DBFS prefix for each file path
            matched_data_files_full_path = [
                f"dbfs:{self._strip_dbfs_prefix(f'{data_file_path}/{file_name}')}" for file_name in matched_data_files
            ]

            # Verify file paths
            matched_file_count = len(matched_data_files)

            # Use Spark to load only the matched files
            df_binary = self._read_binary_file(matched_data_files_full_path)

            # Log number of files loaded into the binary DataFrame
            loaded_file_count = df_binary.select("path").distinct().count()

            # Include loaded file count in the "Matched Data Files" block
            content_lines = [
                f"Number of matched files: {matched_file_count}",
                f"Number of loaded files: {loaded_file_count}"
            ]

            # Check if any files were not loaded
            if matched_file_count != loaded_file_count:
                # Find the files that were not loaded
                loaded_file_paths = df_binary.select("path").rdd.flatMap(lambda x: x).collect()
                loaded_file_names = [os.path.basename(path) for path in loaded_file_paths]
                not_loaded_files = list(set(matched_data_files) - set(loaded_file_names))

                # Add a warning message and display the top 10 not loaded files
                self.logger.log_warning(f"Some files were not loaded. Matched: {matched_file_count}, Loaded: {loaded_file_count}")

                content_lines.append(f"Some files were not loaded. Displaying up to 10 of {len(not_loaded_files)} files not loaded:")
                content_lines.extend([f"- {file_name}" for file_name in not_loaded_files[:10]])

            # Log the "Matched Data Files" block with the updated content
            self.logger.log_block("Matched Data Files", content_lines)

            depth_log_message = ""
            schema = None
            df_initial = None  # Ensure df_initial is initialized

            # Process the file based on the file type
            if file_type == 'json':
                if self.config.use_schema and schema_file_path:
                    # Process JSON schema
                    schema_json, schema = self._json_schema_to_spark_struct(stripped_schema_file_path)
                    max_depth = self._calculate_schema_depth(
                        schema_json,
                        current_depth=0,
                        definitions=schema_json.get('definitions')
                    )
                    
                    # Log JSON schema info with depth details
                    self.logger.log_block("JSON Schema Info", [
                        f"Flattening to depth level: {depth_level if depth_level else max_depth}",
                        "Schema validation enabled"
                    ])

                    # Optionally include schema in the logs (only if debug is True)
                    if include_schema and self.debug:
                        self.logger.log_message(f"Schema JSON: {json.dumps(schema_json, indent=4)}", level="info")
                
                # Process JSON data and assign to df_initial
                df_initial = self._process_json(df_binary, schema)

                # Reorder columns to place 'input_file_name' as the first column
                df_initial = self._reorder_columns(df_initial)

                # Set depth level if not provided explicitly
                if not self.config.use_schema:
                    max_depth = self._infer_max_depth_from_df(df_initial)
                    if depth_level is None:
                        depth_level = max_depth
                depth_log_message = f"Inferred max depth from DataFrame: {max_depth}; Using depth level: {depth_level}"

            elif file_type == 'xlsx':
                # Process XLSX data
                df_initial = self._process_xlsx(df_binary, sheet_name)

                # Log XLSX column information
                self.logger.log_block("XLSX Info", [
                    f"Processed XLSX sheet: {sheet_name}",
                    f"DataFrame columns: {df_initial.columns}"
                ])
                depth_log_message = ""

            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            # At this point, df_initial should be properly assigned
            if df_initial is None:
                raise RuntimeError(f"Failed to process the file. No DataFrame was created for file type: {file_type}")

            # Create a temporary DataFrame for logging without 'input_file_name' if it exists
            if 'input_file_name' in df_initial.columns:
                df_logged_initial = df_initial.drop("input_file_name")
            else:
                df_logged_initial = df_initial

            # Log Initial DataFrame info (before flattening)
            initial_row_count = df_initial.count()
            self.logger.log_block("Initial DataFrame Info", [
                f"Initial DataFrame row count: {initial_row_count}"
            ])
            self.logger.log_message("Initial DataFrame schema:", level="info")
            df_logged_initial.printSchema()

            # Flatten the DataFrame if necessary (for JSON, typically)
            df_flattened, converted_columns = self.flatten_df(df_initial, depth_level=depth_level)

            # Reorder columns to place 'input_file_name' as the first column
            df_flattened = self._reorder_columns(df_flattened)

            # Log flattened DataFrame info
            flattened_row_count = df_flattened.count()
            self.logger.log_block("Flattened DataFrame Info", [
                f"Flattened DataFrame row count: {flattened_row_count}",
                depth_log_message if file_type == 'json' and not self.config.use_schema else ""
            ])
            self.logger.log_message("Flattened DataFrame schema:", level="info")
            df_flattened.printSchema()

            # Log converted columns if there are any
            if converted_columns:
                formatted_conversions = self._format_converted_columns(converted_columns, df_flattened)
                if formatted_conversions:
                    self.logger.log_block("Columns converted by _get_type_mapping", formatted_conversions)

            # End log
            self.logger.log_end("process_and_flatten_data", success=True)

            # Return DataFrames
            return df_initial, df_flattened

        except Exception as e:
            error_message = f"Error during processing: {str(e)}"
            self.logger.log_error(error_message)
            self.logger.log_end("process_and_flatten_data", success=False)
            raise RuntimeError(error_message)