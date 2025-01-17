from custom_utils.logging.logger import Logger
from custom_utils.config.config import Config
from custom_utils.validation.validation import Validator

import os
import json
import xmlschema
import pandas as pd
import shutil
import logging
from io import BytesIO
from typing import Dict, Tuple, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType, StructField, StringType, BooleanType, DoubleType, IntegerType, LongType,
    TimestampType, DecimalType, DateType, BinaryType, StructType, FloatType, DataType
)

class DataFrameTransformer:
    """
    A class to handle DataFrame processing and flattening with robust schema handling and logging.
    """

    def __init__(self, config: Config, logger: Logger = None, validator: Validator = None, debug: bool = None):
        """
        Initializes the DataFrameTransformer with configuration, logger, and debugging options.

        Args:
            config (Config): Configuration instance for the transformer.
            logger (Logger, optional): Logger instance for logging activities. Defaults to the logger in the config.
            validator (Validator, optional): Validator instance for file validation. Defaults to a new Validator instance.
            debug (bool, optional): Enables debug-level logging. Overrides Config.debug if provided.
        """
        self.config = config
        self.debug = debug if debug is not None else config.debug
        self.logger = logger or config.logger
        self.validator = validator or Validator(config=config, debug=self.debug)

        # Adjust logger's debug mode and level
        self.logger.update_debug_mode(self.debug)
        self.logger.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

        # Validate configuration and initialize dbutils
        self.dbutils = self.config.dbutils

        self.logger.log_block("DataFrameTransformer Initialization", [
            f"Debug Mode: {self.debug}",
            f"Logger: {'Custom Logger' if logger else 'Config Logger'}",
            f"Validator: {'Provided' if validator else 'Default Validator'}",
            "Initialization completed successfully."
        ], level="debug")
        
    def rename_and_process(self, df: DataFrame, column_mapping: Dict[str, str], cast_columns: Dict[str, str]) -> DataFrame:
        """
        Renames columns, casts specific columns, and handles case-insensitive matching.

        Args:
            df (DataFrame): The input DataFrame to process.
            column_mapping (dict): Mapping of old column names to new names.
            cast_columns (dict): Columns to cast with their target data types.

        Returns:
            DataFrame: The processed DataFrame.
        """
        self.logger.log_start("rename_and_process")
        renamed_columns = []
        skipped_columns = []
        casted_columns = []
        try:
            # Normalize column names in DataFrame for case-insensitive matching
            normalized_columns = {col.lower(): col for col in df.columns}

            # Rename columns
            for old_col, new_col in column_mapping.items():
                normalized_old_col = old_col.lower()
                if normalized_old_col in normalized_columns:
                    actual_col = normalized_columns[normalized_old_col]
                    self.logger.log_message(f"Renaming column: {actual_col} -> {new_col}", level="debug")
                    df = df.withColumnRenamed(actual_col, new_col)
                    renamed_columns.append(f"{actual_col} -> {new_col}")
                else:
                    skipped_columns.append(old_col)

            # Cast specific columns to required types
            for col_name, data_type in cast_columns.items():
                if col_name in df.columns:
                    self.logger.log_message(f"Casting column: {col_name} to {data_type}", level="debug")
                    df = df.withColumn(col_name, col(col_name).cast(data_type))
                    casted_columns.append(f"{col_name} to {data_type}")
                else:
                    skipped_columns.append(col_name)

            # Log formatted info block summarizing column operations
            self.logger.log_block("Rename and Process Summary", [
                f"Renamed Columns ({len(renamed_columns)}):",
                *renamed_columns,
                f"Skipped Columns ({len(skipped_columns)}):",
                *skipped_columns,
                f"Casted Columns ({len(casted_columns)}):",
                *casted_columns,
            ], level="info")

            self.logger.log_end("rename_and_process", success=True)
            return df

        except Exception as e:
            self.logger.log_error(f"Error in rename_and_process: {str(e)}")
            self.logger.log_end("rename_and_process", success=False)
            raise RuntimeError(f"Failed to rename and process DataFrame: {e}")   

    def _log_processing_configuration(self, schema_file_path, data_file_path, file_type, matched_data_files):
        """Logs the processing configuration."""
        self.logger.log_block("Processing Configuration", [
            f"Schema File Path: {schema_file_path}",
            f"Data File Path: {data_file_path}",
            f"File Type: {file_type}",
            f"Matched Data Files: {len(matched_data_files)} files"
        ], level="info")

    def _flatten_array_column(self, df_initial: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """
        Flattens an array column in the DataFrame, handling nested structs within the array.

        Args:
            df_initial (DataFrame): Input DataFrame.
            column_name (str): Column name to flatten.
            layer_separator (str): Separator for nested field names.

        Returns:
            DataFrame: Updated DataFrame with the array column flattened.
        """
        df_initial = df_initial.withColumn(column_name, F.explode_outer(F.col(column_name)))
        nested_columns = self._get_array_and_struct_columns(df_initial)

        for nested_column_name, data_type in nested_columns:
            if nested_column_name == column_name and isinstance(data_type, StructType):
                df_initial = self._flatten_struct_column(df_initial, column_name, layer_separator)

        return df_initial
    
    def _flatten_struct_column(self, df_initial: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
        """
        Flattens a struct column in the DataFrame, recursively processing nested structs.

        Args:
            df_initial (DataFrame): Input DataFrame.
            column_name (str): Column name to flatten.
            layer_separator (str): Separator for nested field names.

        Returns:
            DataFrame: Updated DataFrame with the struct column flattened.
        """
        expanded_columns = self._get_expanded_columns_with_aliases(df_initial, column_name, layer_separator)
        df_initial = df_initial.select("*", *expanded_columns).drop(column_name)
        complex_columns = self._get_array_and_struct_columns(df_initial)

        for nested_column_name, data_type in complex_columns:
            if nested_column_name.startswith(f"{column_name}{layer_separator}") and isinstance(data_type, StructType):
                df_initial = self._flatten_struct_column(df_initial, nested_column_name, layer_separator)

        return df_initial
    
    def _get_expanded_columns_with_aliases(self, df_initial: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.Column]:
        """
        Returns a list of expanded columns with aliases for nested structs.

        Args:
            df_initial (DataFrame): Input DataFrame.
            column_name (str): Struct column name to expand.
            layer_separator (str): Separator for nested field names.

        Returns:
            List[Column]: List of expanded columns with aliases.
        """
        nested_columns = df_initial.select(f"`{column_name}`.*").columns
        return [
            F.col(f"`{column_name}`.`{nested_column}`").alias(f"{column_name}{layer_separator}{nested_column}")
            for nested_column in nested_columns
        ]

    def _get_array_and_struct_columns(self, df_initial: DataFrame) -> List[Tuple[str, type]]:
        """
        Returns a list of array or struct columns in the DataFrame.

        Args:
            df_initial (DataFrame): Input DataFrame.

        Returns:
            List[Tuple[str, type]]: List of column names and their types.
        """
        return [
            (field.name, field.dataType)
            for field in df_initial.schema.fields
            if isinstance(field.dataType, (ArrayType, StructType))
        ]

    def _format_converted_columns(self, converted_columns: List[Tuple[str, str, str]], df_flattened: DataFrame) -> List[str]:
        """
        Formats converted columns for logging, including only columns present in the flattened DataFrame.

        Args:
            converted_columns (List[Tuple[str, str, str]]): List of converted columns with original and target types.
            df_flattened (DataFrame): Flattened DataFrame.

        Returns:
            List[str]: Formatted conversion descriptions.
        """
        self.logger.log_start("_format_converted_columns")
        
        # Filter columns present in the flattened DataFrame
        valid_columns = [
            (col_name, original_type, target_type)
            for col_name, original_type, target_type in converted_columns
            if col_name in df_flattened.columns
        ]
        
        # Format the filtered columns
        formatted_conversions = [
            f"{col_name}: {original_type} -> {target_type}"
            for col_name, original_type, target_type in valid_columns
        ]
        
        # Log the count and details of conversions
        if formatted_conversions:
            self.logger.log_block("Formatted Converted Columns (DEBUG)", [
                f"Total formatted columns: {len(formatted_conversions)}"
            ] + formatted_conversions, level="debug")
        else:
            self.logger.log_message("No converted columns to format.", level="debug")
        
        self.logger.log_end("_format_converted_columns")
        return formatted_conversions

    def _get_type_mapping(self) -> dict:
        """
        Returns a dictionary mapping JSON types to PySpark types.

        This function provides a mapping between JSON schema data types and PySpark data types.
        The mappings ensure compatibility when converting JSON data to PySpark DataFrames.

        Returns:
            dict: A dictionary where keys are JSON types and values are PySpark types.
        """
        type_mapping = {
            "string": StringType(),
            "boolean": BooleanType(),
            "number": FloatType(),  # Keeping FloatType for compatibility with original behavior
            "integer": IntegerType(),
            "long": LongType(),
            "double": FloatType(),  # Keeping FloatType to align with previous behavior
            "array": ArrayType(StringType()),  # Retain array structure with StringType elements
            "object": StructType([]),  # Default to an empty StructType for nested objects
            "datetime": TimestampType(),
            "decimal": FloatType(),  # Precision handling can be added if needed
            "date": DateType(),
            "binary": BinaryType(),
        }

        # Debug logging to verify the mappings
        self.logger.log_block("Type Mapping (DEBUG)", [
            f"{key}: {value.simpleString()}" for key, value in type_mapping.items()
        ], level="debug")

        return type_mapping
    
    def _reorder_columns(self, df: DataFrame) -> DataFrame:
        """
        Reorders the columns of the DataFrame to place 'input_file_name' as the first column.

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            DataFrame: Reordered DataFrame.
        """
        if 'input_file_name' in df.columns:
            columns = ['input_file_name'] + [col for col in df.columns if col != 'input_file_name']
            df = df.select(columns)
        return df
 
    def _sanitize_column_name(self, col_name: str) -> str:
        """
        Sanitizes column names to remove unsupported characters.

        Args:
            col_name (str): Original column name.

        Returns:
            str: Sanitized column name.
        """
        sanitized_name = col_name.split('}')[-1] if '}' in col_name else col_name
        return sanitized_name.replace('.', '_').replace(':', '_').replace('__', '_')

    def _read_binary_file(self, data_file_path: str, matched_data_files: List[str]) -> DataFrame:
        """
        Reads binary data files from the specified path.

        Args:
            data_file_path (str): Path to data files.
            matched_data_files (List[str]): List of file names to load.

        Returns:
            DataFrame: Binary DataFrame containing file content.
        """
        spark = SparkSession.builder.getOrCreate()
        file_paths = [f"dbfs:{data_file_path}/{file}" for file in matched_data_files]
        return spark.read.format("binaryFile").load(file_paths)

    def _json_schema_to_spark_struct(self, schema_file_path: str, definitions: Dict = None) -> Tuple[dict, StructType]:
        """
        Converts a JSON schema file to a PySpark StructType.

        Args:
            schema_file_path (str): Path to the JSON schema file.
            definitions (Dict, optional): Schema definitions for resolving references.

        Returns:
            Tuple[dict, StructType]: Parsed schema as a dictionary and corresponding StructType.
        """
        self.logger.log_start("_json_schema_to_spark_struct")

        try:
            # Step 1: Load the schema file
            schema_content = self.dbutils.fs.head(schema_file_path)
            json_schema = json.loads(schema_content)

            # Step 2: Retrieve or initialize definitions
            definitions = definitions or json_schema.get("definitions", {})

            # Helper function to resolve $ref fields
            def resolve_ref(ref):
                ref_path = ref.split("/")[-1]
                resolved = definitions.get(ref_path, {})
                if not resolved:
                    self.logger.log_warning(f"Reference '{ref_path}' not found in definitions.")
                return resolved

            # Helper function to parse field types
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
                if json_type == "string":
                    return StringType()
                if json_type == "integer":
                    return IntegerType()
                if json_type == "boolean":
                    return BooleanType()
                if json_type == "number":
                    return DoubleType()
                if json_type == "array":
                    items = field_props.get("items")
                    return ArrayType(parse_type(items) if items else StringType())
                if json_type == "object":
                    properties = field_props.get("properties", {})
                    return StructType([StructField(k, parse_type(v), True) for k, v in properties.items()])
                return StringType()

            # Step 3: Parse the JSON schema into a StructType
            struct = StructType([
                StructField(name, parse_type(props), True) for name, props in json_schema.get("properties", {}).items()
            ])

            # Step 4: Debug log for schema structure
            self.logger.log_block("Parsed Schema (DEBUG)", [
                json.dumps(json_schema, indent=2)[:1000],  # Log first 1000 characters of schema
                "Generated StructType:",
                str(struct)
            ], level="debug")

            self.logger.log_end("_json_schema_to_spark_struct", success=True)
            return json_schema, struct

        except Exception as e:
            error_message = f"Failed to convert schema file '{schema_file_path}' to StructType: {e}"
            self.logger.log_error(error_message)
            self.logger.log_end("_json_schema_to_spark_struct", success=False)
            raise ValueError(error_message)

    def _handle_json_processing(self, params: dict, depth_level: int) -> Tuple[DataFrame, DataFrame]:
        """
        Handles processing and flattening of JSON files.

        Args:
            params (dict): Parameters for JSON processing.
            depth_level (int): Depth level for flattening nested structures.

        Returns:
            Tuple[DataFrame, DataFrame]:
                - Initial parsed DataFrame
                - Flattened DataFrame
        """
        self.logger.log_start("_handle_json_processing")
        schema_file_path = params.get("schema_file_path")

        try:
            # Step 1: Check if schema file exists
            if schema_file_path:
                try:
                    self.dbutils.fs.ls(schema_file_path)  # Test if the path is accessible
                except Exception:
                    raise FileNotFoundError(f"Schema file not found at path: {schema_file_path}")

            # Step 2: Process JSON data
            df_initial, df_flattened = self._process_json_data(
                params=params,
                depth_level=depth_level
            )

            # Step 3: Log completion and return results
            self.logger.log_end("_handle_json_processing", success=True)
            return df_initial, df_flattened

        except Exception as e:
            self._handle_processing_error("_handle_json_processing", e)

    def _handle_xml_processing(self, params: dict, root_name: str, depth_level: int = None) -> Tuple[DataFrame, DataFrame]:
        """
        Handles processing and flattening of XML files.

        Args:
            params (dict): Parameters for XML processing.
            root_name (str): Root element name for XML parsing.
            depth_level (int, optional): Depth level for flattening nested structures. Defaults to None.

        Returns:
            Tuple[DataFrame, DataFrame]: Initial parsed DataFrame and the flattened DataFrame.
        """
        self.logger.log_start("_handle_xml_processing")

        # Debugging step: Log all parameters in a block
        self.logger.log_block("XML Processing Parameters (DEBUG)", [
            f"matched_data_files: {params.get('matched_data_files', 'None')}",
            f"schema_file_path: {params.get('schema_file_path', 'None')}",
            f"matched_schema_files: {params.get('matched_schema_files', 'None')}",
            f"root_name: {root_name}"
        ], level="debug")

        # Extract parameters
        matched_data_files = params.get("matched_data_files")
        schema_path = params.get("schema_file_path")
        matched_schema_files = params.get("matched_schema_files", [])

        try:
            # Process XML data
            df_initial, df_flattened = self._process_xml_data(
                matched_data_files=matched_data_files,
                schema_path=schema_path,
                matched_schema_files=matched_schema_files,
                root_name=root_name,
                depth_level=depth_level
            )

            # Log success
            self.logger.log_end("_handle_xml_processing", success=True)
            return df_initial, df_flattened

        except Exception as e:
            error_context = (
                f"Root Name: {root_name}, Depth Level: {depth_level}, "
                f"Matched Schemas: {matched_schema_files}"
            )
            self._handle_processing_error("_handle_xml_processing", f"{e} | Context: {error_context}")

    def _handle_xlsx_processing(self, params: dict, sheet_name: str) -> Tuple[DataFrame, DataFrame]:
        """
        Handles processing of XLSX files.

        Args:
            params (dict): Parameters for XLSX processing.
            sheet_name (str): Name of the sheet to process.

        Returns:
            Tuple[DataFrame, DataFrame]:
                - Initial parsed DataFrame
                - Flattened DataFrame
        """
        self.logger.log_start("_handle_xlsx_processing")

        try:
            # Step 1: Process XLSX data
            df_initial, df_flattened = self._process_xlsx_data(
                params=params,
                sheet_name=sheet_name
            )

            # Step 2: Log completion and return results
            self.logger.log_end("_handle_xlsx_processing", success=True)
            return df_initial, df_flattened

        except Exception as e:
            # Contextual error handling
            context = f"Sheet name: {sheet_name}, File path: {params.get('data_file_path')}"
            self._handle_processing_error("_handle_xlsx_processing", f"{e} | Context: {context}")

    def _handle_processing_error(self, method_name: str, exception: Exception):
        """
        Handles errors during processing, logs them, and raises a RuntimeError.

        Args:
            method_name (str): Name of the method where the error occurred.
            exception (Exception): Exception instance.
        """
        error_message = f"Error in {method_name}: {str(exception)}"
        self.logger.log_error(error_message)
        self.logger.log_end(method_name, success=False)
        raise RuntimeError(error_message)

    def _process_json_data(self, params: dict, depth_level: int) -> Tuple[DataFrame, DataFrame]:
        """
        Processes and flattens JSON files.

        Args:
            params (dict): Parameters for JSON processing.
            depth_level (int): Depth level for flattening nested structures.

        Returns:
            Tuple[DataFrame, DataFrame]:
                - Initial parsed DataFrame
                - Flattened DataFrame
        """
        self.logger.log_start("_process_json_data")

        schema_file_path = params.get("schema_file_path")
        data_file_path = params.get("data_file_path")
        num_files = len(params.get("matched_data_files", []))
        df_binary = params["df_binary"]

        # Initialize filtered_conversions
        filtered_conversions = []

        try:
            # Step 1: Load JSON schema if applicable
            schema = None
            if self.config.use_schema and schema_file_path:
                schema_json, schema = self._json_schema_to_spark_struct(schema_file_path)

                # Debug logging for schema content
                if schema_json:
                    self.logger.log_block("Loaded Schema Content (DEBUG)", [
                        json.dumps(schema_json, indent=2)[:1000]  # Log first 1000 characters
                    ], level="debug")

            # Debug logging for schema file path and number of files
            self.logger.log_block("Process JSON Data (DEBUG)", [
                f"Schema file path: {schema_file_path}",
                f"Data file path: {data_file_path}",
                f"Number of files to process: {num_files}"
            ], level="debug")

            # Step 2: Prepare JSON string and filename columns
            df_with_filename = df_binary.withColumn("json_string", F.col("content").cast("string")) \
                .withColumn("input_file_name", F.col("path"))

            # Step 3: Parse JSON data
            spark = SparkSession.builder.getOrCreate()
            if schema:
                df_parsed = spark.read.schema(schema).json(df_with_filename.select("json_string").rdd.map(lambda row: row.json_string))
            else:
                df_parsed = spark.read.json(df_with_filename.select("json_string").rdd.map(lambda row: row.json_string))

            # Step 4: Combine metadata and reorder columns
            self.logger.log_debug("Adding input_file_name and reordering columns...")
            input_file_name = df_with_filename.select("input_file_name").first()[0]
            df_initial = df_parsed.withColumn("input_file_name", F.lit(input_file_name))
            df_initial = self._reorder_columns(df_initial)

            # Print initial schema
            self.logger.log_dataframe_summary(df_initial, "Initial DataFrame", level="info")
            #self.logger.log_block("Initial DataFrame schema", [
            #    f"Loaded JSON initial schema: {schema_file_path}"
            #], level="info")
            #df_initial.printSchema()

            # Step 5: Flatten JSON DataFrame
            df_flattened, filtered_conversions = self._flatten_json_df(df_initial, depth_level)

            # Print flattened schema
            self.logger.log_dataframe_summary(df_flattened, "Flattened DataFrame", level="info")
            #self.logger.log_block("Flattened DataFrame schema", [
            #    f"Loaded JSON flattened schema: {schema_file_path}"
            #], level="info")
            #df_flattened.printSchema()

            # Step 6: Log results
            self.logger.log_block("JSON Processing Results (DEBUG)", [
                f"Initial DataFrame columns: {df_initial.columns}",
                f"Flattened DataFrame columns: {df_flattened.columns}",
                f"Number of converted columns: {len(filtered_conversions)}"
            ], level="debug")

            # Step 7: Log converted columns if any
            if filtered_conversions:
                formatted_conversions = self._format_converted_columns(filtered_conversions, df_flattened)
                if formatted_conversions:
                    self.logger.log_block("Formatted Converted Columns", formatted_conversions)

            self.logger.log_end("_process_json_data", success=True)
            return df_initial, df_flattened

        except Exception as e:
            error_context = f"Schema Path: {schema_file_path}, Data Path: {data_file_path}"
            self._handle_processing_error("_process_json_data", f"{e} | Context: {error_context}")

    def _process_xml_data(self, matched_data_files: List[str], schema_path: str, matched_schema_files: List[dict], root_name: str, depth_level: int = None) -> Tuple[DataFrame, DataFrame]:
        """
        Processes XML files and flattens the data.

        Args:
            matched_data_files (List[str]): List of matched XML file names.
            schema_path (str): Path to the XML schema file.
            matched_schema_files (List[dict]): List of matched schema files.
            root_name (str): Root name for the XML structure.
            depth_level (int, optional): Maximum depth level for flattening. Defaults to None.

        Returns:
            Tuple[DataFrame, DataFrame]: Initial parsed DataFrame and the flattened DataFrame.
        """
        self.logger.log_start("_process_xml_data")

        try:
            # Step 1: Resolve file paths
            resolved_file_paths = [f"dbfs:{self.config.source_data_folder_path}/{file}" for file in matched_data_files]
            self.logger.log_block("Resolved File Paths (DEBUG)", resolved_file_paths, level="debug")

            # Step 2: Combine paths into a single string
            self.logger.log_debug("Reading XML files in batch...")
            spark = SparkSession.builder.getOrCreate()
            df_initial = spark.read.format("xml").options(rowTag=root_name).load(",".join(resolved_file_paths))

            # Step 3: Sanitize column names
            sanitized_columns = {col: self._sanitize_column_name(col) for col in df_initial.columns}
            self.logger.log_block("Sanitized Columns (Before)", sanitized_columns, level="debug")

            for old_col, new_col in sanitized_columns.items():
                if old_col != new_col:
                    df_initial = df_initial.withColumnRenamed(old_col, new_col)

            self.logger.log_debug("Column names sanitized successfully.")

            # Step 4: Drop unwanted columns
            unwanted_columns = [col for col in df_initial.columns if "_xmlns" in col]
            if unwanted_columns:
                self.logger.log_info(f"Dropping unwanted '_xmlns' columns: {unwanted_columns}")
                df_initial = df_initial.drop(*unwanted_columns)

            # Step 5: Reorder and add input_file_name
            self.logger.log_debug("Adding input_file_name and reordering columns...")
            df_initial = df_initial.withColumn("input_file_name", F.input_file_name())
            df_initial = self._reorder_columns(df_initial)

            # Log initial DataFrame summary
            self.logger.log_dataframe_summary(df_initial, "Initial DataFrame", level="info")

            # Step 6: Flatten XML DataFrame
            self.logger.log_debug("Flattening XML DataFrame...")
            df_flattened, filtered_conversions = self._flatten_xml_df(df_initial, depth_level=depth_level)

            # Step 3: Sanitize column names
            sanitized_columns = {col: self._sanitize_column_name(col) for col in df_flattened.columns}
            self.logger.log_block("Sanitized Columns (After)", sanitized_columns, level="debug")

            # Log flattened DataFrame summary
            self.logger.log_dataframe_summary(df_flattened, "Flattened DataFrame", level="info")

            # Step 7: Log converted columns if any
            if filtered_conversions:
                formatted_conversions = self._format_converted_columns(filtered_conversions, df_flattened)
                if formatted_conversions:
                    self.logger.log_block("Formatted Converted Columns", formatted_conversions)

            # Step 8: Log results
            self.logger.log_block("JSON Processing Results (DEBUG)", [
                f"Initial DataFrame columns: {df_initial.columns}",
                f"Flattened DataFrame columns: {df_flattened.columns}",
                f"Number of converted columns: {len(filtered_conversions)}"
            ], level="debug")

            self.logger.log_end("_process_xml_data", success=True)
            return df_initial, df_flattened

        except Exception as e:
            error_message = f"Error in _process_xml_data: {e}"
            self.logger.log_error(error_message)
            self.logger.log_end("_process_xml_data", success=False)
            raise RuntimeError(error_message)

    def _process_xlsx_data(self, params: dict, sheet_name: str) -> Tuple[DataFrame, DataFrame]:
        """
        Processes XLSX files into DataFrames.

        Args:
            params (dict): Parameters for XLSX processing.
            sheet_name (str): Name of the sheet to process.

        Returns:
            Tuple[DataFrame, DataFrame]:
                - Initial parsed DataFrame
                - Flattened DataFrame
        """
        self.logger.log_start("_process_xlsx_data")

        data_file_path = params.get("data_file_path")
        num_files = len(params.get("matched_data_files", []))
        df_binary = params["df_binary"]

        # Initialize filtered_conversions
        filtered_conversions = []

        try:
            # Debug logging for file path and sheet name
            self.logger.log_block("Process XLSX Data (DEBUG)", [
                f"Data file path: {data_file_path}",
                f"Sheet name: {sheet_name}",
                f"Number of files to process: {num_files}"
            ], level="debug")

            # Step 1: Read binary content and convert to pandas DataFrame
            content = df_binary.select("content").collect()[0][0]
            raw_data = pd.read_excel(BytesIO(content), engine="openpyxl", sheet_name=sheet_name)

            # Step 2: Clean up column names
            raw_data.columns = raw_data.columns.str.replace(" ", "_").str.replace(":", "_")
            raw_data = raw_data.dropna(how="all")

            # Step 3: Add input_file_name metadata
            self.logger.log_debug("Adding input_file_name and reordering columns...")
            input_file_name = df_binary.select("path").first()[0]
            raw_data["input_file_name"] = input_file_name

            # Step 4: Convert to Spark DataFrame and reorder columns
            spark = SparkSession.builder.getOrCreate()
            df_initial = spark.createDataFrame(raw_data)

            # Reorder columns to ensure input_file_name is the first column
            df_initial = self._reorder_columns(df_initial)

            # Print initial schema
            self.logger.log_dataframe_summary(df_initial, "Initial DataFrame", level="info")
            #self.logger.log_block("Initial DataFrame schema (DEBUG)", [
            #    f"Loaded XLSX initial schema for sheet: {sheet_name}"
            #], level="info")
            #df_initial.printSchema()

            # Step 5: Flatten XLSX DataFrame and apply type conversions
            df_flattened, filtered_conversions = self._flatten_xlsx_df(df_initial)

            # Print flattened schema
            self.logger.log_dataframe_summary(df_flattened, "Flattened DataFrame", level="info")
            #self.logger.log_block("Flattened DataFrame schema (DEBUG)", [
            #    f"Loaded XLSX flattened schema for sheet: {sheet_name}"
            #], level="info")
            #df_flattened.printSchema()

            # Step 6: Log results
            self.logger.log_block("XLSX Processing Results (DEBUG)", [
                f"Initial DataFrame columns: {df_initial.columns}",
                f"Flattened DataFrame columns: {df_flattened.columns}",
                f"Number of converted columns: {len(filtered_conversions)}"
            ], level="debug")

            # Step 7: Log converted columns if any
            if filtered_conversions:
                formatted_conversions = self._format_converted_columns(filtered_conversions, df_flattened)
                if formatted_conversions:
                    self.logger.log_block("Columns converted by _get_type_mapping", formatted_conversions)

            self.logger.log_end("_process_xlsx_data", success=True)
            return df_initial, df_flattened

        except Exception as e:
            error_context = f"Sheet Name: {sheet_name}, Data Path: {data_file_path}"
            self._handle_processing_error("_process_xlsx_data", f"{e} | Context: {error_context}")

    def _flatten_json_df(self, input_df: DataFrame, depth_level: int = None, root_level: int = 0) -> Tuple[DataFrame, List[Tuple[str, str, str]]]:
        """
        Flattens a JSON DataFrame up to a specified depth level.

        Args:
            input_df (DataFrame): Initial DataFrame to flatten.
            depth_level (int, optional): Maximum depth level for flattening. Defaults to None (no limit).
            root_level (int, optional): Starting level for recursion. Defaults to 0.

        Returns:
            Tuple[DataFrame, List[Tuple[str, str, str]]]:
                - Flattened DataFrame
                - List of columns with type conversions (name, original_type, new_type)
        """
        self.logger.log_start("_flatten_json_df")
        self.logger.log_message(f"Flattening JSON DataFrame starting at root level {root_level} with depth level {depth_level or 'unlimited'}", level="info")

        type_mapping = self._get_type_mapping()
        converted_columns = []

        def sanitize_column_name(col_name):
            return col_name.replace(".", "_")

        def apply_type_mapping(column_name, data_type):
            """Applies type mapping to array/struct columns."""
            if isinstance(data_type, ArrayType):
                if depth_level and root_level + 1 >= depth_level:
                    return StringType()
                return ArrayType(apply_type_mapping(column_name, data_type.elementType))
            if isinstance(data_type, StructType):
                return StructType([
                    StructField(f.name, apply_type_mapping(f"{column_name}.{f.name}", f.dataType), f.nullable)
                    for f in data_type.fields
                ])
            original_type = data_type.simpleString()
            new_type = type_mapping.get(original_type, data_type)
            if original_type != new_type.simpleString():
                converted_columns.append((sanitize_column_name(column_name), original_type, new_type.simpleString()))
            return new_type

        # Apply type mapping
        df_flattened = input_df.select([
            F.col(c).cast(apply_type_mapping(c, input_df.schema[c].dataType)).alias(sanitize_column_name(c)) for c in input_df.columns
        ])

        # Flatten recursively
        while not depth_level or root_level < depth_level:
            complex_fields = {f.name: f.dataType for f in df_flattened.schema.fields if isinstance(f.dataType, (StructType, ArrayType))}
            if not complex_fields:
                break
            for col_name, data_type in complex_fields.items():
                if depth_level and root_level + 1 == depth_level:
                    df_flattened = df_flattened.withColumn(sanitize_column_name(col_name), F.to_json(F.col(col_name)))
                elif isinstance(data_type, ArrayType):
                    df_flattened = self._flatten_array_column(df_flattened, col_name)
                elif isinstance(data_type, StructType):
                    df_flattened = self._flatten_struct_column(df_flattened, col_name)
            root_level += 1

        # Ensure converted_columns is filtered before returning
        filtered_conversions = [(col, orig, new) for col, orig, new in converted_columns if col in df_flattened.columns]

        # Log type conversions
        if filtered_conversions:
            self.logger.log_block("Type Conversions (DEBUG)", [
                f"Column: {col}, Original Type: {orig}, New Type: {new}"
                for col, orig, new in filtered_conversions
            ], level="debug")

        self.logger.log_end("_flatten_json_df")
        return df_flattened, filtered_conversions

    def _flatten_xml_df(self, input_df: DataFrame, depth_level: int = None, root_level: int = 0) -> Tuple[DataFrame, List[Tuple[str, str, str]]]:
        """
        Flattens an XML DataFrame up to a specified depth level.

        Args:
            input_df (DataFrame): Initial XML DataFrame to flatten.
            depth_level (int, optional): Maximum depth level for flattening. Defaults to None (no limit).
            root_level (int, optional): Starting level for recursion. Defaults to 0.

        Returns:
            Tuple[DataFrame, List[Tuple[str, str, str]]]:
                - Flattened DataFrame
                - List of columns with type conversions (name, original_type, new_type)
        """
        self.logger.log_start("_flatten_xml_df")
        self.logger.log_message(f"Flattening XML DataFrame starting at root level {root_level}/{depth_level or 'unlimited'}", level="info")

        type_mapping = self._get_type_mapping()
        converted_columns = []

        #def sanitize_column_name(col_name):
        #    return col_name.replace(":", "_").replace(".", "_").replace("__", "_")

        def apply_type_mapping(column_name, data_type):
            """Applies type mapping to array/struct columns."""
            if isinstance(data_type, ArrayType):
                if depth_level and root_level + 1 >= depth_level:
                    return StringType()  # Convert to JSON string when depth level is reached
                return ArrayType(apply_type_mapping(column_name, data_type.elementType))
            if isinstance(data_type, StructType):
                if depth_level and root_level + 1 >= depth_level:
                    return StringType()  # Convert to JSON string when depth level is reached
                return StructType([
                    StructField(f.name, apply_type_mapping(f"{column_name}.{f.name}", f.dataType), f.nullable)
                    for f in data_type.fields
                ])
            original_type = data_type.simpleString()
            new_type = type_mapping.get(original_type, data_type)
            if original_type != new_type.simpleString():
                converted_columns.append((self._sanitize_column_name(column_name), original_type, new_type.simpleString()))
            return new_type

        # Step 1: Sanitize column names and apply type mapping
        df_flattened = input_df.select([
            F.col(c).cast(apply_type_mapping(c, input_df.schema[c].dataType)).alias(self._sanitize_column_name(c))
            for c in input_df.columns
        ])

        # Step 2: Drop unwanted columns
        unwanted_columns = [col for col in df_flattened.columns if "_xmlns" in col]
        if unwanted_columns:
            self.logger.log_info(f"Dropping unwanted '_xmlns' columns: {unwanted_columns}")
            df_flattened = df_flattened.drop(*unwanted_columns)

        # Step 3: Recursive flattening for complex fields
        complex_fields = {f.name: f.dataType for f in df_flattened.schema.fields if isinstance(f.dataType, (StructType, ArrayType))}
        while complex_fields and (depth_level is None or root_level < depth_level):
            for col_name, data_type in complex_fields.items():
                self.logger.log_debug(f"Processing column '{col_name}' with type '{data_type.simpleString()}'")
                if isinstance(data_type, ArrayType) or isinstance(data_type, StructType):
                    if depth_level and root_level + 1 == depth_level:
                        df_flattened = df_flattened.withColumn(self._sanitize_column_name(col_name), F.to_json(F.col(col_name)))
                    elif isinstance(data_type, ArrayType):
                        df_flattened = self._flatten_array_column(df_flattened, col_name)
                    elif isinstance(data_type, StructType):
                        df_flattened = self._flatten_struct_column(df_flattened, col_name)

            # Update complex fields after each iteration
            complex_fields = {f.name: f.dataType for f in df_flattened.schema.fields if isinstance(f.dataType, (StructType, ArrayType))}
            root_level += 1

        # Step 4: Final sanitization for consistency
        sanitized_columns = {col: self._sanitize_column_name(col) for col in df_flattened.columns}
        for old_col, new_col in sanitized_columns.items():
            if old_col != new_col:
                df_flattened = df_flattened.withColumnRenamed(old_col, new_col)

        self.logger.log_debug(f"Final Sanitized Columns: {sanitized_columns}")

        # Step 5: Log type conversions
        filtered_conversions = [(col, orig, new) for col, orig, new in converted_columns if col in df_flattened.columns]
        if filtered_conversions:
            self.logger.log_block("Type Conversions (DEBUG)", [
                f"Column: {col}, Original Type: {orig}, New Type: {new}"
                for col, orig, new in filtered_conversions
            ], level="debug")

        self.logger.log_end("_flatten_xml_df")
        return df_flattened, filtered_conversions

    def _flatten_xlsx_df(self, input_df: DataFrame) -> Tuple[DataFrame, List[Tuple[str, str, str]]]:
        """
        Processes an XLSX DataFrame as flattened data, including type conversions.

        Args:
            input_df (DataFrame): Initial XLSX DataFrame.

        Returns:
            Tuple[DataFrame, List[Tuple[str, str, str]]]:
                - Flattened DataFrame
                - List of columns with type conversions (name, original_type, new_type)
        """
        self.logger.log_start("_flatten_xlsx_df")
        self.logger.log_message("Processing XLSX DataFrame (type conversions applied)", level="info")

        type_mapping = self._get_type_mapping()
        converted_columns = []

        def sanitize_column_name(col_name):
            return col_name.replace(" ", "_").replace(":", "_")

        def apply_type_mapping(column_name, data_type):
            """Applies type mapping for XLSX columns."""
            original_type = data_type.simpleString()
            new_type = type_mapping.get(original_type, data_type)
            if original_type != new_type.simpleString():
                converted_columns.append((sanitize_column_name(column_name), original_type, new_type.simpleString()))
            return new_type

        # Apply type mapping and sanitize column names
        df_flattened = input_df.select([
            F.col(c).cast(apply_type_mapping(c, input_df.schema[c].dataType)).alias(sanitize_column_name(c)) for c in input_df.columns
        ])

        # Ensure converted_columns is filtered before returning
        filtered_conversions = [(col, orig, new) for col, orig, new in converted_columns if col in df_flattened.columns]

        # Log type conversions
        if filtered_conversions:
            self.logger.log_block("Type Conversions (DEBUG)", [
                f"Column: {col}, Original Type: {orig}, New Type: {new}"
                for col, orig, new in filtered_conversions
            ], level="debug")

        self.logger.log_end("_flatten_xlsx_df")
        return df_flattened, filtered_conversions

    def process_and_flatten_data(self, depth_level: int = None) -> Tuple[DataFrame, DataFrame]:
        """
        Main function to process and flatten JSON, XML, or XLSX data.

        Args:
            depth_level (int, optional): Depth level for flattening nested structures.

        Returns:
            Tuple[DataFrame, DataFrame]:
                - Initial parsed DataFrame
                - Flattened DataFrame
        """
        self.logger.log_start("process_and_flatten_data")

        try:
            # Step 1: Retrieve validated paths and file lists
            matched_data_files = self.validator.matched_data_files
            matched_schema_files = self.validator.matched_schema_files \
                if self.config.use_schema else None
            data_file_path = self.config.source_data_folder_path
            source_filename = self.config.source_filename
            use_schema = self.config.use_schema
            schema_file_path = f"{self.config.source_schema_folder_path}/{self.validator.main_schema_name}" \
                if self.config.use_schema else None

            # Debug logging for essential paths
            self.logger.log_block("Data Flattening Processing", [
                f"Use schema: {use_schema}",
                f"Depth level of flattening: {depth_level}",
                f"Schema file path: {schema_file_path}",
                f"Data file path: {data_file_path}",
                f"Source_filename: {source_filename}",
                f"Number of files to process: {len(matched_data_files)}"
            ], level="info")

            # Step 2: Exit early if no files are found
            if not matched_data_files:
                self.logger.log_warning("No matching data files provided. Exiting process.")
                return None, None

            # Step 3: Prepare common parameters
            common_params = {
                "schema_file_path": schema_file_path,
                "matched_schema_files": matched_schema_files,
                "data_file_path": data_file_path,
                "matched_data_files": matched_data_files,
                "df_binary": self._read_binary_file(data_file_path, matched_data_files)
            }

            # Step 4: Dispatch processing based on file type
            file_type = self.config.file_type.lower()

            dispatch_map = {
                # Handles processing and flattening of JSON files
                "json": lambda: self._handle_json_processing(common_params, depth_level)[:2],
                # Handles processing and flattening of XML files
                "xml": lambda: self._handle_xml_processing(common_params, self.config.xml_root_name, depth_level)[:2],
                # Handles processing of XLSX files
                "xlsx": lambda: self._handle_xlsx_processing(common_params, self.config.sheet_name)[:2]
            }

            if file_type not in dispatch_map:
                raise ValueError(f"Unsupported file type: {file_type}")

            # Step 5: Execute the appropriate processing function
            try:
                result = dispatch_map[file_type]()
                if len(result) != 2:
                    raise ValueError(f"Unexpected number of return values: {len(result)}")
                df_initial, df_flattened = result
            except ValueError as ve:
                self.logger.log_error(f"Error in dispatch processing: {ve}")
                raise

            # Step 6: Log success and return results
            self.logger.log_end("process_and_flatten_data", success=True)
            return df_initial, df_flattened

        except Exception as e:
            self._handle_processing_error("process_and_flatten_data", e)