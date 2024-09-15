# File: custom_utils/transformations/dataframe.py

import json
import pyspark.sql.functions as F
from typing import List
from pyspark.sql.types import ArrayType, StructType, StringType
from pyspark.sql import SparkSession, DataFrame
from custom_utils.dp_storage import reader, writer
from custom_utils.logging.logger import Logger  # Import the Logger class
from custom_utils.config.config import Config # Import the Config class

# Create an instance of Logger (can be passed from outside or use default)
logger = Logger(debug=True)  # Set debug=True or False based on the requirement
config = Config(debug=True)  # Set debug=True or False based on the requirement

def _get_array_and_struct_columns(df: DataFrame) -> List[tuple]:
    """Return a list with columns (names and types) of either ArrayType or StructType."""
    complex_columns = [
        (field.name, type(field.dataType))
        for field in df.schema.fields
        if isinstance(field.dataType, (ArrayType, StructType))
    ]
    return complex_columns

def _get_expanded_columns_with_aliases(df: DataFrame, column_name: str, layer_separator: str = "_") -> List[F.col]:
    """Return a list of nested columns (pyspark.sql.column.Column) in a column struct to be used in df.select()."""
    expanded_columns = []
    for nested_column in df.select(f"`{column_name}`.*").columns:
        expanded_column = f"`{column_name}`.`{nested_column}`"
        expanded_column_alias = f"{column_name}{layer_separator}{nested_column}"
        expanded_columns.append(F.col(expanded_column).alias(expanded_column_alias))
    return expanded_columns

def flatten_array_column(df: DataFrame, column_name: str) -> DataFrame:
    """Return dataframe with flattened array column."""
    try:
        return df.withColumn(column_name, F.explode_outer(F.col(column_name)))
    except Exception as e:
        logger.log_message(f"Error flattening array column '{column_name}': {e}", level="error")
        logger.exit_notebook(f"Error flattening array column '{column_name}'")

def flatten_struct_column(df: DataFrame, column_name: str, layer_separator: str = "_") -> DataFrame:
    """Return dataframe with flattened struct column."""
    try:
        expanded_columns = _get_expanded_columns_with_aliases(df, column_name, layer_separator)
        return df.select("*", *expanded_columns).drop(column_name)
    except Exception as e:
        logger.log_message(f"Error flattening struct column '{column_name}': {e}", level="error")
        logger.exit_notebook(f"Error flattening struct column '{column_name}'")

def flatten(df: DataFrame, layer_separator: str = "_") -> DataFrame:
    """Return dataframe with flattened arrays and structs."""
    try:
        complex_columns = _get_array_and_struct_columns(df)
        while complex_columns:
            column_name, data_type = complex_columns[0]
            if data_type == StructType:
                df = flatten_struct_column(df, column_name, layer_separator)
            elif data_type == ArrayType:
                df = flatten_array_column(df, column_name)
            complex_columns = _get_array_and_struct_columns(df)
        return df
    except Exception as e:
        logger.log_message(f"Error flattening DataFrame: {e}", level="error")
        logger.exit_notebook(f"Error flattening DataFrame")

def flatten_df(
    df: DataFrame,
    depth_level: int = None,
    current_level: int = 0,
    max_depth: int = 1,
    type_mapping: dict = None,
) -> DataFrame:
    """
    Flattens complex fields in a DataFrame up to a specified depth level.
    
    Args:
        df (DataFrame): A PySpark DataFrame containing nested structures.
        depth_level (int, optional): The maximum depth level to flatten. Uses config if not provided.
        current_level (int): The current depth level (used internally).
        max_depth (int): The maximum depth calculated from the schema.
        type_mapping (dict, optional): A dictionary mapping original types to desired Spark SQL types.

    Returns:
        DataFrame: A flattened DataFrame.
    """
    try:
        # Determine the depth level to flatten to
        depth_level = max_depth if depth_level is None or depth_level == "" else depth_level
        if current_level >= depth_level:
            return df

        # Apply custom type mappings if provided
        if type_mapping:
            df = df.select(
                [
                    F.col(c).cast(
                        type_mapping.get(df.schema[c].dataType.simpleString(), df.schema[c].dataType)
                    )
                    for c in df.columns
                ]
            )

        # Identify columns with complex data types
        complex_fields = {
            field.name: field.dataType
            for field in df.schema.fields
            if isinstance(field.dataType, (ArrayType, StructType))
        }

        while complex_fields and current_level < depth_level:
            for col_name, data_type in complex_fields.items():
                if current_level + 1 == depth_level:
                    df = df.withColumn(col_name, F.to_json(F.col(col_name)))
                else:
                    if isinstance(data_type, ArrayType):
                        df = df.withColumn(col_name, F.explode_outer(F.col(col_name)))
                    if isinstance(data_type, StructType):
                        expanded_columns = [
                            F.col(f"{col_name}.{k}").alias(f"{col_name}_{k}")
                            for k in data_type.fieldNames()
                        ]
                        df = df.select("*", *expanded_columns).drop(col_name)

            # Re-evaluate complex fields after flattening
            complex_fields = {
                field.name: field.dataType
                for field in df.schema.fields
                if isinstance(field.dataType, (ArrayType, StructType))
            }
            current_level += 1

        # Convert any remaining complex fields to strings if the depth is reached
        if current_level >= depth_level:
            df = df.select(
                [
                    F.col(c).cast(StringType()) if isinstance(df.schema[c].dataType, (ArrayType, StructType)) else F.col(c)
                    for c in df.columns
                ]
            )

        # Rename columns to replace '__' and '.' with '_'
        df = rename_columns(df, replacements={"__": "_", ".": "_"})
        return df

    except Exception as e:
        logger.log_message(f"Error flattening DataFrame: {e}", level="error")
        logger.exit_notebook(f"Error flattening DataFrame")

def _string_replace(s: str, replacements: dict) -> str:
    """Helper function to perform multiple string replacements."""
    for string_before, string_after in replacements.items():
        s = s.replace(string_before, string_after)
    return s

def rename_columns(df: DataFrame, replacements: dict) -> DataFrame:
    """Renames DataFrame columns based on a dictionary of replacement rules."""
    for column_name in df.columns:
        df = df.withColumnRenamed(column_name, _string_replace(column_name, replacements))
    return df

def rename_and_cast_columns(
    df: DataFrame, column_mapping: dict = None, cast_type_mapping: dict = None
) -> DataFrame:
    """Renames specified columns and optionally casts them to a different data type."""
    if column_mapping:
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

    if cast_type_mapping:
        for col_name, new_type in cast_type_mapping.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(new_type))

    return df

def add_columns_that_are_not_in_df(df: DataFrame, column_names: List[str]) -> DataFrame:
    """Add columns in column_names that are not already in dataframe."""
    for column_name in column_names:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None))
            logger.log_message(f'Column "{column_name}" was added.', level="info")
    return df

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
        logger.log_message(f"Error processing binary JSON files: {e}", level="error")
        logger.exit_notebook(f"Error processing binary JSON files: {e}")

def process_and_flatten_json(
    schema_file_path,
    data_file_path,
    logger=None,
    debug=False
) -> tuple:
    """
    Orchestrates the JSON processing pipeline from schema reading to DataFrame flattening.

    Args:
        schema_file_path (str): Path to the JSON schema file.
        data_file_path (str): Path to the data file(s).
        logger (Logger, optional): Logger object for logging. Defaults to None.
        debug (bool, optional): If True, enables debug logging. Defaults to False.

    Returns:
        tuple: Original DataFrame and the flattened DataFrame.
    """
    try:
        # Get the active Spark session
        spark = SparkSession.builder.getOrCreate()

        # Reading schema and parsing JSON to Spark StructType
        schema_json, schema = writer.json_schema_to_spark_struct(schema_file_path)

        if logger:
            logger.log_message(f"Schema file path: {schema_file_path}", level="info")
            logger.log_message(f"Data file path: {data_file_path}", level="info")

            if debug:
                # Pretty print the schema JSON
                logger.log_message(f"Schema JSON:\n{json.dumps(schema_json, indent=4)}", level="info")

        # Read the JSON data with binary fallback
        df = read_json_from_binary(spark, schema, data_file_path)

        if logger and debug:
            # Log the initial DataFrame schema and row count
            logger.log_message("Initial DataFrame schema:", level="info")
            df.printSchema()
            initial_row_count = df.count()
            logger.log_message(f"Initial DataFrame row count: {initial_row_count}", level="info")

        # Get the depth level and flatten the DataFrame
        max_depth = reader.get_json_depth(schema_json, logger=logger, depth_level=config.depth_level)
        df_flattened = flatten_df(df, depth_level=config.depth_level, max_depth=max_depth, type_mapping=reader.get_type_mapping())

        if logger and debug:
            # Log the flattened DataFrame schema and row count
            logger.log_message("Flattened DataFrame schema:", level="info")
            df_flattened.printSchema()
            flattened_row_count = df_flattened.count()
            logger.log_message(f"Flattened DataFrame row count: {flattened_row_count}", level="info")

        # Drop the "input_file_name" column from the original DataFrame
        df = df.drop("input_file_name")

        if logger:
            logger.log_message("Completed JSON processing and flattening.", level="info")

        return df, df_flattened

    except Exception as e:
        if logger:
            logger.log_message(f"Error during processing and flattening: {str(e)}", level="error")
        raise

def create_temp_view_with_most_recent_records(
    spark,
    view_name: str,
    key_columns: str,
    columns_of_interest: str,
    order_by_columns: list = ["input_file_name DESC"],
    logger=None
) -> str:
    """Creates a temporary view with the most recent version of records based on key columns and ordering logic."""
    try:
        if not key_columns:
            raise ValueError("ERROR: No KeyColumns provided!")

        key_columns_list = [col.strip() for col in key_columns.split(',')]
        temp_view_name = f"temp_{view_name}"
        new_data_sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW {temp_view_name} AS
        SELECT {columns_of_interest}
        FROM (
            SELECT t.*, 
                   row_number() OVER (PARTITION BY {', '.join(key_columns_list)} 
                                      ORDER BY {', '.join(order_by_columns)}) AS rnr
            FROM {view_name} t
        ) x
        WHERE rnr = 1;
        """

        if logger:
            logger.log_message(f"Constructed SQL query: {new_data_sql}", level="info")

        spark.sql(new_data_sql)
        if logger:
            logger.log_message(f"Temporary view {temp_view_name} created successfully.", level="info")

        return temp_view_name

    except ValueError as ve:
        if logger:
            logger.log_message(f"Configuration Error: {ve}", level="error")
        raise

    except Exception as e:
        if logger:
            logger.log_message(f"Error creating temporary view {temp_view_name}: {e}", level="error")
        raise