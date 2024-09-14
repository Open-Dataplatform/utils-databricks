from typing import List
from pyspark.sql.types import ArrayType, StructType, StringType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from custom_utils.dp_storage import reader, writer
from custom_utils.logging.logger import Logger  # Import the Logger class

# Create an instance of Logger (this should ideally be passed from outside)
logger = Logger(debug=True)  # Set debug=True or False based on the requirement

def _get_array_and_struct_columns(df):
    """Return list with columns (names and types) of either ArrayType or StructType"""
    complex_columns = []
    for field in df.schema.fields:
        data_type = type(field.dataType)
        if data_type == ArrayType or data_type == StructType:
            complex_columns.append((field.name, data_type))
    return complex_columns

def flatten(df, layer_separator="_"):
    """Return dataframe with flattened arrays and structs."""
    complex_columns = _get_array_and_struct_columns(df)

    while len(complex_columns) != 0:
        column_name, data_type = complex_columns[0]

        if data_type == StructType:
            df = flatten_struct_column(df, column_name, layer_separator)
        elif data_type == ArrayType:
            df = flatten_array_column(df, column_name)

        complex_columns = _get_array_and_struct_columns(df)
    
    return df

def read_json_from_binary(spark: SparkSession, schema: StructType, data_file_path: str) -> DataFrame:
    """
    Reads files as binary, parses the JSON content, and ensures that the `input_file_name` is correctly
    associated with each row in the resulting DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        schema (StructType): The schema to enforce on the JSON data.
        data_file_path (str): The path to the data file(s).

    Returns:
        DataFrame: The DataFrame parsed from the JSON content, with `input_file_name` as the first column.
    """
    try:
        # Load all files as binary and extract relevant columns
        binary_df = spark.read.format("binaryFile").load(data_file_path)

        # Add necessary columns including JSON content as string, input file path, and a unique ID
        df_with_filename = (
            binary_df.withColumn("json_string", F.col("content").cast("string"))
            .withColumn("input_file_name", F.col("path"))
            .withColumn("id", F.monotonically_increasing_id())
        )

        # Parse the JSON content using the provided schema
        df_parsed = (
            spark.read.schema(schema)
            .json(
                df_with_filename.select("json_string").rdd.map(
                    lambda row: row.json_string
                )
            )
            .withColumn("id", F.monotonically_increasing_id())
        )

        # Join the parsed DataFrame with the original binary DataFrame on the unique ID
        df_final_with_filename = df_parsed.join(df_with_filename, on="id", how="inner")

        # Drop unnecessary columns after join
        df_final_with_filename = df_final_with_filename.drop(
            "json_string", "content", "path", "modificationTime", "length", "id"
        )

        # Reorder columns to have `input_file_name` as the first column
        columns = ["input_file_name"] + [
            col for col in df_final_with_filename.columns if col != "input_file_name"
        ]
        df_final_with_filename = df_final_with_filename.select(columns)

        return df_final_with_filename

    except Exception as e:
        # Handle and log any errors during processing
        raise RuntimeError(f"Error processing binary JSON files: {str(e)}")

def process_and_flatten_json(spark, config, schema_file_path, data_file_path, logger=None, depth_level=None, type_mapping=None) -> tuple:
    """
    Orchestrates the JSON processing pipeline from schema reading to DataFrame flattening.
    """
    # Use the provided depth_level or fallback to the config value
    depth_level = depth_level if depth_level is not None else config.depth_level

    # If type_mapping is None (default), use reader.get_type_mapping()
    if type_mapping is None:
        type_mapping = reader.get_type_mapping()

    # Convert the JSON schema to PySpark StructType and retrieve the original JSON schema
    schema_json, schema = writer.json_schema_to_spark_struct(schema_file_path)

    # Read and parse the JSON data with binary fallback
    df = read_json_from_binary(spark, schema, data_file_path)

    # Determine the maximum depth of the JSON schema
    max_depth = reader.get_json_depth(schema_json, logger=logger, depth_level=depth_level)

    # Flatten the DataFrame based on the depth level
    df_flattened = flatten_df(df, depth_level=depth_level, max_depth=max_depth, type_mapping=type_mapping)

    # Drop the "input_file_name" column from the original DataFrame
    df = df.drop("input_file_name")

    # Log the completion of the process
    if logger:
        logger.log_message("Completed JSON processing and flattening.", level="info")

    # Return both the schema DataFrame and the flattened DataFrame
    return df, df_flattened

def create_temp_view_with_most_recent_records(
    spark,
    view_name: str,
    key_columns: str,
    columns_of_interest: str,
    order_by_columns: list = ["input_file_name DESC"],
    logger=None  # Replace helper with logger
) -> str:
    """
    Creates a temporary view with the most recent version of records based on key columns and ordering logic.
    """
    try:
        # Ensure key columns are provided
        if not key_columns:
            raise ValueError("ERROR: No KeyColumns provided!")

        # Create a list of key columns and trim any extra whitespace
        key_columns_list = [col.strip() for col in key_columns.split(',')]

        # Construct the SQL query to create the temporary view
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

        # Log the constructed SQL query for debugging
        if logger:
            logger.log_message(f"Constructed SQL query: {new_data_sql}", level="info")

        # Execute the SQL query to create the temporary view
        spark.sql(new_data_sql)
        if logger:
            logger.log_message(f"Temporary view {temp_view_name} created successfully.", level="info")

        # Return the name of the temporary view
        return temp_view_name

    except ValueError as ve:
        if logger:
            logger.log_message(f"Configuration Error: {ve}", level="error")
        raise

    except Exception as e:
        if logger:
            logger.log_message(f"Error creating temporary view {temp_view_name}: {e}", level="error")
        raise