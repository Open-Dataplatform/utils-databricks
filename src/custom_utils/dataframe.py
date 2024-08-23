from typing import List
from pyspark.sql.types import ArrayType, StructType, StringType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from custom_utils.dp_storage import reader, writer


def _get_array_and_struct_columns(df):
    """Return list with columns (names and types) of either ArrayType or StructType"""
    complex_columns = []
    for field in df.schema.fields:
        data_type = type(field.dataType)
        if data_type == ArrayType or data_type == StructType:
            complex_columns.append((field.name, data_type))

    return complex_columns


def _get_expanded_columns_with_aliases(df, column_name, layer_separator="_"):
    """Return list of nested columns (pyspark.sql.column.Column) in a column struct. To be used in df.select().

    layer_separator is used as the separator in the new column name between the column and nested column names.
    """
    expanded_columns = []
    for nested_column in df.select(f"`{column_name}`.*").columns:
        expanded_column = f"`{column_name}`.`{nested_column}`"
        expanded_column_alias = f"{column_name}{layer_separator}{nested_column}"

        expanded_columns.append(F.col(expanded_column).alias(expanded_column_alias))

    return expanded_columns


def flatten_array_column(df, column_name):
    """Return dataframe with flattened array column."""
    df = df.withColumn(column_name, F.explode_outer(F.col(column_name)))
    return df


def flatten_struct_column(df, column_name, layer_separator="_"):
    """Return dataframe with flattened struct column."""
    expanded_columns = _get_expanded_columns_with_aliases(
        df, column_name, layer_separator
    )
    df = df.select("*", *expanded_columns).drop(column_name)
    return df


def flatten(df, layer_separator="_"):
    """Return dataframe with flattened arrays and structs.

    Written with inspiration from https://www.youtube.com/watch?v=jD8JIw1FVVg.
    """
    complex_columns = _get_array_and_struct_columns(df)

    while len(complex_columns) != 0:

        column_name, data_type = complex_columns[0]

        if data_type == StructType:
            df = flatten_struct_column(df, column_name, layer_separator)
        elif data_type == ArrayType:
            df = flatten_array_column(df, column_name)

        complex_columns = _get_array_and_struct_columns(df)

    return df


def flatten_df(
    df: DataFrame,
    depth_level=None,
    current_level=0,
    max_depth=1,
    type_mapping: dict = None,
) -> DataFrame:
    """
    Flattens complex fields in a DataFrame up to a specified depth level, applies type mapping,
    and renames columns by replacing '__' and '.' with '_'.

    Args:
        df (DataFrame): A PySpark DataFrame containing nested structures.
        depth_level (int, optional): The maximum depth level to flatten. If None, max_depth is used.
        current_level (int): The current depth level (used internally during recursion).
        max_depth (int): The maximum depth calculated from the schema.
        type_mapping (dict, optional): A dictionary mapping original types to desired Spark SQL types.

    Returns:
        DataFrame: A flattened DataFrame with custom types applied and column names adjusted.
    """
    # Determine the depth level to flatten to
    depth_level = max_depth if depth_level is None or depth_level == "" else depth_level

    # Exit early if the current level has reached or exceeded the depth level
    if current_level >= depth_level:
        return df

    # Apply custom type mappings if provided
    if type_mapping:
        df = df.select(
            [
                F.col(c).cast(
                    type_mapping.get(
                        df.schema[c].dataType.simpleString(), df.schema[c].dataType
                    )
                )
                for c in df.columns
            ]
        )

    # Identify columns with complex data types (arrays and structs)
    complex_fields = {
        field.name: field.dataType
        for field in df.schema.fields
        if isinstance(field.dataType, (ArrayType, StructType))
    }

    # Flatten each complex field based on its data type
    while complex_fields and current_level < depth_level:
        for col_name, data_type in complex_fields.items():
            if current_level + 1 == depth_level:
                # Convert nested structures to JSON strings if at the final depth level
                df = df.withColumn(col_name, F.to_json(F.col(col_name)))
            else:
                # Handle arrays and structs differently
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

    # After flattening, convert any remaining complex fields to strings if the depth is reached
    if current_level >= depth_level:
        df = df.select(
            [
                F.col(c).cast(StringType())
                if isinstance(df.schema[c].dataType, (ArrayType, StructType))
                else F.col(c)
                for c in df.columns
            ]
        )

    # Rename columns by replacing '__' and '.' with '_'
    df = rename_columns(df, replacements={"__": "_", ".": "_"})

    return df


def _string_replace(s: str, replacements: dict) -> str:
    """
    Helper function to perform multiple string replacements.

    Args:
        s (str): The input string to be modified.
        replacements (dict): Dictionary of replacements where the key is the substring to replace and the value is the replacement.

    Returns:
        str: The modified string with all replacements applied.
    """
    for string_before, string_after in replacements.items():
        s = s.replace(string_before, string_after)

    return s


def rename_columns(df: DataFrame, replacements: dict) -> DataFrame:
    """
    Renames DataFrame columns based on a dictionary of replacement rules.

    Args:
        df (DataFrame): The DataFrame whose columns need renaming.
        replacements (dict): A dictionary with keys as substrings to be replaced and values as their replacements.

    Returns:
        DataFrame: The DataFrame with renamed columns.
    """
    for column_name in df.columns:
        df = df.withColumnRenamed(
            column_name, _string_replace(column_name, replacements)
        )
    return df


def rename_and_cast_columns(
    df: DataFrame, column_mapping: dict = None, cast_type_mapping: dict = None
) -> DataFrame:
    """
    Renames specified columns and optionally casts them to a different data type.

    Args:
        df (DataFrame): The DataFrame whose columns need to be renamed and cast.
        column_mapping (dict, optional): Dictionary specifying columns to rename.
                                         Example: {"Timestamp": "EventTimestamp"}.
        cast_type_mapping (dict, optional): Dictionary specifying columns to cast to specific data types.
                                             Example: {"EventTimestamp": "timestamp"}.

    Returns:
        DataFrame: A DataFrame with renamed and potentially casted columns.
    """
    # Rename columns based on the provided mapping
    if column_mapping:
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

    # Cast columns based on the provided type mapping
    if cast_type_mapping:
        for col_name, new_type in cast_type_mapping.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(new_type))

    return df


def add_columns_that_are_not_in_df(df, column_names: List[str]):
    """Add columns in column_names that are not already in dataframe.

    The new columns are empty. This function can be used before a CAST statement to ensure that all
    expected columns are included in dataframe.
    """
    for column_name in column_names:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None))
            print(f'Column "{column_name}" was added.')

    return df


def read_json_from_binary(spark, schema, data_file_path):
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


def process_and_flatten_json(spark, config, schema_file_path, data_file_path, helper=None, depth_level=None, type_mapping=None) -> tuple:
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
    max_depth = reader.get_json_depth(schema_json, helper=helper, depth_level=depth_level)

    # Flatten the DataFrame based on the depth level
    df_flattened = flatten_df(df, depth_level=depth_level, max_depth=max_depth, type_mapping=type_mapping)

    # Drop the "input_file_name" column from the original DataFrame
    df = df.drop("input_file_name")

    # Return both the schema DataFrame and the flattened DataFrame
    return df, df_flattened

def create_temp_view_with_most_recent_records(
    spark,  # Pass the Spark session as the first parameter
    view_name: str,
    key_columns: str,  # Explicitly pass key_columns as a comma-separated string
    columns_of_interest: str,  # Explicitly pass columns_of_interest as a comma-separated string
    order_by_columns: list = ["input_file_name DESC"],  # Default order by input_file_name in descending order
    helper=None  # Optional helper for logging
) -> None:
    """
    Creates a temporary view with the most recent version of records based on key columns and ordering logic.

    Args:
        spark (SparkSession): The active Spark session.
        view_name (str): The name of the temporary view containing the data.
        key_columns (str): A comma-separated string of key columns.
        columns_of_interest (str): A comma-separated string of columns to be included in the final view.
        order_by_columns (list, optional): List of column names used in the ORDER BY clause.
                                           Defaults to ["input_file_name DESC"].
        helper (optional): A logging helper object for writing messages. Defaults to None.

    Raises:
        ValueError: If key_columns is empty or there is an issue with it.
        Exception: If there is an error executing the SQL query.
    """
    try:
        # Ensure key columns are provided
        if not key_columns:
            raise ValueError("ERROR: No KeyColumns provided!")

        # Create a list of key columns and trim any extra whitespace
        key_columns_list = [col.strip() for col in key_columns.split(',')]

        # Construct the SQL query to create the temporary view
        new_data_sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW temp_{view_name} AS
        SELECT {columns_of_interest}
        FROM (
            SELECT t.*, 
                   row_number() OVER (PARTITION BY {', '.join(key_columns_list)} 
                                      ORDER BY {', '.join(order_by_columns)}) AS rnr
            FROM {view_name} t
        ) x
        WHERE rnr = 1;
        """

        # Log the constructed SQL query for debugging, if helper is available
        if helper:
            helper.write_message(f"Constructed SQL query: {new_data_sql}")

        # Execute the SQL query to create the temporary view
        spark.sql(new_data_sql)
        if helper:
            helper.write_message(f"Temporary view temp_{view_name} created successfully.")

        # Optionally: Display the DataFrame to verify the result (useful in Databricks or Jupyter)
        # display(spark.sql(f"SELECT * FROM temp_{view_name}"))

    except ValueError as ve:
        if helper:
            helper.write_message(f"Configuration Error: {ve}")
        raise

    except Exception as e:
        if helper:
            helper.write_message(f"Error creating temporary view temp_{view_name}: {e}")
        raise