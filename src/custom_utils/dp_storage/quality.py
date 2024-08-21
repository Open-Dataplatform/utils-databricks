from pyspark.sql.utils import AnalysisException

def build_duplicate_check_query(view_name: str, key_columns_list: list) -> str:
    """
    Constructs an SQL query to check for duplicates in the dataset.

    Args:
        view_name (str): The name of the temporary view containing the data.
        key_columns_list (list): A list of key columns to check for duplicates.

    Returns:
        str: The constructed SQL query for duplicate checking.
    """
    partition_by_columns = ', '.join(['input_file_name'] + key_columns_list)
    key_columns_str = ', '.join(key_columns_list)

    query = f"""
    SELECT 
        'ERROR: duplicates in new data for {key_columns_str}' AS error_message, 
        COUNT(*) AS duplicate_count, 
        {', '.join(['input_file_name'] + key_columns_list)}
    FROM {view_name}
    GROUP BY {partition_by_columns}
    HAVING COUNT(*) > 1;
    """
    return query

def check_for_duplicates(query: str, spark, helper=None) -> None:
    """
    Executes the SQL query to check for duplicates and handles the result.

    Args:
        query (str): The SQL query to check for duplicates.
        spark (SparkSession): The active Spark session.
        helper (optional): A logging helper object.

    Raises:
        ValueError: If duplicates are found in the new data.
    """
    if helper:
        helper.write_message(f"Executing duplicate check query: {query}")

    try:
        duplicates_df = spark.sql(query)
        duplicate_count = duplicates_df.count()

        if duplicate_count > 0:
            if helper:
                helper.write_message(f"ERROR: Found {duplicate_count} duplicate records!")
            duplicates_df.show(truncate=False)
            raise ValueError(f"Data Quality Check Failed: Found {duplicate_count} duplicates in the new data.")
        else:
            if helper:
                helper.write_message("Data Quality Check Passed: No duplicates found in the new data.")
    except AnalysisException as e:
        if helper:
            helper.write_message(f"Error executing duplicate check query: {e}")
        raise

def perform_quality_check(spark, helper=None) -> None:
    """
    Performs the quality check for duplicates in the new data.

    Args:
        spark (SparkSession): The active Spark session.
        helper (optional): A logging helper object.
    """
    # Retrieve the key_columns from the global context
    key_columns = globals().get('key_columns')
    if not key_columns:
        raise ValueError("ERROR: No KeyColumns defined in the global context!")

    # Get the list of key columns
    key_columns_list = helper.get_key_columns_list(key_columns)

    # Retrieve the view name from the global context
    view_name = globals().get('source_datasetidentifier')
    if not view_name:
        raise ValueError("ERROR: No view name (source_datasetidentifier) defined in the global context!")

    # Build the SQL query for duplicate checking
    query = build_duplicate_check_query(view_name, key_columns_list)

    # Execute the query and handle the results
    check_for_duplicates(query, spark, helper)