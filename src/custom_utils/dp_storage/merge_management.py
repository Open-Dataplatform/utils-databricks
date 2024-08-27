from custom_utils.dp_storage import writer  # Import writer module

def get_merge_destination_details(spark, destination_environment, source_datasetidentifier, helper=None):
    """
    Retrieves the destination path, database name, and table name for merging data.

    Args:
        spark (SparkSession): The active Spark session.
        destination_environment (str): The destination environment.
        source_datasetidentifier (str): The dataset identifier (usually a folder or dataset name).
        helper (optional): An optional logging helper object for writing messages.

    Returns:
        tuple: A tuple containing the destination path, database name, and table name.
    """
    # Fetch the destination path and table information based on the environment and dataset identifier
    destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
    database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
    
    if helper:
        helper.write_message(f"Destination path: {destination_path}")
        helper.write_message(f"Database name: {database_name}")
        helper.write_message(f"Table name: {table_name}")
    
    return destination_path, database_name, table_name

def get_pre_merge_version(spark, database_name, table_name, helper=None):
    """
    Retrieves the current version of the Delta table before the merge.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the table.
        helper (optional): A logging helper object for writing messages.

    Returns:
        int: The pre-merge version number of the Delta table.
    """
    try:
        # Query the version history of the Delta table to get the latest version before the merge
        pre_merge_version = spark.sql(f"DESCRIBE HISTORY {database_name}.{table_name} LIMIT 1").select("version").collect()[0][0]
        return pre_merge_version
    except Exception as e:
        if helper:
            helper.write_message(f"Error retrieving pre-merge version: {e}")
        raise

def generate_merge_sql(view_name, database_name, table_name, key_columns, helper=None):
    """
    Constructs the SQL query for the MERGE operation.

    Args:
        view_name (str): The name of the view containing new data.
        database_name (str): The name of the database.
        table_name (str): The name of the target table for the merge.
        key_columns (str): Comma-separated list of key columns.
        helper (optional): An optional logging helper object for writing messages.

    Returns:
        str: The constructed SQL query for the MERGE operation.
    """
    # Construct the match condition based on the provided key columns
    match_sql = ' AND '.join([f"s.{col.strip()} = t.{col.strip()}" for col in key_columns.split(',')])

    # Construct the MERGE SQL statement
    merge_sql = f"""
    MERGE INTO {database_name}.{table_name} AS t
    USING {view_name} AS s
    ON {match_sql}
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """

    if helper:
        helper.write_message(f"Executing SQL query:\n{'-' * 30}\n{merge_sql.strip()}\n{'-' * 30}")
    
    return merge_sql

def execute_merge_and_get_post_version(spark, database_name, table_name, merge_sql, pre_merge_version, helper=None):
    """
    Executes the MERGE operation and calculates the post-merge version.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the target table for the merge.
        merge_sql (str): The SQL query for the MERGE operation.
        pre_merge_version (int): The version of the table before the merge.
        helper (optional): An optional logging helper object for writing messages.

    Returns:
        int: The post-merge version number of the Delta table.
    """
    try:
        # Execute the MERGE operation using the constructed SQL
        spark.sql(merge_sql)
        if helper:
            helper.write_message("Data merged successfully.")
    except Exception as e:
        if helper:
            helper.write_message(f"Error during data merge: {e}")
        raise

    # Calculate and return the new version after the merge
    try:
        post_merge_version = pre_merge_version + 1
        return post_merge_version
    except Exception as e:
        if helper:
            helper.write_message(f"Error calculating post-merge version: {e}")
        raise

def display_newly_merged_data(spark, database_name, table_name, pre_merge_version, post_merge_version, helper=None):
    """
    Displays the data that was newly merged by querying the differences between versions.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the target table.
        pre_merge_version (int): The version of the table before the merge.
        post_merge_version (int): The version of the table after the merge.
        helper (optional): A logging helper object for writing messages.
    """
    # Construct the query to fetch newly merged data by comparing pre and post versions
    merged_data_sql = f"""
    SELECT * FROM {database_name}.{table_name} VERSION AS OF {post_merge_version}
    EXCEPT
    SELECT * FROM {database_name}.{table_name} VERSION AS OF {pre_merge_version}
    """

    if helper:
        helper.write_message(f"Displaying newly merged data with query:\n{'-' * 30}\n{merged_data_sql.strip()}\n{'-' * 30}")

    try:
        merged_data_df = spark.sql(merged_data_sql)
        
        # Display the results if there are any rows returned
        if merged_data_df.count() == 0:
            if helper:
                helper.write_message("Query returned no results.")
        else:
            helper.write_message(f"Displaying up to 100 rows of the newly merged data:")
            display(merged_data_df.limit(100))  # Limit the output to 100 rows for clarity
    except Exception as e:
        if helper:
            helper.write_message(f"Error displaying merged data: {e}")
        raise

def manage_data_merge(spark, destination_environment, source_datasetidentifier, view_name, key_columns, helper=None):
    """
    Manages the entire data merge process, from generating the SQL to displaying the merged data.

    Args:
        spark (SparkSession): The active Spark session.
        destination_environment (str): The destination environment.
        source_datasetidentifier (str): The dataset identifier (usually a folder or dataset name).
        view_name (str): The name of the view containing new data.
        key_columns (str): A comma-separated string of key columns.
        helper (optional): A logging helper object for writing messages.
    """
    # Get the destination details including path, database, and table names
    destination_path, database_name, table_name = get_merge_destination_details(spark, destination_environment, source_datasetidentifier, helper)

    # Retrieve the current version of the Delta table before the merge
    pre_merge_version = get_pre_merge_version(spark, database_name, table_name, helper)

    # Generate the SQL for the MERGE operation
    merge_sql = generate_merge_sql(view_name, database_name, table_name, key_columns, helper)

    # Execute the merge and get the new version after the merge
    post_merge_version = execute_merge_and_get_post_version(spark, database_name, table_name, merge_sql, pre_merge_version, helper)

    # Display the newly merged data for review
    display_newly_merged_data(spark, database_name, table_name, pre_merge_version, post_merge_version, helper)