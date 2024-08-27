from custom_utils.dp_storage import writer  # Import writer module

def get_merge_destination_details(spark, destination_environment, source_datasetidentifier, helper=None):
    """
    Retrieves the destination path, database name, and table name for merging data.
    """
    destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
    database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
    
    # Log the destination details if helper is provided
    if helper:
        helper.write_message(f"Destination path: {destination_path}")
        helper.write_message(f"Database name: {database_name}")
        helper.write_message(f"Table name: {table_name}")
    
    return destination_path, database_name, table_name

def get_pre_merge_version(spark, database_name, table_name, helper=None):
    """
    Retrieves the current version of the Delta table before the merge.
    """
    try:
        # Fetch the latest version of the Delta table from history
        pre_merge_version = spark.sql(f"DESCRIBE HISTORY {database_name}.{table_name} LIMIT 1") \
                                .select("version").collect()[0][0]
        return pre_merge_version
    except Exception as e:
        if helper:
            helper.write_message(f"Error retrieving pre-merge version: {e}")
        raise

def generate_merge_sql(temp_view_name, database_name, table_name, key_columns, helper=None):
    """
    Constructs the SQL query for the MERGE operation.
    """
    # Construct the match condition for the MERGE operation based on key columns
    match_sql = ' AND '.join([f"s.{col.strip()} = t.{col.strip()}" for col in key_columns.split(',')])

    # Build the MERGE SQL query
    merge_sql = f"""
    MERGE INTO {database_name}.{table_name} AS t
    USING {temp_view_name} AS s
    ON {match_sql}
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """

    # Log the SQL query for the MERGE operation
    if helper:
        helper.write_message(f"Executing SQL query:\n{'-' * 30}\n{merge_sql.strip()}\n{'-' * 30}")
    
    return merge_sql

def find_duplicate_keys_in_source(spark, temp_view_name, key_column, helper=None):
    """
    Identifies rows in the source data that have duplicate keys.
    """
    duplicate_keys_query = f"""
    SELECT {key_column}, COUNT(*) AS duplicate_count
    FROM {temp_view_name}
    GROUP BY {key_column}
    HAVING COUNT(*) > 1
    """

    # Execute the duplicate check query
    duplicates_df = spark.sql(duplicate_keys_query)

    if duplicates_df.count() == 0:
        if helper:
            helper.write_message("No duplicates found in the source data.")
    else:
        # Log and display the duplicate query only if duplicates are found
        if helper:
            helper.write_message(f"Executing query to find duplicate keys:\n{'-' * 30}\n{duplicate_keys_query.strip()}\n{'-' * 30}")
    
    return duplicates_df

def display_problematic_rows_and_raise_error(spark, temp_view_name, key_column, duplicates_df, helper=None):
    """
    Displays the rows from the source data that have duplicate keys and raises an error if duplicates are found.
    """
    if duplicates_df.count() > 0:
        # Collect the problematic key values
        duplicate_keys = [row[key_column] for row in duplicates_df.collect()]

        # Query to fetch the problematic rows based on duplicate keys
        problematic_rows_query = f"""
        SELECT *
        FROM {temp_view_name}
        WHERE {key_column} IN ({','.join([f"'{key}'" for key in duplicate_keys])})
        """

        if helper:
            helper.write_message(f"Displaying rows with duplicate keys:\n{'-' * 30}\n{problematic_rows_query.strip()}\n{'-' * 30}")

        # Display the problematic rows (limited to 100 for readability)
        problematic_rows_df = spark.sql(problematic_rows_query)
        display(problematic_rows_df.limit(100))

        # Raise an error to stop the process if duplicates are found
        raise ValueError("Duplicate keys found in the source data. Merge operation aborted.")

def execute_merge_and_get_post_version(spark, database_name, table_name, merge_sql, pre_merge_version, helper=None):
    """
    Executes the MERGE operation and calculates the post-merge version.
    """
    try:
        # Execute the MERGE SQL operation
        spark.sql(merge_sql)
        if helper:
            helper.write_message("Data merged successfully.")
    except Exception as e:
        if helper:
            helper.write_message(f"Error during data merge: {e}")
        raise

    # Calculate the new version after the merge
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
    """
    # Query to identify newly merged data by comparing versions
    merged_data_sql = f"""
    SELECT * FROM {database_name}.{table_name} VERSION AS OF {post_merge_version}
    EXCEPT
    SELECT * FROM {database_name}.{table_name} VERSION AS OF {pre_merge_version}
    """

    if helper:
        helper.write_message(f"Displaying newly merged data with query:\n{'-' * 30}\n{merged_data_sql.strip()}\n{'-' * 30}")

    try:
        merged_data_df = spark.sql(merged_data_sql)
        
        # Display the results if there are any newly merged rows
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

def manage_data_merge(spark, destination_environment, source_datasetidentifier, temp_view_name, key_columns, helper=None):
    """
    Manages the entire data merge process, from generating the SQL to displaying the merged data.
    """
    # Get the destination details including path, database, and table names
    destination_path, database_name, table_name = get_merge_destination_details(spark, destination_environment, source_datasetidentifier, helper)

    # Check for duplicate keys before performing the merge
    duplicates_df = find_duplicate_keys_in_source(spark, temp_view_name, key_columns, helper)
    display_problematic_rows_and_raise_error(spark, temp_view_name, key_columns, duplicates_df, helper)

    # Retrieve the current version of the Delta table before the merge
    pre_merge_version = get_pre_merge_version(spark, database_name, table_name, helper)

    # Generate the SQL for the MERGE operation
    merge_sql = generate_merge_sql(temp_view_name, database_name, table_name, key_columns, helper)

    # Execute the merge and get the new version after the merge
    post_merge_version = execute_merge_and_get_post_version(spark, database_name, table_name, merge_sql, pre_merge_version, helper)

    # Display the newly merged data for review
    display_newly_merged_data(spark, database_name, table_name, pre_merge_version, post_merge_version, helper)