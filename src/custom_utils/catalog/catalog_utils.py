# File: custom_utils/catalog/catalog_utils.py

from custom_utils.dp_storage import writer
from pyspark.sql import SparkSession
from typing import List, Dict

# ========================
# UTILITY FUNCTIONS
# ========================

def pretty_print_message(message: str, helper=None, level: str = "info"):
    """
    Helper function to print or log messages in a consistent format.
    """
    if helper:
        helper.write_message(message)
    else:
        print(f"[{level.upper()}] {message}")

def get_destination_details(spark: SparkSession, destination_environment: str, source_datasetidentifier: str, helper=None):
    """
    Retrieves the destination path, database name, and table name.
    """
    try:
        destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
        database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
        pretty_print_message(f"Destination Details:\nPath: {destination_path}\nDatabase: {database_name}\nTable: {table_name}", helper)
        return destination_path, database_name, table_name
    except Exception as e:
        pretty_print_message(f"Error retrieving destination details: {e}", helper, "error")
        raise

def ensure_path_exists(dbutils, destination_path, helper=None):
    """
    Ensures the destination path exists in DBFS. If not, creates the path.
    """
    try:
        dbutils.fs.ls(destination_path)
        pretty_print_message(f"Path already exists: {destination_path}", helper)
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            dbutils.fs.mkdirs(destination_path)
            pretty_print_message(f"Path did not exist. Created path: {destination_path}", helper)
        else:
            pretty_print_message(f"Error while ensuring path exists: {e}", helper, "error")
            raise

# ========================
# DUPLICATE HANDLING AND VALIDATION
# ========================

def delete_or_filter_duplicates(spark: SparkSession, temp_view_name: str, key_columns: List[str], helper=None):
    """
    Deletes or filters out duplicate records from the source data in the temporary view based on key columns.
    """
    key_columns_str = ', '.join(key_columns)
    filtered_view_name = f"{temp_view_name}_deduped"
    dedupe_query = f"""
    WITH ranked_data AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_columns_str} ORDER BY {key_columns_str}) AS row_num
        FROM {temp_view_name}
    )
    SELECT * FROM ranked_data WHERE row_num = 1
    """

    try:
        deduped_df = spark.sql(dedupe_query)
        deduped_df.createOrReplaceTempView(filtered_view_name)
        pretty_print_message(f"Created new view without duplicates: {filtered_view_name}", helper)
        return filtered_view_name
    except Exception as e:
        pretty_print_message(f"Error during duplicate filtering: {e}", helper, "error")
        raise

def validate_and_create_duplicate_view(spark: SparkSession, temp_view_name: str, key_columns: List[str], remove_duplicates: bool = False, helper=None):
    """
    Validates the data in the temporary view for duplicates and creates a view with duplicates if found.
    """
    key_columns_str = ', '.join(key_columns)
    duplicate_keys_query = f"""
    SELECT {key_columns_str}, COUNT(*) AS duplicate_count
    FROM {temp_view_name}
    GROUP BY {key_columns_str}
    HAVING COUNT(*) > 1
    """
    duplicates_df = spark.sql(duplicate_keys_query)

    if duplicates_df.count() > 0:
        duplicates_view_name = f"view_duplicates_{temp_view_name}"
        duplicates_df.createOrReplaceTempView(duplicates_view_name)
        pretty_print_message(f"Duplicate records found. View created: {duplicates_view_name}", helper)

        if remove_duplicates:
            pretty_print_message(f"Duplicates were found and removed. Continuing with the operation.", helper)
            return delete_or_filter_duplicates(spark, temp_view_name, key_columns, helper)
        else:
            pretty_print_message(f"Duplicate keys found in {temp_view_name}. Set 'remove_duplicates=True' to automatically remove duplicates.", helper, "error")
            raise ValueError(f"Duplicate keys found in {temp_view_name}. Operation aborted.")
    else:
        pretty_print_message(f"No duplicates found in {temp_view_name}.", helper)
        return temp_view_name

# ========================
# SQL GENERATION AND EXECUTION
# ========================

def generate_merge_sql(temp_view_name: str, database_name: str, table_name: str, key_columns: List[str], helper=None):
    """
    Constructs the SQL query for the MERGE operation.
    """
    all_columns = [col for col in spark.table(temp_view_name).columns if col not in key_columns and col != 'CreatedDate']
    match_sql = ' AND '.join([f"s.{col.strip()} = t.{col.strip()}" for col in key_columns])
    update_sql = ', '.join([f"t.{col.strip()} = s.{col.strip()}" for col in all_columns])
    insert_columns = ', '.join([f"{col.strip()}" for col in key_columns + all_columns + ['CreatedDate']])
    insert_values = ', '.join([f"s.{col.strip()}" for col in key_columns + all_columns + ['CreatedDate']])

    merge_sql = f"""
    MERGE INTO {database_name}.{table_name} AS t
    USING {temp_view_name} AS s
    ON {match_sql}
    WHEN MATCHED THEN
      UPDATE SET {update_sql}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns}) VALUES ({insert_values})
    """

    pretty_print_message(f"MERGE SQL:\n{merge_sql.strip()}", helper)
    return merge_sql

def create_or_replace_table(spark: SparkSession, database_name: str, table_name: str, destination_path: str, temp_view_name: str, helper=None):
    """
    Creates a Databricks Delta table if it does not exist and writes data to it.
    """
    try:
        df = spark.table(temp_view_name)
        schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            {schema_str}
        )
        USING DELTA
        LOCATION 'dbfs:{destination_path}/'
        """
        pretty_print_message(f"Creating table with query:\n{create_table_sql.strip()}", helper)

        if not check_if_table_exists(spark, database_name, table_name, helper):
            spark.sql(create_table_sql)
            df.write.format("delta").mode("overwrite").save(destination_path)
            pretty_print_message(f"Table {database_name}.{table_name} created and data written to {destination_path}.", helper)
        else:
            pretty_print_message(f"Table {database_name}.{table_name} already exists.", helper)
    except Exception as e:
        pretty_print_message(f"Error creating or replacing table: {e}", helper, "error")
        raise

# ========================
# MAIN OPERATION MANAGEMENT
# ========================

def manage_data_operation(spark: SparkSession, dbutils, destination_environment: str, source_datasetidentifier: str, temp_view_name: str, key_columns: List[str], operation_type: str = "merge", remove_duplicates: bool = False, helper=None):
    """
    Orchestrates the data operation (create or merge) process by validating paths and managing the Delta table.
    """
    try:
        # Retrieve destination details
        destination_path, database_name, table_name = get_destination_details(spark, destination_environment, source_datasetidentifier, helper)
        ensure_path_exists(dbutils, destination_path, helper)

        # Validate for duplicates (and remove if specified)
        temp_view_name = validate_and_create_duplicate_view(spark, temp_view_name, key_columns, remove_duplicates, helper)

        if operation_type == "create":
            # Create table if it doesn't exist
            create_or_replace_table(spark, database_name, table_name, destination_path, temp_view_name, helper)
        elif operation_type == "merge":
            # Merge data into the target table
            merge_sql = generate_merge_sql(temp_view_name, database_name, table_name, key_columns, helper)
            spark.sql(merge_sql)
            pretty_print_message(f"Data merged into {database_name}.{table_name}.", helper)
        else:
            raise ValueError(f"Invalid operation type: {operation_type}. Supported types are 'create' and 'merge'.")

    except Exception as e:
        pretty_print_message(f"Error managing data operation: {e}", helper, "error")
        raise