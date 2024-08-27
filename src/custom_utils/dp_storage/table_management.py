from custom_utils.dp_storage import writer  # Import writer module

def get_destination_details(spark, destination_environment, source_datasetidentifier, helper=None):
    """
    Retrieves the destination path, database name, and table name for the given environment and dataset identifier.
    """
    destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
    database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
    
    if helper:
        helper.write_message(f"Destination Path: {destination_path}\nDatabase: {database_name}\nTable: {table_name}")
    
    return destination_path, database_name, table_name

def validate_and_prepare_destination(dbutils, destination_path, temp_view_name, spark, helper=None):
    """
    Validates that the destination path exists and creates it if it does not exist. 
    If the path does not exist, it recreates the Delta Parquet files from scratch.
    """
    path_existed = True
    try:
        # Check if the destination path exists
        try:
            dbutils.fs.ls(destination_path)
            if helper:
                helper.write_message(f"Path already exists: {destination_path}")
        except Exception as e:
            # Handle case where the path does not exist
            if "java.io.FileNotFoundException" in str(e):
                path_existed = False
                if helper:
                    helper.write_message(f"Path does not exist. Creating path: {destination_path}")
                dbutils.fs.mkdirs(destination_path)
            else:
                raise e

        if not path_existed:
            # Recreate Delta Parquet files from the beginning if the path did not exist
            if helper:
                helper.write_message(f"Recreating Delta Parquet files at: {destination_path} as path was missing.")
            recreate_delta_parquet(spark, destination_path, temp_view_name, helper)
    except Exception as e:
        if helper:
            helper.write_message(f"Error checking or creating path: {str(e)}")
        raise

    return path_existed

def recreate_delta_parquet(spark, destination_path, temp_view_name, helper=None):
    """
    Recreates the Delta Parquet files from the data in temp_view_name.
    """
    try:
        df = spark.table(temp_view_name)
        if helper:
            helper.write_message(f"Displaying DataFrame used to create Delta Parquet at {destination_path}:")
            df.show()

        # Write the data to the Delta Parquet files
        df.write.format("delta").mode("overwrite").save(destination_path)

        if helper:
            helper.write_message(f"Delta Parquet files successfully written to: {destination_path}")

    except Exception as e:
        if helper:
            helper.write_message(f"Error recreating Delta Parquet files: {str(e)}")
        raise

def check_if_table_exists(spark, database_name, table_name, helper=None):
    """
    Checks if a Databricks table exists in the specified database.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the table.
        helper (optional): A logging helper object for writing messages.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    table_exists = False
    try:
        # Use SHOW TABLES to ensure table existence
        table_check = spark.sql(f"SHOW TABLES IN {database_name}").collect()

        # Ensure you correctly access the tableName field
        table_exists = any(row["tableName"] == table_name for row in table_check)

        if helper and not table_exists:
            helper.write_message(f"Table {database_name}.{table_name} does not exist. It will be created.")
    except Exception as e:
        if helper:
            helper.write_message(f"Error checking table existence: {str(e)}")
        raise
    
    return table_exists

def create_table_if_not_exists(spark, database_name, table_name, destination_path, temp_view_name, helper=None):
    """
    Creates a Databricks Delta table at the specified path if it does not already exist, and writes data from temp_view_name.
    """
    try:
        df = spark.table(temp_view_name)
        schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])

        # Generate the SQL query to create the Delta table
        create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
    {schema_str}
)
USING DELTA
LOCATION 'dbfs:{destination_path}/'
"""

        # Print the SQL query regardless of whether the table exists
        if helper:
            helper.write_message(f"SQL Query to Create the Table:\n{'-' * 30}\n{create_table_sql.strip()}\n{'-' * 30}")

        # Check if the table exists before creating it
        table_exists = check_if_table_exists(spark, database_name, table_name, helper)

        if not table_exists:
            # Execute the SQL query to create the table
            spark.sql(create_table_sql)
            if helper:
                helper.write_message(f"Table {database_name}.{table_name} created successfully.")

            # Write data from the temp view to the Delta table location
            df.write.format("delta").mode("overwrite").save(destination_path)

            if helper:
                helper.write_message(f"Data successfully written to {destination_path}")
        else:
            if helper:
                helper.write_message(f"Table {database_name}.{table_name} already exists. Skipping creation.")

    except Exception as e:
        if helper:
            helper.write_message(f"Error during table creation: {str(e)}")
        raise

def manage_table_creation(spark, dbutils, destination_environment, source_datasetidentifier, temp_view_name, helper=None):
    """
    Orchestrates the process of managing table creation in Databricks based on the dataset and environment.
    """
    destination_path, database_name, table_name = get_destination_details(spark, destination_environment, source_datasetidentifier, helper)

    path_existed = validate_and_prepare_destination(dbutils, destination_path, temp_view_name, spark, helper)

    create_table_if_not_exists(spark, database_name, table_name, destination_path, temp_view_name, helper)