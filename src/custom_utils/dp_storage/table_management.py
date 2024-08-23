from custom_utils.dp_storage import writer  # Import writer module

def get_destination_details(spark, destination_environment, source_datasetidentifier, helper=None):
    """
    Retrieves the destination path, database name, and table name for the given environment and dataset identifier.

    Args:
        spark (SparkSession): The active Spark session.
        destination_environment (str): The destination environment.
        source_datasetidentifier (str): The dataset identifier (usually a folder or dataset name).
        helper (optional): An optional logging helper object for writing messages.

    Returns:
        tuple: A tuple containing the destination path, database name, and table name.
    """
    # Fetching destination details using the provided parameters
    destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
    database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
    
    if helper:
        helper.write_message(f"Destination path: {destination_path}")
        helper.write_message(f"Database: {database_name}")
        helper.write_message(f"Table: {table_name}")
    
    return destination_path, database_name, table_name

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
        spark.sql(f"DESCRIBE TABLE {database_name}.{table_name}")
        table_exists = True
        if helper:
            helper.write_message(f"Table {database_name}.{table_name} already exists.")
    except Exception as e:
        if "Table or view not found" in str(e):
            if helper:
                helper.write_message(f"Table {database_name}.{table_name} does not exist. Creating a new table.")
        else:
            if helper:
                helper.write_message(f"Error checking table existence: {str(e)}")
            raise
    
    return table_exists

def create_table_if_not_exists(spark, database_name, table_name, destination_path, table_exists, helper=None):
    """
    Creates a Databricks Delta table at the specified path if it does not already exist.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the table.
        destination_path (str): The path where the table data will be stored.
        table_exists (bool): Boolean flag indicating if the table already exists.
        helper (optional): A logging helper object for writing messages.
    """
    create_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    USING DELTA
    LOCATION 'dbfs:{destination_path}/'
    """

    # Always print the SQL query regardless of whether the table already exists
    if helper:
        helper.write_message(f"Executing SQL query: {create_tbl_sql.strip()}")

    # Only execute the SQL and print a success message if the table does not exist
    if not table_exists:
        try:
            spark.sql(create_tbl_sql)
            if helper:
                helper.write_message(f"Table {database_name}.{table_name} created successfully.")
        except Exception as e:
            if helper:
                helper.write_message(f"Error creating table {database_name}.{table_name}: {str(e)}")
            raise

def manage_table_creation(spark, destination_environment, source_datasetidentifier, helper=None):
    """
    Orchestrates the process of managing table creation in Databricks based on the dataset and environment.

    Args:
        spark (SparkSession): The active Spark session.
        destination_environment (str): The destination environment.
        source_datasetidentifier (str): The dataset identifier (usually a folder or dataset name).
        helper (optional): A logging helper object for writing messages.
    """
    # Fetch the destination details
    destination_path, database_name, table_name = get_destination_details(spark, destination_environment, source_datasetidentifier, helper)

    # Check if the table exists
    table_exists = check_if_table_exists(spark, database_name, table_name, helper)

    # Create the table if it does not exist
    create_table_if_not_exists(spark, database_name, table_name, destination_path, table_exists, helper)