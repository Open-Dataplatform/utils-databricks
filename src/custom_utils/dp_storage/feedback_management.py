from pyspark.sql import DataFrame

def construct_feedback_sql(view_name: str, feedback_column: str, helper=None) -> str:
    """
    Constructs an SQL query to get the minimum and maximum timestamps for the feedback column.

    Args:
        view_name (str): The name of the view containing the new data.
        feedback_column (str): The name of the feedback timestamp column.
        helper (optional): An optional logging helper object for writing messages.

    Returns:
        str: The constructed SQL query to fetch the minimum and maximum feedback timestamps.
    """
    feedback_sql = f"""
    SELECT
        MIN({feedback_column}) AS from_datetime,
        MAX({feedback_column}) AS to_datetime
    FROM {view_name}
    """

    if helper:
        helper.write_message(f"Executing SQL query: {feedback_sql}")

    return feedback_sql

def execute_feedback_sql(spark, feedback_sql: str, helper=None) -> DataFrame:
    """
    Executes the SQL query to get the minimum and maximum timestamps.

    Args:
        spark (SparkSession): The active Spark session.
        feedback_sql (str): The SQL query to execute.
        helper (optional): An optional logging helper object for writing messages.

    Returns:
        DataFrame: A DataFrame containing the result of the SQL query.

    Raises:
        Exception: If the SQL query fails.
    """
    try:
        return spark.sql(feedback_sql)
    except Exception as e:
        if helper:
            helper.write_message(f"Error executing SQL query: {e}")
        raise

def handle_feedback_result(df_min_max: DataFrame, view_name: str, helper=None):
    """
    Handles the result of the feedback SQL query, converts it to JSON, and exits the notebook.

    Args:
        df_min_max (DataFrame): The DataFrame containing the minimum and maximum timestamps.
        view_name (str): The name of the view for logging purposes.
        helper (optional): An optional logging helper object for writing messages.

    Raises:
        ValueError: If no data is found in the DataFrame.
    """
    if df_min_max.head(1):  # Efficient way to check if the DataFrame is empty
        # Convert the result to JSON and get the first record
        notebook_output = df_min_max.toJSON().first()
        if helper:
            helper.write_message(f"Notebook output: {notebook_output}")

        # Exit the notebook with the JSON output
        dbutils.notebook.exit(notebook_output)
    else:
        error_message = f"No data found in {view_name} to calculate the feedback timestamps."
        if helper:
            helper.write_message(error_message)
        raise ValueError(error_message)

def generate_feedback_timestamps(spark, view_name, feedback_column, helper=None):
    """
    Orchestrates the entire process of calculating and returning feedback timestamps.

    Args:
        spark (SparkSession): The active Spark session.
        view_name (str): The name of the view containing the new data.
        feedback_column (str): The name of the feedback timestamp column.
        helper (optional): An optional logging helper object for writing messages.
    """
    # Generate the feedback SQL query
    feedback_sql = construct_feedback_sql(view_name, feedback_column, helper)

    # Execute the feedback SQL query
    df_min_max = execute_feedback_sql(spark, feedback_sql, helper)

    # Handle the result and exit the notebook with the feedback output
    handle_feedback_result(df_min_max, view_name, helper)