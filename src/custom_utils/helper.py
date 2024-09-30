# File: custom_utils/helper.py

"""Helper common functions for logging, parameter retrieval, and notebook control."""

import os
from custom_utils import adf
from custom_utils.logging.logger import Logger  # Import Logger

# Initialize the Logger
logger = Logger()

def write_message(message):
    """Log or print a message."""
    print(message)

def exit_notebook(message, dbutils=None):
    """
    Exit the notebook with an error message. If `dbutils` is not available, raises a system exit.
    Args:
        message (str): The error message to display.
        dbutils: The Databricks dbutils object, used to exit the notebook.
    """
    logger.log_message(message, level="error")  # Use logger for error messages
    if dbutils:
        dbutils.notebook.exit(f"[ERROR] {message}")
    else:
        raise SystemExit(f"[ERROR] {message}")

def get_adf_parameter(dbutils, param_name, default_value=""):
    """
    Get a parameter from Azure Data Factory (ADF).
    Args:
        dbutils: Databricks utilities.
        param_name (str): Name of the parameter.
        default_value (str): Default value if parameter is not found.
    Returns:
        str: Parameter value.
    """
    try:
        return adf.get_parameter(dbutils, param_name)
    except Exception as e:
        logger.log_message(f"Could not get parameter '{param_name}': {e}", level="warning")  # Use logger for warnings
        return default_value

def get_param_value(dbutils, param_name, default_value=None, required=False):
    """
    Fetches a parameter value from Databricks widgets, environment variables, or defaults.
    """
    value = None
    try:
        if dbutils:
            value = dbutils.widgets.get(param_name)
    except Exception as e:
        logger.log_message(f"Could not retrieve widget '{param_name}': {e}", level="warning")

    if not value:  # If widget value is not set, fallback to default or environment variables
        value = os.getenv(param_name.upper(), default_value)

    if required and not value:
        logger.log_message(f"Required parameter '{param_name}' is missing.", level="error")
        raise ValueError(f"Required parameter '{param_name}' is missing.")

    return value

def get_key_columns_list(key_columns: str) -> list:
    """
    Retrieves the list of key columns from the provided key_columns string.
    Args:
        key_columns (str): A comma-separated string of key columns.
    Returns:
        list: A list of key columns.
    Raises:
        ValueError: If key_columns is empty or not provided.
    """
    if not key_columns:
        raise ValueError("ERROR: No KeyColumns defined!")

    # Convert key_columns to a list and strip any extra spaces
    return [col.strip() for col in key_columns.split(',')]