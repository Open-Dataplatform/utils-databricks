
"""Helper common functions"""

from custom_utils import adf

def write_message(message, level="info"):
    """Log or print a message with an optional log level."""
    print(f"[{level.upper()}] {message}")

def exit_notebook(message):
    """Exit the notebook with a message."""
    write_message(message)
    dbutils.notebook.exit(message)

def get_adf_parameter(dbutils, param_name, default_value=""):
    """Get a parameter from Azure Data Factory (ADF).
    
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
        write_message(f"Could not get parameter {param_name}: {e}", level="warning")
        return default_value

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