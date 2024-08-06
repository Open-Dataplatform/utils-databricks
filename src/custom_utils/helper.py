"""Helper common functions"""

def write_message(message):
    """Log or print a message."""
    print(message)

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
        write_message(f"Could not get parameter {param_name}: {e}")
        return default_value
