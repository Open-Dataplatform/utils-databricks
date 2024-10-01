# File: custom_utils/path_utils/path_utils.py

def generate_source_path(source_environment, source_datasetidentifier):
    """
    Generate the folder path for source files.

    Args:
        source_environment (str): The storage account/environment.
        source_container (str): The container where the source data resides.
        source_datasetidentifier (str): The dataset identifier for the source data.

    Returns:
        str: The source folder path.
    """
    return f"/mnt/{source_environment}/{source_datasetidentifier}"

def generate_source_file_path(source_folder_path, source_filename):
    """
    Generate the full file path for source files.

    Args:
        source_folder_path (str): The folder path of the source files.
        source_filename (str): The filename pattern for the source files.

    Returns:
        str: The full file path for the source files.
    """
    return f"{source_folder_path}/{source_filename}"

def generate_schema_path(source_environment, schema_folder_name, source_datasetidentifier):
    """
    Generate the folder path for schema files.

    Args:
        source_environment (str): The storage account/environment.
        source_container (str): The container where the source data resides.
        schema_folder_name (str): The name of the schema folder.
        source_datasetidentifier (str): The dataset identifier for the schema files.

    Returns:
        str: The schema folder path.
    """
    return f"/mnt/{source_environment}/{schema_folder_name}/{source_datasetidentifier}"

def generate_schema_file_path(schema_folder_path, source_schema_filename):
    """
    Generate the full file path for schema files.

    Args:
        schema_folder_path (str): The folder path of the schema files.
        source_schema_filename (str): The filename for the schema file.

    Returns:
        str: The full file path for the schema files.
    """
    return f"{schema_folder_path}/{source_schema_filename}.json"