# File: custom_utils/path_utils/path_utils.py
from pathlib import Path
def generate_source_path(source_environment: str, source_datasetidentifier: str, base: str = "") -> str:
    """
    Generate the folder path for source files.

    Args:
        source_environment (str): The storage account/environment.
        source_datasetidentifier (str): The dataset identifier for the source data.
        base (str): base of path

    Returns:
        str: The source folder path.
    """
    if not base:
        base = "/mnt"
    base_path: Path = Path(base)
    path: Path = base_path/source_environment/source_datasetidentifier
    return str(path)

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

def generate_schema_path(source_environment: str, schema_folder_name: str, source_datasetidentifier: str, base: str = "") -> str:
    """
    Generate the folder path for schema files.

    Args:
        source_environment (str): The storage account/environment.
        schema_folder_name (str): The name of the schema folder.
        source_datasetidentifier (str): The dataset identifier for the schema files.
        base (str): base of path

    Returns:
        str: The schema folder path.
    """
    if not base:
        base = "/mnt"
    base_path: Path = Path(base)
    path: Path = base_path/source_environment/schema_folder_name/source_datasetidentifier
    return str(path)

def generate_schema_file_path(schema_folder_path: str, source_schema_filename: str) -> str:
    """
    Generate the full file path for schema files.

    Args:
        schema_folder_path (str): The folder path of the schema files.
        source_schema_filename (str): The filename for the schema file.

    Returns:
        str: The full file path for the schema files.
    """
    dir_path: Path = Path(schema_folder_path)
    full_path: Path = dir_path/(f"{source_schema_filename}.json")
    return str(full_path)