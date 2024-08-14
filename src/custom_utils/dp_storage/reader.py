"""Functions related to reading from storage"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType, BooleanType, DoubleType, IntegerType, LongType, TimestampType, DecimalType,
    DateType, BinaryType, StructType, FloatType
)
from .connector import get_mount_point_name


def get_dataset_path(data_config: dict) -> str:
    """Extracts path to mounted dataset

    :param data_config: Format: {"type": "adls", "dataset": "<dataset_name>", "container": "<container>",
                                 "account": "<storage_account>"}
    """
    mount_point = get_mount_point_name(data_config["account"])
    dataset_path = f'{mount_point}/{data_config["dataset"]}'
    return dataset_path


def get_path_to_triggering_file(folder_path: str, filename: str, config_for_triggered_dataset: dict) -> str:
    """Returns path to file that triggered a storage event in Azure.

    :param folder_path:     @triggerBody().folderPath from ADF
    :param filename:        @triggerBody().fileName from ADF
    :param config_for_triggered_dataset:    Format: {"type": "adls", "dataset": "<dataset_name>",
                                                     "container": "<container>", "account": "<storage_account>"}.
    """

    verify_source_path_and_source_config(folder_path, config_for_triggered_dataset)

    directory = '/'.join(folder_path.split('/')[1:])  # Remember that folderPath from an ADF trigger has the format "<container>/<directory>"
    mount_point = get_mount_point_name(config_for_triggered_dataset['account'])

    file_path = os.path.join(mount_point, directory, filename)

    return file_path


def get_path_to_triggering_file_extended(folder_path: str, filename: str, storage_account: str, container: str, datasetidentifier: str, schema: bool = False) -> str:
    """
    Returns the path to the file that triggered a storage event in Azure.
    If 'schema' is True, the path will start with '/dbfs/'.
    
    Args:
        folder_path (str): The folder path.
        filename (str): The filename.
        storage_account (str): The storage account name.
        container (str): The container name.
        datasetidentifier (str): The dataset identifier.
        schema (bool, optional): Whether the path is for a schema file. Defaults to False.
    
    Returns:
        str: The full path to the triggering file.
    """
    verify_source_path_and_source_config_extended(folder_path, container, datasetidentifier)
    directory = '/'.join(folder_path.split('/')[1:])
    base_path = f"/dbfs{get_mount_point_name(storage_account)}" if schema else get_mount_point_name(storage_account)
    full_path = os.path.join(base_path, directory, filename)
    return full_path

def verify_source_path_and_source_config_extended(folder_path: str, container: str, datasetidentifier: str):
    """
    Verifies that the config and trigger parameters are aligned.
    
    Args:
        folder_path (str): The folder path.
        container (str): The container name.
        datasetidentifier (str): The dataset identifier.
    
    Raises:
        AssertionError: If the container or dataset identifier does not match.
    """
    container_from_trigger = folder_path.split('/')[0]
    identifier_from_trigger = folder_path.split('/')[2] if "schemachecks" in folder_path else folder_path.split('/')[1]

    if container_from_trigger != container:
        raise AssertionError(f"Expected container '{container}', but got '{container_from_trigger}'")
    if identifier_from_trigger != datasetidentifier:
        raise AssertionError(f"Expected dataset identifier '{datasetidentifier}', but got '{identifier_from_trigger}'")


def verify_source_path_and_source_config(folder_path: str, config_for_triggered_dataset: dict):
    """Verify that config and trigger parameters are aligned

    :param folder_path:                     @triggerBody().folderPath from ADF
    :param config_for_triggered_dataset:    Get this from source_config['identifier_for_dataset']
    """
    container_from_trigger = folder_path.split('/')[0]
    identifier_from_trigger = folder_path.split('/')[1]

    assert container_from_trigger == config_for_triggered_dataset['container']
    assert identifier_from_trigger == config_for_triggered_dataset['dataset']


def get_json_depth(json_schema, current_depth=0, definitions=None) -> int:
    """
    Recursively determines the maximum depth of a JSON schema, including handling references.
    
    Args:
        json_schema (dict): A JSON schema represented as a dictionary.
        current_depth (int): The current depth level (used internally).
        definitions (dict, optional): Definitions from the JSON schema to resolve $ref references. Defaults to None.
    
    Returns:
        int: The maximum depth level of the JSON schema.
    """
    if definitions is None:
        definitions = json_schema.get('definitions', {})
        
    if isinstance(json_schema, dict):
        if '$ref' in json_schema:
            ref_schema = definitions.get(json_schema['$ref'].split('/')[-1], {})
            return get_json_depth(ref_schema, current_depth, definitions)
        
        if 'properties' in json_schema:
            return max(get_json_depth(v, current_depth + 1, definitions) for v in json_schema['properties'].values())
        
        if 'items' in json_schema:
            return get_json_depth(json_schema['items'], current_depth + 1, definitions)
        
        return current_depth
    return current_depth


def get_type_mapping() -> dict:
    """
    Returns a dictionary mapping JSON data types to PySpark SQL types.
    
    Returns:
        dict: A dictionary where keys are JSON data types as strings and values are PySpark SQL data types.
    """
    return {
        "string": StringType(),
        "boolean": BooleanType(),
        "number": FloatType(),
        "integer": IntegerType(),
        "long": LongType(),
        "double": FloatType(),
        "array": StringType(),
        "object": StringType(),
        "datetime": TimestampType(),
        "decimal": FloatType(),
        "date": DateType(),
        "time": StringType(),
        "binary": BinaryType()
    }

def get_columns_of_interest(df: DataFrame) -> str:
    """
    Returns the columns of interest as a string, excluding 'input_file_name'.
    
    Args:
        df (DataFrame): A PySpark DataFrame after flattening.
    
    Returns:
        str: A string of columns of interest, excluding 'input_file_name'.
    """
    # Exclude 'input_file_name' from the list of columns of interest
    columns_of_interest = [col for col in df.columns if col != 'input_file_name']
    columns_of_interest_str = ', '.join(columns_of_interest)
    
    return columns_of_interest_str
