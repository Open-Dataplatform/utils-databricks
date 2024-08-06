"""Functions related to reading from storage"""

import os

from .connector import get_mount_point_name
from pyspark.sql import SparkSession


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

def get_path_to_triggering_file_extended(folder_path: str, filename: str, storage_account: str, source_container: str, source_datasetidentifier: str) -> str:
    """Returns path to file that triggered a storage event in Azure."""
    verify_source_path_and_source_config_extended(folder_path, source_container, source_datasetidentifier)
    directory = '/'.join(folder_path.split('/')[1:])  # Format "<container>/<directory>"
    mount_point = get_mount_point_name(storage_account)
    return os.path.join(mount_point, directory, filename)


def verify_source_path_and_source_config(folder_path: str, config_for_triggered_dataset: dict):
    """Verify that config and trigger parameters are aligned

    :param folder_path:                     @triggerBody().folderPath from ADF
    :param config_for_triggered_dataset:    Get this from source_config['identifier_for_dataset']
    """
    container_from_trigger = folder_path.split('/')[0]
    identifier_from_trigger = folder_path.split('/')[1]

    assert container_from_trigger == config_for_triggered_dataset['container']
    assert identifier_from_trigger == config_for_triggered_dataset['dataset']

def verify_source_path_and_source_config_extended(folder_path: str, container: str, datasetidentifier: str):
    """Verify that config and trigger parameters are aligned."""
    container_from_trigger = folder_path.split('/')[0]
    identifier_from_trigger = folder_path.split('/')[2] if "schemachecks" in folder_path else folder_path.split('/')[1]

    if container_from_trigger != container:
        raise AssertionError(f"Expected container '{container}', but got '{container_from_trigger}'")
    if identifier_from_trigger != datasetidentifier:
        raise AssertionError(f"Expected dataset identifier '{datasetidentifier}', but got '{identifier_from_trigger}'")

def read_json_schema(spark: SparkSession, file_path: str) -> dict:
    """Reads a JSON schema from a given path using Spark."""
    text_df = spark.read.text(file_path)
    json_str = ''.join(text_df.select('value').rdd.flatMap(lambda x: x).collect())
    return json.loads(json_str)

def get_col_types_from_schema(schema: dict, prefix: str = '', type_mapping: dict = None, flatten_completely: bool = True) -> dict:
    """Convert JSON schema to column definitions."""
    if type_mapping is None:
        type_mapping = {
            "string": "string",
            "boolean": "boolean",
            "number": "float",
            "integer": "int",
            "long": "bigint",
            "double": "float",
            "array": "string",  # Could be expanded to handle arrays differently if needed
            "object": "string",  # Default for complex/nested types
            "datetime": "timestamp",  # Handle datetime type explicitly
            "decimal": "decimal(38, 10)",  # For high precision decimal numbers
            "date": "date",  # For date-only fields
            "time": "time",  # For time-only fields
            "binary": "binary"  # For binary data
        }

    def handle_field(field_name: str, field_info: dict, col_types: dict):
        """Handle a single field in the schema."""
        col_type = field_info.get("type", "string")
        if isinstance(col_type, list):
            col_type = col_type[0]  # Handle the first type in the list for simplicity

        if flatten_completely and col_type in ["object", "array"]:
            return
        col_types[field_name] = type_mapping.get(col_type, "string")

    def expand_properties(properties: dict, prefix: str, col_types: dict):
        """Expand properties to get their data types."""
        for field, field_info in properties.items():
            field_name = f"{prefix}{field}"
            handle_field(field_name, field_info, col_types)
            if flatten_completely:
                if field_info.get("type") == "object" and "properties" in field_info:
                    expand_properties(field_info["properties"], f"{field_name}_", col_types)
                if field_info.get("type") == "array" and "items" in field_info:
                    item_info = field_info["items"]
                    if "properties" in item_info:
                        expand_properties(item_info["properties"], f"{field_name}_", col_types)

    col_types = {}
    expand_properties(schema.get("properties", {}), prefix, col_types)

    if flatten_completely and "definitions" in schema:
        for def_key, def_val in schema["definitions"].items():
            expand_properties(def_val.get("properties", {}), f"{def_key}_", col_types)

    if flatten_completely:
        col_types = {k: v for k, v in col_types.items() if not v.startswith('array')}

    return col_types
