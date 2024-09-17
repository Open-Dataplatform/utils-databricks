# File: custom_utils/dp_storage/reader.py

"""Functions related to reading from storage"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
    DecimalType,
    DateType,
    BinaryType,
    StructType,
    FloatType,
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


def get_path_to_triggering_file(
    folder_path: str, filename: str, config_for_triggered_dataset: dict
) -> str:
    """Returns path to file that triggered a storage event in Azure.

    :param folder_path:     @triggerBody().folderPath from ADF
    :param filename:        @triggerBody().fileName from ADF
    :param config_for_triggered_dataset:    Format: {"type": "adls", "dataset": "<dataset_name>",
                                                     "container": "<container>", "account": "<storage_account>"}.
    """

    verify_source_path_and_source_config(folder_path, config_for_triggered_dataset)

    directory = "/".join(
        folder_path.split("/")[1:]
    )  # Remember that folderPath from an ADF trigger has the format "<container>/<directory>"
    mount_point = get_mount_point_name(config_for_triggered_dataset["account"])

    file_path = os.path.join(mount_point, directory, filename)

    return file_path


def verify_source_path_and_source_config(
    folder_path: str, config_for_triggered_dataset: dict
):
    """Verify that config and trigger parameters are aligned

    :param folder_path:                     @triggerBody().folderPath from ADF
    :param config_for_triggered_dataset:    Get this from source_config['identifier_for_dataset']
    """
    container_from_trigger = folder_path.split("/")[0]
    identifier_from_trigger = folder_path.split("/")[1]

    assert container_from_trigger == config_for_triggered_dataset["container"]
    assert identifier_from_trigger == config_for_triggered_dataset["dataset"]


def get_type_mapping() -> dict:
    """
    Returns a dictionary that maps JSON data types to corresponding PySpark SQL types.

    The mappings are useful when converting JSON schemas into Spark DataFrame schemas,
    ensuring correct data types are assigned during parsing.

    Returns:
        dict: A dictionary where:
            - Keys are JSON data types as strings (e.g., "string", "integer").
            - Values are corresponding PySpark SQL data types (e.g., StringType(), IntegerType()).
    """
    return {
        "string": StringType(),  # Maps JSON "string" to PySpark's StringType
        "boolean": BooleanType(),  # Maps JSON "boolean" to PySpark's BooleanType
        "number": FloatType(),  # Maps JSON "number" to PySpark's FloatType (default for numbers)
        "integer": IntegerType(),  # Maps JSON "integer" to PySpark's IntegerType
        "long": LongType(),  # Maps JSON "long" (if specified) to PySpark's LongType
        "double": FloatType(),  # Maps JSON "double" to PySpark's FloatType
        "array": StringType(),  # Treats arrays as strings (flattening scenarios)
        "object": StringType(),  # Treats objects as strings (flattening scenarios)
        "datetime": TimestampType(),  # Maps JSON "datetime" to PySpark's TimestampType
        "decimal": FloatType(),  # Maps JSON "decimal" to PySpark's FloatType
        "date": DateType(),  # Maps JSON "date" to PySpark's DateType
        "time": StringType(),  # Treats time as a string (time-only types)
        "binary": BinaryType(),  # Maps binary data to PySpark's BinaryType
    }
