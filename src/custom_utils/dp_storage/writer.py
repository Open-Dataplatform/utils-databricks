"""Functions related to writing to the Delta lake"""

from typing import Tuple
from .connector import get_mount_point_name

def get_destination_path(destination_config: dict) -> str:
    """Extracts destination path from destination_config"""

    data_config = list(destination_config.values())[0]
    mount_point = get_mount_point_name(data_config["account"])
    destination_path = f'{mount_point}/{data_config["dataset"]}'

    return destination_path


def get_destination_path_extended(storage_account: str, datasetidentifier: str) -> str:
    """Extracts destination path from storage account and dataset identifier.
    Args:
        storage_account (str): Storage account name.
        datasetidentifier (str): Dataset identifier.
    Returns:
        str: The destination path.
    """
    mount_point = get_mount_point_name(storage_account)
    destination_path = f"{mount_point}/{datasetidentifier}"

    return destination_path


def get_databricks_table_info(destination_config: dict) -> Tuple[str, str]:
    """Constructs database and table names to be used in Databricks."""

    data_config = list(destination_config.values())[0]
    mount_point = get_mount_point_name(data_config["account"])

    database_name = mount_point.split("/")[-1]
    table_name = data_config["dataset"]

    return database_name, table_name


def get_databricks_table_info_extended(
    storage_account: str, datasetidentifier: str
) -> Tuple[str, str]:
    """Constructs database and table names to be used in Databricks.

    Args:
        storage_account (str): Storage account name.
        datasetidentifier (str): Dataset identifier.

    Returns:
        Tuple[str, str]: The database name and table name.
    """
    mount_point = get_mount_point_name(storage_account)

    database_name = mount_point.split("/")[-1]
    table_name = datasetidentifier

    return database_name, table_name