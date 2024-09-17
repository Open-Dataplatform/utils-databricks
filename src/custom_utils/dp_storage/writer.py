"""Functions related to writing to the Delta lake"""

import json
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    ArrayType,
    StructField,
    BooleanType,
    DoubleType,
)
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


def apply_type_mapping(schema: StructType, type_mapping: dict) -> StructType:
    """
    Applies a custom type mapping to a given StructType schema.

    Args:
        schema (StructType): The original schema to map.
        type_mapping (dict): A dictionary mapping original types to desired Spark SQL types.

    Returns:
        StructType: A new StructType with the custom types applied.
    """

    def map_field(field):
        field_type = field.dataType
        if isinstance(field_type, StructType):
            mapped_type = apply_type_mapping(field_type, type_mapping)
        elif isinstance(field_type, ArrayType):
            mapped_type = ArrayType(
                type_mapping.get(
                    field_type.elementType.simpleString(), field_type.elementType
                ),
                field_type.containsNull,
            )
        else:
            mapped_type = type_mapping.get(field_type.simpleString(), field_type)
        return StructField(field.name, mapped_type, field.nullable)

    return StructType([map_field(field) for field in schema.fields])