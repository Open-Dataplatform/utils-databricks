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


def json_schema_to_spark_struct(schema_file_path, definitions=None):
    """
    Reads a JSON schema file, parses it, and converts it to a PySpark StructType.

    Args:
        schema_file_path (str): Path to the JSON schema file.
        definitions (dict, optional): The schema definitions. Defaults to None.

    Returns:
        tuple: A tuple containing the original JSON schema as a dictionary and the corresponding PySpark StructType.
    """
    try:
        # Read and parse the schema JSON file
        with open(schema_file_path, "r") as f:
            json_schema = json.load(f)
    except Exception as e:
        raise ValueError(
            f"Failed to load or parse schema file '{schema_file_path}': {e}"
        )

    # Initialize definitions if not provided
    if definitions is None:
        definitions = json_schema.get("definitions", {})

    def resolve_ref(ref):
        """Resolve a JSON schema $ref to its definition."""
        ref_path = ref.split("/")[-1]
        return definitions.get(ref_path, {})

    def parse_type(field_props):
        """Recursively parse the JSON schema properties and map them to PySpark data types."""
        if isinstance(field_props, list):
            # Handle lists of possible types, ignoring 'null' if present
            valid_types = [
                parse_type(fp) for fp in field_props if fp.get("type") != "null"
            ]
            return valid_types[0] if valid_types else StringType()

        # Resolve references if present
        if "$ref" in field_props:
            field_props = resolve_ref(field_props["$ref"])

        # Determine the JSON type and map it to a corresponding PySpark type
        json_type = field_props.get("type")
        if isinstance(json_type, list):
            # Handle lists of types (e.g., ["null", "string"])
            json_type = next((t for t in json_type if t != "null"), None)

        # Map JSON types to PySpark types
        if json_type == "string":
            return StringType()
        elif json_type == "integer":
            return IntegerType()
        elif json_type == "boolean":
            return BooleanType()
        elif json_type == "number":
            return DoubleType()
        elif json_type == "array":
            items = field_props.get("items")
            return ArrayType(parse_type(items) if items else StringType())
        elif json_type == "object":
            properties = field_props.get("properties", {})
            return StructType(
                [StructField(k, parse_type(v), True) for k, v in properties.items()]
            )
        else:
            # Default to StringType for unsupported or missing types
            return StringType()

    def parse_properties(properties):
        """Parse the top-level properties in the JSON schema."""
        return StructType(
            [
                StructField(name, parse_type(props), True)
                for name, props in properties.items()
            ]
        )

    # Return both the original JSON schema as a dictionary and the parsed PySpark StructType
    return json_schema, parse_properties(json_schema.get("properties", {}))


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
