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


def get_json_depth(
    json_schema, current_depth=0, definitions=None, helper=None, depth_level=None
) -> int:
    """
    Recursively determines the maximum depth of a JSON schema, including handling references and mixed structures.

    Args:
        json_schema (dict): A JSON schema represented as a dictionary.
        current_depth (int): The current depth level (used internally).
        definitions (dict, optional): Definitions from the JSON schema to resolve $ref references. Defaults to None.
        helper (object, optional): Helper object used for logging. If provided, logs the maximum depth.
        depth_level (int, optional): The specified flattening depth level for comparison in the log message.

    Returns:
        int: The maximum depth level of the JSON schema.
    """

    def calculate_depth(schema, current_depth, definitions):
        # Handle $ref references
        if isinstance(schema, dict) and "$ref" in schema:
            ref_key = schema["$ref"].split("/")[-1]
            ref_schema = definitions.get(ref_key, {})
            if ref_schema:
                return calculate_depth(ref_schema, current_depth, definitions)
            else:
                raise ValueError(f"Reference '{ref_key}' not found in definitions.")

        # Initialize the max depth as the current depth
        max_depth = current_depth

        if isinstance(schema, dict):
            # Handle properties (objects)
            if "properties" in schema:
                properties_depth = max(
                    calculate_depth(v, current_depth + 1, definitions)
                    for v in schema["properties"].values()
                )
                max_depth = max(max_depth, properties_depth)

            # Handle items (arrays)
            if "items" in schema:
                # Only increase depth if items contain nested structures
                if isinstance(schema["items"], dict):
                    items_depth = calculate_depth(
                        schema["items"], current_depth + 1, definitions
                    )
                    max_depth = max(max_depth, items_depth)

        if isinstance(schema, list):
            # Handle cases where items is a list of objects
            list_depths = [
                calculate_depth(item, current_depth + 1, definitions) for item in schema
            ]
            max_depth = max(max_depth, *list_depths)

        return max_depth

    # Calculate the maximum depth using the recursive helper function
    max_depth = calculate_depth(json_schema, current_depth, definitions)

    # Log the depth once if the helper is provided
    if helper:
        helper.write_message(f"Maximum depth level of the JSON schema: {max_depth}; Flattened depth level of the JSON file: {depth_level}")

    return max_depth


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


def get_columns_of_interest(df: DataFrame, helper=None) -> str:
    """
    Returns a comma-separated string of column names, excluding 'input_file_name'.
    Optionally logs the columns if a helper is provided.

    Args:
        df (DataFrame): A PySpark DataFrame from which columns are extracted.
        helper (optional): An optional logging helper object. If provided, logs the columns of interest.

    Returns:
        str: A string containing column names, separated by commas, excluding 'input_file_name'.
    """
    # Generate a list of column names excluding 'input_file_name'
    columns_of_interest = [col for col in df.columns if col != "input_file_name"]

    # Join the column names into a single string, separated by commas
    columns_of_interest_str = ", ".join(columns_of_interest)

    # Log the columns if a helper is provided
    if helper:
        helper.write_message(
            f"Columns of interest (excluding 'input_file_name'): {columns_of_interest_str}"
        )

    return columns_of_interest_str