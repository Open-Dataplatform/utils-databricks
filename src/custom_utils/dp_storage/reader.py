"""Functions related to reading from storage"""

import os
from custom_utils.dp_storage.connector import get_mount_point_name

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

    # Debug: Print input values
    print(f"folder_path: {folder_path}")
    print(f"filename: {filename}")
    print(f"config_for_triggered_dataset: {config_for_triggered_dataset}")

    verify_source_path_and_source_config(folder_path, config_for_triggered_dataset)

    # Use the full folder_path as it is
    directory = folder_path  # Debug: Print directory after assignment
    print(f"directory: {directory}")

    mount_point = get_mount_point_name(config_for_triggered_dataset['account'])  # Debug: Print mount_point after assignment
    print(f"mount_point: {mount_point}")

    file_path = os.path.join(mount_point, directory, filename)  # Debug: Print file_path after assignment
    print(f"file_path: {file_path}")

    return file_path

def verify_source_path_and_source_config(folder_path: str, config_for_triggered_dataset: dict):
    """Verify that config and trigger parameters are aligned

    :param folder_path:                     @triggerBody().folderPath from ADF
    :param config_for_triggered_dataset:    Get this from source_config['identifier_for_dataset']
    """
    container_from_trigger = folder_path.split('/')[0]
    identifier_from_trigger = folder_path.split('/')[1]

    print(f"container_from_trigger: {container_from_trigger}")  # Debug: Print container_from_trigger
    print(f"identifier_from_trigger: {identifier_from_trigger}")  # Debug: Print identifier_from_trigger

    assert container_from_trigger == config_for_triggered_dataset['container']
    assert identifier_from_trigger == config_for_triggered_dataset['dataset']
