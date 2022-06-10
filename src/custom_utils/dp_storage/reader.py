"""Functions related to reading from storage"""

import os


def get_path_to_triggering_file(folder_path: str, filename: str, config_for_triggered_dataset: dict) -> str:
    """Returns path to file that triggered a storage event in Azure.

    :param folder_path:     @triggerBody().folderPath from ADF
    :param filename:        @triggerBody().fileName from ADF
    :param config_for_triggered_dataset:    Format: {"type": "adls", "dataset": "<dataset_name>", "container": "<container>",
                                                      "account": "<storage_account>", "mount_point": "/mnt/<storage_account>"}.
    """

    verify_source_path_and_source_config(folder_path, config_for_triggered_dataset)

    directory = '/'.join(folder_path.split('/')[1:])  # Remember that folderPath from an ADF trigger has the format "<container>/<directory>"
    file_path = os.path.join(config_for_triggered_dataset['mount_point'], directory, filename)

    return file_path


def verify_source_path_and_source_config(folder_path: str, config_for_triggered_dataset: dict):
    """Verify that config and trigger parameters are aligned

    :param folder_path:                     @triggerBody().folderPath from ADF
    :param config_for_triggered_dataset:    Get this from source_config['identifier_for_dataset']
    """
    container_from_trigger = folder_path.split('/')[0]
    identifier_from_trigger = folder_path.split('/')[1]

    assert container_from_trigger == config_for_triggered_dataset['container']
    assert identifier_from_trigger == config_for_triggered_dataset['dataset']
