"""Functions related to reading from storage"""

import os


def get_path_to_triggering_file(mount_point, folder_path, filename, config_for_triggered_dataset):
    """Returns path to file that triggered a storage event in Azure.

    :param mount_point:     The mount point returned from the functions in connector.py
    :param folder_path:     @triggerBody().folderPath from ADF
    :param filename:        @triggerBody().fileName from ADF
    """

    verify_source_path_and_source_config(folder_path, config_for_triggered_dataset)

    directory = '/'.join(folder_path.split('/')[1:])  # Remember that folderPath from an ADF trigger has the format "<container>/<directory>"
    file_path = os.path.join(mount_point, directory, filename)

    return file_path


def verify_source_path_and_source_config(folder_path, config_for_triggered_dataset: dict):
    """Verify that config and trigger parameters are aligned

    :param folder_path:                     @triggerBody().folderPath from ADF
    :param config_for_triggered_dataset:    Get this from source_config['identifier_for_dataset']
    """
    container_from_trigger = folder_path.split('/')[0]
    identifier_from_trigger = folder_path.split('/')[1]

    assert container_from_trigger == config_for_triggered_dataset['container']
    assert identifier_from_trigger == config_for_triggered_dataset['dataset']
