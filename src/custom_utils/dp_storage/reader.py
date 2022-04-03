"""Functions related to reading from storage"""

import os


def get_path_to_triggering_file(mount_point, folder_path, filename):
    """Returns path to file that triggered a storage event in Azure.
    
    :param mount_point:     The mount point returned from the functions in connector.py
    :param folder_path:     @triggerBody().folderPath from ADF
    :param filename:        @triggerBody().fileName from ADF
    """
    directory = '/'.join(folder_path.split('/')[1:])  # Remember that folderPath from an ADF trigger has the format "<container>/<directory>"
    file_path = os.path.join(mount_point, directory, filename)

    return file_path
