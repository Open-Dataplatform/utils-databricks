# File: custom_utils/file_handler/file_handler.py

import json
import fnmatch  # Necessary for file name pattern matching
from custom_utils.config.config import Config


class FileHandler:
    def __init__(self, config: Config):
        """
        Initialize the FileHandler with the given configuration.

        Args:
            config (Config): An instance of the Config class.
        """
        self.config = config

    def filter_files(self, files):
        """
        Filter files based on the source filename pattern.

        Args:
            files (list): List of files to filter.

        Returns:
            list: List of filenames that match the source filename pattern.
        """
        if self.config.source_filename == "*":
            return [file.name for file in files]
        return [file.name for file in files if fnmatch.fnmatch(file.name, self.config.source_filename)]