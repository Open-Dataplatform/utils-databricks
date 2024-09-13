import json
import fnmatch  # Necessary for file name pattern matching
from custom_utils.config.config import Config


class FileHandler:
    def __init__(self, config: Config):
        """
        Initialize the FileHandler with the provided configuration.
        
        Args:
            config (Config): Configuration object containing ADF details, paths, and authentication strategy.
        """
        self.config = config
        self.error_log_path = config.full_schema_file_path
        self.output_file_path = config.full_source_file_path

    def update_source_filename(self, source_filename):
        """
        Update the source filename in the configuration.
        
        Args:
            source_filename (str): The new source file name or pattern.
        """
        self.config.update_source_filename(source_filename)

    def log_errors(self, error_log):
        """
        Log any errors that occurred during the process to a file.
        
        Args:
            error_log (dict): A dictionary containing the errors to be logged.
        """
        if error_log:
            with open(self.error_log_path, "a") as log_file:
                json.dump(error_log, log_file, indent=4)
            print(f"Errors logged to {self.error_log_path}")