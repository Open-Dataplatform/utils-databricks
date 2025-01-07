import json
import fnmatch
import os
from typing import Optional, Tuple, List, Dict
from pyspark.sql.utils import AnalysisException
from custom_utils.config.config import Config

class FileHandler:
    """
    A class to handle file operations, including path management, file filtering,
    and logging available files.
    """

    def __init__(self, config: Config, debug: Optional[bool] = None):
        """
        Initialize the FileHandler with the given configuration and logger.

        Args:
            config (Config): An instance of the Config class.
            debug (bool, optional): Debug flag for enabling debug-level logging. 
                                    Defaults to Config.debug if not provided.
        """
        self.config = config
        self.debug = debug if debug is not None else config.debug
        self.logger = config.logger
        self.dbutils = config.dbutils

        # Update logger's debug mode
        self.logger.update_debug_mode(self.debug)

        # Log initialization details
        self.logger.log_block("FileHandler Initialization", [
            f"Debug Mode: {self.debug}",
            "FileHandler successfully initialized."
        ], level="debug")

    def manage_paths(self) -> Dict[str, str]:
        """
        Generate paths for schema and data directories based on configuration.

        Returns:
            dict: A dictionary containing the base paths for data and schema.
        """
        self.logger.log_debug("Generating paths for schema and data directories.")
        paths = {
            "data_base_path": self.config.source_data_folder_path.rstrip('/')
        }
        self.logger.log_debug(f"Data Base Path: {paths['data_base_path']}")

        if self.config.use_schema:
            paths["schema_base_path"] = self.config.source_schema_folder_path.rstrip('/')
            self.logger.log_debug(f"Schema Base Path: {paths['schema_base_path']}")
        else:
            self.logger.log_info("Schema validation is disabled.")
        return paths

    def filter_files(self, files: List[str], extensions: Optional[List[str]] = None) -> List[str]:
        """
        Filter files based on a filename pattern and optional extensions.

        Args:
            files (List[str]): List of file names.
            extensions (List[str], optional): List of allowed file extensions.

        Returns:
            List[str]: Filtered file names.
        """
        if not files:
            raise ValueError("The 'files' list is empty or None.")

        self.logger.log_debug("Filtering files based on source filename pattern.")
        filtered_files = (
            files if self.config.source_filename == "*"
            else [file for file in files if fnmatch.fnmatch(file, self.config.source_filename)]
        )

        if extensions:
            self.logger.log_debug(f"Filtering files with allowed extensions: {extensions}")
            filtered_files = [file for file in filtered_files if any(file.endswith(ext) for ext in extensions)]

        self.logger.log_debug(f"Filtered files: {filtered_files}")
        return filtered_files

    def get_similar_files(self, files: List[str], pattern: str) -> List[str]:
        """
        Find files with names similar to the given pattern.

        Args:
            files (List[str]): List of available file names.
            pattern (str): The search pattern.

        Returns:
            List[str]: Similar file names.
        """
        self.logger.log_debug(f"Finding files matching pattern: {pattern}")
        current_pattern = pattern
        similar_files = []

        while len(current_pattern) > 1:
            similar_files = [file for file in files if fnmatch.fnmatch(file, current_pattern)]
            if similar_files:
                break
            current_pattern = current_pattern[:-2] + "*"

        self.logger.log_debug(f"Similar files found: {similar_files}")
        return similar_files

    def directory_exists(self, directory_path: str) -> bool:
        """
        Check if a directory exists in DBFS.

        Args:
            directory_path (str): Path to the directory.

        Returns:
            bool: True if the directory exists, False otherwise.
        """
        self.logger.log_debug(f"Checking if directory exists: {directory_path}")
        try:
            self.dbutils.fs.ls(directory_path)
            self.logger.log_debug(f"Directory exists: {directory_path}")
            return True
        except FileNotFoundError:
            self.logger.log_warning(f"Directory does not exist: {directory_path}")
            return False
        except Exception as e:
            self.logger.log_error(f"Error while checking directory: {directory_path}. Error: {e}")
            return False

    def normalize_path(self, path: str) -> str:
        """
        Normalize a path to ensure consistent formatting.

        Args:
            path (str): The file path to normalize.

        Returns:
            str: The normalized file path.
        """
        self.logger.log_debug(f"Normalizing path: {path}")
        normalized_path = f"/{path.lstrip('/')}"
        self.logger.log_debug(f"Normalized path: {normalized_path}")
        return normalized_path

    def log_available_files(self, files: List[str], title: str, max_display: int = 10):
        """
        Log a list of files with a title.

        Args:
            files (List[str]): List of file names to log.
            title (str): Title for the log block.
            max_display (int): Maximum number of files to display.
        """
        self.logger.log_debug(f"Logging available files. Title: {title}")
        if not files:
            self.logger.log_block(title, ["No files found."], level="info")
            return

        content_lines = files[:max_display]
        if len(files) > max_display:
            content_lines.append("...and more.")
        self.logger.log_block(title, [f"{file}" for file in content_lines], level="info")