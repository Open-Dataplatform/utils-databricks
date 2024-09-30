# File: custom_utils/validation/validation.py

import os
from pyspark.sql.utils import AnalysisException
from custom_utils.config.config import Config
from custom_utils.file_handler.file_handler import FileHandler
from custom_utils.logging.logger import Logger

class Validator:
    def __init__(self, config: Config, logger: Logger = None, debug=None):
        """
        Initialize the Validator with the given configuration.

        Args:
            config (Config): An instance of the Config class containing configuration parameters.
            logger (Logger, optional): An instance of Logger for logging. Defaults to None.
            debug (bool, optional): Flag to control the verbosity of logging. If None, use the config's debug setting.
        """
        self.config = config
        self.dbutils = config.dbutils
        
        # Use the logger from config if not provided
        self.logger = logger if logger else config.logger
        
        # Use the debug flag from the parameter if provided, else use config's debug setting
        self.debug = debug if debug is not None else config.debug
        
        # Update the logger's debug flag to match the one passed to Validator
        self.logger.debug = self.debug

        # Create a new FileHandler with the config
        self.file_handler = FileHandler(config)

    def verify_paths_and_files(self):
        """
        Verify that the schema folder, schema file, and source folder exist and contain the expected files.
        Returns the schema file path, full source file path, matched files, and file type.

        Returns:
            tuple: Schema file path, full source file path, matched files, and file type.

        Raises:
            Exception: If validation fails.
        """
        # Log the start of the process
        self.logger.log_start("verify_paths_and_files")

        try:
            # Retrieve the mount point for the source environment
            mount_point = self._get_mount_point()

            # Verify schema folder and file
            schema_file_path, schema_directory_path, schema_file_name, file_type = self._verify_schema_folder(mount_point)

            # Verify source folder and files
            source_directory_path, full_source_file_path, matched_files = self._verify_source_folder(mount_point)

            # Log path validation results
            self._log_path_validation(schema_directory_path, source_directory_path, len(matched_files))

            # Log file validation results
            self._log_file_validation(schema_file_name, matched_files, file_type, self.config.source_filename)

            # Log the end of the process with the additional message
            self.logger.log_end("verify_paths_and_files", success=True, additional_message="Proceeding with notebook execution.")

            # Return schema file path, full source file path, matched files, and file type
            return schema_file_path, full_source_file_path, matched_files, file_type

        except Exception as e:
            # Log error message
            error_message = f"Failed to validate paths or files: {str(e)}"
            self.logger.log_error(error_message)

            # Log the end of the process with failure
            self.logger.log_end("verify_paths_and_files", success=False, additional_message="Check error logs for details.")

            # Exit the notebook with an error
            self.logger.exit_notebook(error_message, self.dbutils)

    def _verify_schema_folder(self, mount_point: str) -> tuple:
        """
        Verify the schema folder and the expected schema file.

        Args:
            mount_point (str): The mount point path.

        Returns:
            tuple: The schema file path (with '/dbfs/' prefix), schema directory path, schema file name, and file type.

        Raises:
            Exception: If the schema file is not found or if an error occurs.
        """
        schema_directory_path = os.path.join(mount_point, self.config.schema_folder_name, self.config.source_datasetidentifier)

        try:
            schema_files = self.dbutils.fs.ls(schema_directory_path)
            expected_schema_filename = f"{self.config.source_datasetidentifier}_schema"
            schema_format_mapping = {".json": "json", ".xsd": "xml"}

            found_schema_file = None
            file_type = None

            for file in schema_files:
                file_name = file.name if hasattr(file, 'name') else file
                for ext, ftype in schema_format_mapping.items():
                    if file_name == f"{expected_schema_filename}{ext}":
                        found_schema_file = file_name
                        file_type = ftype
                        break

            if not found_schema_file:
                available_files = [file.name if hasattr(file, 'name') else file for file in schema_files]
                error_message = (f"Expected schema file '{expected_schema_filename}.json' or "
                                 f"'{expected_schema_filename}.xsd' not found in {schema_directory_path}. "
                                 f"Available files: {available_files}")
                self.logger.log_error(error_message)
                raise Exception(error_message)
            
            # Construct the schema file path
            schema_file_path = f"/dbfs{os.path.join(schema_directory_path, found_schema_file)}"

            return schema_file_path, schema_directory_path, found_schema_file, file_type

        except AnalysisException as e:
            error_message = f"Failed to access schema folder: {str(e)}"
            self.logger.log_error(error_message)
            raise Exception(error_message)

    def _verify_source_folder(self, mount_point: str) -> tuple:
        """
        Verify the source folder and the expected files.

        Args:
            mount_point (str): The mount point path.

        Returns:
            tuple: The source directory path, full source file path (including filename with wildcard), and a list of matched files.

        Raises:
            Exception: If no matching files are found or if an error occurs.
        """
        source_directory_path = os.path.join(mount_point, self.config.source_datasetidentifier)

        try:
            source_files = self.dbutils.fs.ls(source_directory_path)
            matched_files = self.file_handler.filter_files(source_files)

            if not matched_files:
                available_files = [file.name if hasattr(file, 'name') else file for file in source_files]
                error_message = f"No files matching '{self.config.source_filename}' found in {source_directory_path}. Available files: {available_files}"
                self.logger.log_error(error_message)
                raise Exception(error_message)

            # Construct the full source file path with wildcard pattern
            full_source_file_path = os.path.join(source_directory_path, self.config.source_filename)

            return source_directory_path, full_source_file_path, matched_files

        except AnalysisException as e:
            error_message = f"Failed to access source folder: {str(e)}"
            self.logger.log_error(error_message)
            raise Exception(error_message)

    def _log_path_validation(self, schema_directory_path, source_directory_path, number_of_files):
        """
        Log the results of the path validation.

        Args:
            schema_directory_path (str): The path to the schema directory.
            source_directory_path (str): The path to the source directory.
            number_of_files (int): The number of files found in the source directory.
        """
        schema_directory_path = schema_directory_path if not schema_directory_path.startswith('/dbfs/') else schema_directory_path[5:]
        content_lines = [
            f"Schema directory path: {schema_directory_path}",
            f"Source directory path: {source_directory_path}",
            f"Number of files found: {number_of_files}"
        ]
        self.logger.log_block("Path Validation Results", content_lines)

    def _log_file_validation(self, schema_file_name, matched_files, file_type, source_filename):
        """
        Log the results of the file validation.

        Args:
            schema_file_name (str): The name of the schema file.
            matched_files (list): List of files that match the source filename pattern.
            file_type (str): The type of the schema file (e.g., 'json' or 'xml').
            source_filename (str): The filename pattern used for matching.
        """
        num_files = len(matched_files)
        files_to_display = matched_files[:10]  # Limit to 10 files
        more_files_text = f"...and {num_files - 10} more files." if num_files > 10 else ""

        content_lines = [
            f"File Type: {file_type}",
            f"Schema file name: {schema_file_name}",
            f"Files found matching the pattern '{source_filename}':"
        ] + [f"- {file.name if hasattr(file, 'name') else file}" for file in files_to_display] + ([more_files_text] if more_files_text else [])

        self.logger.log_block("File Validation Results", content_lines)

    def _get_mount_point(self) -> str:
        """
        Retrieve the mount point for the specified source environment.

        Returns:
            str: The mount point path.

        Raises:
            Exception: If the mount point is not found or if an error occurs.
        """
        try:
            target_mount = [
                m.mountPoint
                for m in self.dbutils.fs.mounts()
                if self.config.source_environment in m.source
            ]
            if not target_mount:
                error_message = f"No mount point found for environment: {self.config.source_environment}"
                self.logger.log_error(error_message)
                raise Exception(error_message)

            return target_mount[0]

        except Exception as e:
            error_message = f"Error while retrieving mount points: {str(e)}"
            self.logger.log_error(error_message)
            raise Exception(error_message)