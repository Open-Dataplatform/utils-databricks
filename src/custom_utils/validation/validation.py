# File: custom_utils/validation/validation.py

import os
from pyspark.sql.utils import AnalysisException
from custom_utils.config.config import Config
from custom_utils.file_handler.file_handler import FileHandler
from custom_utils.logging.logger import Logger

class PathValidator:
    def __init__(self, config: Config):
        """
        Initialize the PathValidator with the given configuration.

        Args:
            config (Config): An instance of the Config class containing configuration parameters.
        """
        self.config = config
        self.dbutils = config.dbutils
        self.logger = config.logger  # Use logger from the config
        self.file_handler = FileHandler(config)

    def verify_paths_and_files(self):
        """
        Verify that the schema folder, schema file, and source folder exist and contain the expected files.
        Returns the schema file path, source file paths, matched files, and file type.

        Returns:
            tuple: Full schema file path, list of full source file paths, matched files, and file type.

        Raises:
            Exception: If validation fails.
        """
        try:
            # Retrieve the mount point for the source environment
            mount_point = self._get_mount_point()

            # Verify schema folder and file
            schema_directory_path, schema_file_path, schema_file_name, file_type = self._verify_schema_folder(mount_point)

            # Verify source folder and files
            source_directory_path, source_file_paths, matched_files = self._verify_source_folder(mount_point)

            # Log path validation results
            self.logger.log_path_validation(schema_directory_path, source_directory_path, len(matched_files))

            # Log file validation results
            self.logger.log_file_validation(schema_file_name, matched_files, file_type, self.config.source_filename)

            # Log success message
            self.logger.log_message("All paths and files verified successfully. Proceeding with notebook execution.",
                                    level="info", single_info_prefix=True)

            # Return schema file path, list of full source file paths, matched files, and file type
            return schema_file_path, source_file_paths, matched_files, file_type

        except Exception as e:
            error_message = f"Failed to validate paths or files: {str(e)}"
            self.logger.log_message(error_message, level="error")
            self.logger.exit_notebook(error_message, self.dbutils)

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
                self.logger.log_message(error_message, level="error")
                raise Exception(error_message)

            return target_mount[0]

        except Exception as e:
            error_message = f"Error while retrieving mount points: {str(e)}"
            self.logger.log_message(error_message, level="error")
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
                for ext, ftype in schema_format_mapping.items():
                    if file.name == f"{expected_schema_filename}{ext}":
                        found_schema_file = file.name
                        file_type = ftype
                        break

            if not found_schema_file:
                available_files = [file.name for file in schema_files]
                error_message = (f"Expected schema file '{expected_schema_filename}.json' or "
                                 f"'{expected_schema_filename}.xsd' not found in {schema_directory_path}. "
                                 f"Available files: {available_files}")
                self.logger.log_message(error_message, level="error")
                raise Exception(error_message)

            # Construct schema file path with '/dbfs/' prefix
            schema_file_path = os.path.join("/dbfs", schema_directory_path, found_schema_file)
            schema_file_name = found_schema_file

            # Return the full schema file path with '/dbfs/' prefix
            return schema_file_path, schema_directory_path, schema_file_name, file_type

        except AnalysisException as e:
            error_message = f"Failed to access schema folder: {str(e)}"
            self.logger.log_message(error_message, level="error")
            raise Exception(error_message)

    def _verify_source_folder(self, mount_point: str) -> tuple:
        """
        Verify the source folder and the expected files.

        Args:
            mount_point (str): The mount point path.

        Returns:
            tuple: The source directory path, a list of full paths to matched files, and a list of filenames.

        Raises:
            Exception: If no matching files are found or if an error occurs.
        """
        source_directory_path = os.path.join(mount_point, self.config.source_datasetidentifier)

        try:
            source_files = self.dbutils.fs.ls(source_directory_path)
            matched_files = self.file_handler.filter_files(source_files)

            if not matched_files:
                available_files = [file.name for file in source_files]
                error_message = f"No files matching '{self.config.source_filename}' found in {source_directory_path}. Available files: {available_files}"
                self.logger.log_message(error_message, level="error")
                raise Exception(error_message)

            # Construct full paths for matched files
            matched_file_paths = [os.path.join(source_directory_path, file) for file in matched_files]

            # Return the source directory path, full paths, and filenames
            return source_directory_path, matched_file_paths, matched_files

        except AnalysisException as e:
            error_message = f"Failed to access source folder: {str(e)}"
            self.logger.log_message(error_message, level="error")
            raise Exception(error_message)