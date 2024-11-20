import os
from pyspark.sql.utils import AnalysisException
from custom_utils.logging.logger import Logger
from custom_utils.config.config import Config
from custom_utils.file_handler.file_handler import FileHandler

class Validator:
    # Class-level attributes for notebook exit flags
    exit_notebook = False
    exit_notebook_message = ""

    def __init__(self, config: Config, logger: Logger = None, debug=None):
        """
        Validator class for verifying directories and files in DBFS, 
        including schema and file type inference.
        """
        self.config = config
        self.dbutils = config.dbutils
        self.logger = logger if logger else config.logger
        self.debug = debug if debug is not None else config.debug
        self.logger.debug = self.debug
        self.file_handler = FileHandler(config)
        self.logger.log_info("Validator initialized successfully.")

        # Reset class-level attributes on initialization
        Validator.exit_notebook = False
        Validator.exit_notebook_message = ""

    def _get_additional_schema_paths(self, schema_folder_path: str) -> list:
        """
        Retrieves additional schema files from the schema folder.

        Args:
            schema_folder_path (str): Path to the schema folder.

        Returns:
            list: List of additional schema file names found in the folder.
        """
        try:
            all_schema_files = self.dbutils.fs.ls(schema_folder_path)
            additional_schema_files = [file.name for file in all_schema_files if file.name.endswith(".xsd")]

            if additional_schema_files:
                self.logger.log_block(
                    "Additional Schema Files Found",
                    [f"- {file}" for file in additional_schema_files]
                )

            return additional_schema_files

        except Exception as e:
            self.logger.log_error(f"Failed to retrieve additional schema files: {str(e)}")
            return []

    def _strip_dbfs_prefix(self, path: str) -> str:
        """
        Removes the '/dbfs' prefix from the provided path.

        Args:
            path (str): The path to process.

        Returns:
            str: Path without '/dbfs' prefix.
        """
        return path[5:] if path and path.startswith('/dbfs') else path

    def _directory_exists(self, directory_path: str) -> bool:
        """
        Checks whether a directory exists in DBFS.

        Args:
            directory_path (str): Path to the directory.

        Returns:
            bool: True if the directory exists, False otherwise.
        """
        try:
            self.dbutils.fs.ls(directory_path)
            return True
        except Exception:
            self.logger.log_warning(f"Directory does not exist: {directory_path}")
            return False

    def _verify_schema_folder(self) -> tuple:
        """
        Verifies the schema folder and identifies schema files based on file extensions.

        Returns:
            tuple: Schema file path, found schema file name, data file type, schema file type, and schema files set.
        """
        try:
            schema_files = self.dbutils.fs.ls(self._strip_dbfs_prefix(self.config.source_schema_folder_path))
            expected_schema_filename = f"{self.config.source_datasetidentifier}_schema"
            schema_format_mapping = {".xsd": "xml", ".json": "json"}

            found_schema_file, data_file_type, schema_file_type = None, None, None
            schema_files_set = set()

            for file in schema_files:
                file_name = file.name if hasattr(file, 'name') else file
                for ext, ftype in schema_format_mapping.items():
                    if file_name == f"{expected_schema_filename}{ext}":
                        found_schema_file = file_name
                        data_file_type = ftype
                        schema_file_type = ext.replace('.', '')
                        schema_files_set.add(found_schema_file)
                        break
                if file_name.endswith(".xsd"):
                    schema_files_set.add(file_name)

            if not found_schema_file:
                available_files = [file.name for file in schema_files]
                self._exit_notebook(f"Expected schema file not found. Available files: {available_files}")

            schema_file_path = f"{self.config.source_schema_folder_path}/{found_schema_file}"
            return schema_file_path, found_schema_file, data_file_type, schema_file_type, schema_files_set

        except AnalysisException as e:
            self._exit_notebook(f"Failed to access schema folder: {str(e)}")

    def _verify_source_folder(self) -> tuple:
        """
        Verify the source folder's existence and check for files matching the specified pattern.
        Logs detailed information and exits the notebook if no matching files are found.

        Returns:
            tuple: Total number of files in the source folder and a list of matched files.
        """
        try:
            # List all files in the source folder
            source_files = self.dbutils.fs.ls(self._strip_dbfs_prefix(self.config.source_folder_path))
            total_files = len(source_files)  # Total number of files

            # Filter files using the file handler
            search_pattern = self.config.source_filename
            matched_data_files = self.file_handler.filter_files(source_files)

            # Log the result if no matching files are found
            if not matched_data_files:
                available_files = [file.name if hasattr(file, 'name') else file for file in source_files]

                # Gather closest matches for the file pattern
                close_matches = [file for file in available_files if search_pattern.split('*')[0] in file]
                matches_to_display = close_matches[:10]
                unmatched_to_display = available_files[:10] if not matches_to_display else []

                content_lines = [
                    f"Looking for files matching the pattern: {search_pattern}",
                    "No files closely matching the filter were found."
                ]

                if matches_to_display:
                    content_lines.append("Files partially matching the filter:")
                    content_lines.extend([f"- {file}" for file in matches_to_display])
                    if len(close_matches) > 10:
                        content_lines.append("...and more.")
                else:
                    content_lines.append("Available files in the directory:")
                    content_lines.extend([f"- {file}" for file in unmatched_to_display])
                    if len(available_files) > 10:
                        content_lines.append("...and more.")

                self.logger.log_block("File Matching Summary", content_lines)

                # Exit using the centralized logic
                exit_message = f"File pattern being searched: '{search_pattern}'\nNo matching files found. Notebook execution is terminating."
                self._exit_notebook(exit_message)

            return total_files, matched_data_files

        except AnalysisException as e:
            # Handle and log errors when accessing the folder
            error_message = f"Error accessing source folder: {str(e)}"
            self.logger.log_error(error_message)
            self._exit_notebook(error_message)

    def _infer_file_type_from_files(self, matched_data_files) -> str:
        """
        Infer the file type based on the extension of the matched files.

        Args:
            matched_data_files (list): List of matched file names or file objects.

        Returns:
            str: File type inferred from the file extension.

        Raises:
            RuntimeError: If no matched files are provided.
        """
        if not matched_data_files:
            raise RuntimeError("No files found to infer file type.")

        # Extract the extension from the first matched file
        sample_file = matched_data_files[0].name if hasattr(matched_data_files[0], 'name') else matched_data_files[0]
        _, ext = os.path.splitext(sample_file)

        # Remove the leading dot and return the extension
        return ext.lstrip('.')

    def _log_path_validation(self, number_of_files: int):
        """
        Logs the results of path validation, including schema and source paths.

        Args:
            number_of_files (int): Number of files found in the source directory.
        """
        content_lines = [
            f"Source directory path: {self.config.source_folder_path}"
        ]

        # Include schema path if schema validation is enabled
        if self.config.use_schema:
            content_lines.insert(0, f"Schema directory path: {self.config.source_schema_folder_path}")

        # Log the results as a cohesive block
        self.logger.log_block("Path Validation Results", content_lines)

    def _log_file_validation(self, total_files: int, matched_data_files, data_file_type: str, data_file_path: str):
        """
        Logs details about the validated files, including counts, types, and matches.

        Args:
            total_files (int): Total number of files in the source directory.
            matched_data_files (list): List of files matching the specified filter.
            data_file_type (str): The inferred type of the data files.
            data_file_path (str): Path to the data files in the source directory.
        """
        num_files = len(matched_data_files)
        files_to_display = matched_data_files[:10]  # Limit display to the first 10 files
        more_files_text = f"...and {num_files - 10} more files." if num_files > 10 else ""

        # Prepare the log content
        content_lines = [
            f"Total number of files in source directory: {total_files}",
            f"Number of files matching filter: {num_files}",
            f"Data File Type (data_file_type): {data_file_type}",
            f"Files found matching the pattern '{self.config.source_filename}':"
        ] + [f"- {file.name if hasattr(file, 'name') else file}" for file in files_to_display]

        # Add a note about more files if applicable
        if more_files_text:
            content_lines.append(more_files_text)

        # Log the file validation results
        self.logger.log_block("File Validation Results", content_lines)

    def _log_schema_validation(self, schema_file_type: str, schema_files: set):
        """
        Logs details of validated schemas, including their type and names.

        Args:
            schema_file_type (str): The inferred schema file type (e.g., "json", "xsd").
            schema_files (set): Set of schema file names.
        """
        # Prepare the log content
        content_lines = [
            f"Number of schemas found: {len(schema_files)}",
            f"Schema File Type (schema_file_type): {schema_file_type}",
            f"Schemas found matching schema_type '{schema_file_type}':"
        ] + [f"- {file}" for file in schema_files]

        # Log the schema validation results
        self.logger.log_block("Schema Validation Results", content_lines)

    def _handle_verification_error(self, exception: Exception):
        """
        Handles errors that occur during path or file verification by logging and exiting.

        Args:
            exception (Exception): The exception encountered during verification.

        Raises:
            SystemExit: Exits the notebook after logging the error.
        """
        # Log the error message and terminate the notebook
        error_message = f"Failed to validate paths or files: {str(exception)}"
        self.logger.log_error(error_message)

        # Log the end of the process with a termination message
        self.logger.log_end("verify_paths_and_files", success=False, additional_message="Notebook execution terminated.")

        # Exit the notebook with the error message
        dbutils.notebook.exit(error_message)

    def _exit_notebook(self, message: str):
        """
        Gracefully exit the notebook with a message after displaying all logs.

        Args:
            message (str): The message to display before exiting.
        """
        if getattr(self, "_notebook_exit_triggered", False):  # Avoid duplicate exits
            return

        # Log the exit message
        self.logger.log_block("Notebook Exiting", [
            message
        ])

        # Mark the exit as triggered
        self._notebook_exit_triggered = True

        # Set the exit flag for external handling
        global exit_notebook, exit_notebook_message
        Validator.exit_notebook = True
        Validator.exit_notebook_message = message

    def verify_paths_and_files(self):
        """
        Verifies schema and source paths, checks for files, and infers file types.
        Logs validation results and exits the notebook if files are missing.

        Returns:
            tuple: Validated schema file path, data file path, matched files, and inferred file type.
        """
        self.logger.log_start("verify_paths_and_files")
        try:
            schema_file_path, found_schema_file, data_file_type, schema_file_type = None, None, None, None
            schema_files = set()
            data_file_path = None

            # Validate schema folder
            if self.config.use_schema:
                strip_schema_folder_path = self._strip_dbfs_prefix(self.config.source_schema_folder_path)
                if not self._directory_exists(strip_schema_folder_path):
                    self._exit_notebook(f"Schema directory does not exist: {strip_schema_folder_path}")

                schema_file_path, found_schema_file, data_file_type, schema_file_type, schema_files = self._verify_schema_folder()
                schema_files.add(found_schema_file)
                schema_files.update(self._get_additional_schema_paths(strip_schema_folder_path))

            # Validate source folder and files
            strip_source_folder_path = self._strip_dbfs_prefix(self.config.source_folder_path)
            if not self._directory_exists(strip_source_folder_path):
                self._exit_notebook(f"Source directory does not exist: {strip_source_folder_path}")

            total_files, matched_data_files = self._verify_source_folder()

            # Stop execution if no matched files
            if not matched_data_files:
                self._exit_notebook("No matching files found. Notebook execution is terminating.")
                return None, None, None, None  # Ensure termination

            # Infer file type if not determined via schema
            if not data_file_type:
                data_file_type = self._infer_file_type_from_files(matched_data_files)

            # Construct full paths
            data_file_path = f"{self.config.source_folder_path}"

            # Log validation results
            self._log_path_validation(len(matched_data_files))
            self._log_file_validation(total_files, matched_data_files, data_file_type, data_file_path)
            self._log_schema_validation(schema_file_type, schema_files)

            # Log successful validation
            self.logger.log_end("verify_paths_and_files", success=True, additional_message="Proceeding with notebook execution.")

            return (
                schema_file_path if self.config.use_schema else None,
                data_file_path,
                list(matched_data_files),
                data_file_type
            )

        except Exception as e:
            # Log failure and exit the notebook
            error_message = f"Failed to validate paths or files: {str(e)}"
            self.logger.log_error(error_message)
            self._exit_notebook(error_message)
