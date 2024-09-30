from pyspark.sql.utils import AnalysisException
from custom_utils.config.config import Config
from custom_utils.file_handler.file_handler import FileHandler
from custom_utils.logging.logger import Logger

class Validator:
    def __init__(self, config: Config, logger: Logger = None, debug=None):
        self.config = config
        self.dbutils = config.dbutils
        self.logger = logger if logger else config.logger
        self.debug = debug if debug is not None else config.debug
        self.logger.debug = self.debug
        self.file_handler = FileHandler(config)

    def verify_paths_and_files(self):
        self.logger.log_start("verify_paths_and_files")

        try:
            schema_file_path = schema_file_name = file_type = None

            # Strip "/dbfs" from paths for dbutils.fs functions
            schema_folder_path = self._strip_dbfs_prefix(self.config.source_schema_folder_path)
            source_folder_path = self._strip_dbfs_prefix(self.config.source_folder_path)

            # Schema validation only if 'use_schema' is True
            if self.config.use_schema:
                if not self._directory_exists(schema_folder_path):
                    raise Exception(f"Schema directory does not exist: {schema_folder_path}")

                schema_file_path, schema_file_name, file_type = self._verify_schema_folder()

            # Verify source folder and files
            if not self._directory_exists(source_folder_path):
                raise Exception(f"Source directory does not exist: {source_folder_path}")

            full_source_file_path, matched_files = self._verify_source_folder()

            # Infer file_type if schema is not available
            if not file_type:
                file_type = self._infer_file_type_from_files(matched_files)

            self._log_path_validation(len(matched_files))
            self._log_file_validation(schema_file_name, matched_files, file_type, self.config.source_filename)

            self.logger.log_end("verify_paths_and_files", success=True, additional_message="Proceeding with notebook execution.")
            return schema_file_path, full_source_file_path, matched_files, file_type

        except Exception as e:
            error_message = f"Failed to validate paths or files: {str(e)}"
            self.logger.log_error(error_message)
            self.logger.log_end("verify_paths_and_files", success=False, additional_message="Check error logs for details.")
            self.logger.exit_notebook(error_message, self.dbutils)

    def _strip_dbfs_prefix(self, path: str) -> str:
        """Remove '/dbfs' prefix for dbutils.fs compatibility."""
        if path and path.startswith('/dbfs'):
            return path[5:]
        return path

    def _directory_exists(self, directory_path: str) -> bool:
        """Check if a directory exists in DBFS."""
        try:
            directory_path = self._strip_dbfs_prefix(directory_path)
            self.dbutils.fs.ls(directory_path)
            return True
        except Exception:
            self.logger.log_warning(f"Directory does not exist: {directory_path}")
            return False

    def _verify_schema_folder(self) -> tuple:
        """Verify schema folder and file using paths from config."""
        try:
            schema_folder_path = self._strip_dbfs_prefix(self.config.source_schema_folder_path)
            schema_files = self.dbutils.fs.ls(schema_folder_path)
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
                error_message = f"Expected schema file not found. Available files: {available_files}"
                self.logger.log_error(error_message)
                raise Exception(error_message)

            # Return the schema file path and name
            return self.config.full_schema_file_path, found_schema_file, file_type

        except AnalysisException as e:
            error_message = f"Failed to access schema folder: {str(e)}"
            self.logger.log_error(error_message)
            raise Exception(error_message)

    def _verify_source_folder(self) -> tuple:
        """Verify source folder and files."""
        try:
            source_folder_path = self._strip_dbfs_prefix(self.config.source_folder_path)
            source_files = self.dbutils.fs.ls(source_folder_path)
            matched_files = self.file_handler.filter_files(source_files)

            if not matched_files:
                available_files = [file.name if hasattr(file, 'name') else file for file in source_files]
                error_message = f"No files matching '{self.config.source_filename}' found in {self.config.source_folder_path}. Available files: {available_files}"
                self.logger.log_error(error_message)
                raise Exception(error_message)

            # Construct the full source file path
            full_source_file_path = self.config.full_source_file_path
            return full_source_file_path, matched_files

        except AnalysisException as e:
            error_message = f"Failed to access source folder: {str(e)}"
            self.logger.log_error(error_message)
            raise Exception(error_message)

    def _infer_file_type_from_files(self, matched_files) -> str:
        """Infer file type from the extension of the matched files."""
        if not matched_files:
            raise Exception("No files found to infer file type.")
        sample_file = matched_files[0].name if hasattr(matched_files[0], 'name') else matched_files[0]
        _, ext = os.path.splitext(sample_file)
        return ext.replace('.', '')  # Remove the leading period from the extension

    def _log_path_validation(self, number_of_files):
        """Log path validation."""
        content_lines = []

        # Log schema directory path if schema is used
        if self.config.use_schema:
            content_lines.append(f"Schema directory path: {self.config.source_schema_folder_path}")

        content_lines.append(f"Source directory path: {self.config.source_folder_path}")
        content_lines.append(f"Number of files found: {number_of_files}")
        self.logger.log_block("Path Validation Results", content_lines)

    def _log_file_validation(self, schema_file_name, matched_files, file_type, source_filename):
        """Log file validation."""
        num_files = len(matched_files)
        files_to_display = matched_files[:10]  # Limit to 10 files
        more_files_text = f"...and {num_files - 10} more files." if num_files > 10 else ""

        content_lines = [
            f"File Type (file_type): {file_type}",
            f"Schema File Path (schema_file_path): {self.config.full_schema_file_path}",
            f"Data File Path (full_source_file_path): {self.config.full_source_file_path}",
            f"Files found matching the pattern '{source_filename}':"
        ] + [f"- {file.name if hasattr(file, 'name') else file}" for file in files_to_display] + ([more_files_text] if more_files_text else [])

        self.logger.log_block("File Validation Results", content_lines)