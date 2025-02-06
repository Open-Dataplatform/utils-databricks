import os
import logging
from typing import Tuple, Optional, List
from pyspark.sql.utils import AnalysisException
from custom_utils.logging.logger import Logger
from custom_utils.config.config import Config
from custom_utils.file_handler.file_handler import FileHandler

class Validator:
    """
    Optimized Validator class for verifying directories and files in DBFS,
    including schema and file type inference.
    """

    exit_notebook = False
    exit_notebook_message = ""

    def __init__(self, config: Config, debug: Optional[bool] = None, file_handler: Optional[FileHandler] = None):
        self.config = config
        self.debug = debug if debug is not None else config.debug

        # Initialize logger
        self.logger = config.logger
        self.logger.debug = self.debug
        self.logger.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

        # Initialize FileHandler
        self.file_handler = file_handler or FileHandler(config=config, debug=self.debug)

        # Initialize paths and variables
        self.paths = self.file_handler.manage_paths()
        self.schema_directory_path = None
        self.data_directory_path = None
        self.matched_data_files = []
        self.schema_file_type = None
        self.matched_schema_files = set()
        self.main_schema_name = None

        self.logger.log_block("Validator Initialization", [
            f"Debug Mode: {self.debug}",
            f"FileHandler initialized: {'Yes (custom)' if file_handler else 'No (default instance)'}",
            f"Paths: {self.paths}"
        ], level="debug")

        self._verify_paths_and_files()

    def _verify_paths_and_files(self):
        self.logger.log_start("verify_paths_and_files")
        try:
            self.data_directory_path = self.paths["data_base_path"]

            # Validate schema folder (Fixed: Restore _verify_schema_folder)
            if self.config.use_schema:
                self.schema_directory_path = self.paths.get("schema_base_path")
                (
                    self.schema_directory_path,
                    self.schema_file_type,
                    self.matched_schema_files,
                ) = self._verify_schema_folder()
                self.main_schema_name = next(
                    (schema["name"] for schema in self.matched_schema_files if schema["type"] == "main"),
                    None
                )

            # Validate file folder (Optimized)
            (
                self.data_directory_path,
                self.data_file_type,
                self.all_data_files,
                self.matched_data_files,
            ) = self._verify_file_folder()

            self._log_validation_results()

            if not self.exit_notebook:
                self.logger.log_end("verify_paths_and_files", success=True)

        except Exception as e:
            self.logger.log_warning(f"Validation failed: {str(e)}")
            self._exit_notebook(f"Validation failed: {str(e)}")

    def _verify_file_folder(self) -> Tuple[str, Optional[str], List[str], List[str]]:
        data_directory_path = self.file_handler.normalize_path(self.config.source_data_folder_path)
        self.logger.log_debug(f"Checking data source folder: {data_directory_path}")

        if not self.file_handler.directory_exists(data_directory_path):
            self._exit_notebook(f"Source directory does not exist: {data_directory_path}")
            return data_directory_path, None, [], []

        try:
            all_files = {file.name for file in self.config.dbutils.fs.ls(data_directory_path)}
            matched_data_files = {file for file in all_files if file.startswith(self.config.source_filename.rstrip("*"))}

            if not matched_data_files:
                similar_files = self.file_handler.get_similar_files(list(all_files), self.config.source_filename)
                self.file_handler.log_available_files(similar_files, "Similar Files to Filter")
                self._exit_notebook(f"No matching files found: {self.config.source_filename}")
                return data_directory_path, None, list(all_files), []

            data_file_type = os.path.splitext(next(iter(matched_data_files)))[1].lstrip('.') if matched_data_files else None
            return data_directory_path, data_file_type, list(all_files), list(matched_data_files)

        except Exception as e:
            self._exit_notebook(f"Error accessing files in directory: {str(e)}")
            return data_directory_path, None, [], []

    def _verify_schema_folder(self) -> Tuple[str, Optional[str], List[dict]]:
        """
        Restored: Verify the schema folder and identify schema files based on file extensions.
        """
        schema_directory_path = self.schema_directory_path
        self.logger.log_debug(f"Checking schema source folder: {schema_directory_path}")

        if not self.file_handler.directory_exists(schema_directory_path):
            self._exit_notebook(f"Schema directory does not exist: {schema_directory_path}")
            return schema_directory_path, None, []

        matched_schema_files = [
            file.name for file in self.config.dbutils.fs.ls(schema_directory_path)
            if file.name.endswith(('.json', '.xsd'))
        ]

        if not matched_schema_files:
            self.logger.log_warning(f"No valid schema files found in directory: {schema_directory_path}")
            self._exit_notebook(f"No valid schema files found in directory: {schema_directory_path}")
            return schema_directory_path, None, []

        schema_file_type = "json" if any(f.endswith('.json') for f in matched_schema_files) else "xsd"
        labeled_schemas = [{"name": file_name, "type": "main" if file_name == f"{self.config.source_datasetidentifier}_schema.{schema_file_type}" else "referenced"} for file_name in matched_schema_files]

        return schema_directory_path, schema_file_type, labeled_schemas

    def _log_validation_results(self):
        self.logger.log_block("Path Validation Results", [
            f"Data directory path: {self.data_directory_path}",
            f"Schema directory path: {self.schema_directory_path}" if self.config.use_schema else "Schema validation skipped",
        ])

        if self.all_data_files:
            self.logger.log_block("File Validation Results", [
                f"Total files in source directory: {len(self.all_data_files)}",
                f"Files matching filter: {len(self.matched_data_files)}",
                f"Data File Type: {self.data_file_type}",
            ])
            self.file_handler.log_available_files(self.matched_data_files, title="Matched Files", max_display=10)
        else:
            self.logger.log_warning("No files found in the directory.")

    def check_and_exit(self):
        if self.exit_notebook:
            self.logger.log_warning(self.exit_notebook_message)
            self.logger.log_info(f"Notebook exit flagged with message: {self.exit_notebook_message}")

    def _exit_notebook(self, message: str):
        self.exit_notebook_message = message
        self.exit_notebook = True
        self.logger.log_warning(f"Notebook flagged for exit: {message}")

    def unpack(self, namespace: dict):
        namespace.update(vars(self))