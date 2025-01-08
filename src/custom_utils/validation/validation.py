import os
import logging
from typing import Tuple, Optional, List
from pyspark.sql.utils import AnalysisException
from custom_utils.logging.logger import Logger
from custom_utils.config.config import Config
from custom_utils.file_handler.file_handler import FileHandler

class Validator:
    """
    Validator class for verifying directories and files in DBFS,
    including schema and file type inference.
    """

    # Class-level attributes for notebook exit flags
    exit_notebook = False
    exit_notebook_message = ""

    def __init__(self, config: Config, debug: Optional[bool] = None, file_handler: Optional[FileHandler] = None):
        """
        Initialize the Validator with configuration, debug mode, and file handler.

        Args:
            config (Config): Configuration instance.
            debug (bool, optional): Debug flag to override Config.debug. Defaults to Config.debug.
            file_handler (FileHandler, optional): Custom FileHandler instance. Defaults to a new instance.
        """
        self.config = config

        # Determine debug mode (override Config.debug if provided)
        self.debug = debug if debug is not None else config.debug

        # Initialize logger and dynamically adjust its level
        self.logger = config.logger
        self.logger.debug = self.debug
        new_level = logging.DEBUG if self.debug else logging.INFO
        self.logger.logger.setLevel(new_level)

        # Initialize or override FileHandler
        self.file_handler = file_handler or FileHandler(config=config, debug=self.debug)

        # Initialize paths and variables
        self.paths = self.file_handler.manage_paths()
        self.schema_directory_path = None
        self.data_directory_path = None
        self.matched_data_files = []
        self.schema_file_type = None
        self.matched_schema_files = set()
        self.main_schema_name = None

        # Log initialization
        self.logger.log_block("Validator Initialization", [
            f"Debug Mode: {self.debug}",
            f"FileHandler initialized: {'Yes (custom)' if file_handler else 'No (default instance)'}",
            f"Paths: {self.paths}"
        ], level="debug")

        # Perform path and file validation
        self._verify_paths_and_files()

    def _verify_paths_and_files(self):
        """
        Validate schema and source paths, check for files, and log results.
        """
        self.logger.log_start("verify_paths_and_files")
        try:
            self.data_directory_path = self.paths["data_base_path"]

            # Validate schema folder
            if self.config.use_schema:
                self.logger.log_block("Validate schema folder", [
                ], level="debug")
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
            else:
                self.schema_directory_path = None
                self.schema_file_type = None
                self.matched_schema_files = set()

            # Validate file folder
            self.logger.log_block("Validate file folder", [
                ], level="debug")
            (
                self.data_directory_path,
                self.data_file_type,
                self.all_data_files,
                self.matched_data_files,
            ) = self._verify_file_folder()

            # Log validation results
            self._log_validation_results()

            # Log success only if no exit flag is set
            if not Validator.exit_notebook:
                self.logger.log_end("verify_paths_and_files", success=True)

        except Exception as e:
            # Log failure explicitly and trigger notebook exit
            self.logger.log_warning(f"Validation failed: {str(e)}")
            self._exit_notebook(f"Validation failed: {str(e)}")

        finally:
            if Validator.exit_notebook:
                self.logger.log_message("Notebook flagged for exit due to validation failure.", level="warning")

    def _verify_file_folder(self) -> Tuple[str, Optional[str], List[str], List[str]]:
        """
        Verify the source folder and identify data files based on the filter.
        """
        data_directory_path = self.file_handler.normalize_path(self.config.source_data_folder_path)
        self.logger.log_debug(f"Checking data source folder: {data_directory_path}")

        if not self.file_handler.directory_exists(data_directory_path):
            self._exit_notebook(f"Source directory does not exist: {data_directory_path}")
            return data_directory_path, None, [], []

        # Fetch all files
        all_files = [file.name for file in self.config.dbutils.fs.ls(data_directory_path)]
        matched_data_files = self.file_handler.filter_files(all_files)

        if not matched_data_files:
            self._exit_notebook(f"No matching files found based on the provided filter: {self.config.source_filename}")
            similar_files = self.file_handler.get_similar_files(all_files, self.config.source_filename)
            self.file_handler.log_available_files(similar_files, "Similar Files to Filter")
            return data_directory_path, None, all_files, []

        # Infer data file type
        data_file_type = os.path.splitext(matched_data_files[0])[1].lstrip('.') if matched_data_files else None

        return data_directory_path, data_file_type, all_files, matched_data_files

    def _verify_schema_folder(self) -> Tuple[str, Optional[str], List[dict]]:
        """
        Verify the schema folder and identify schema files based on file extensions.

        Returns:
            Tuple[str, Optional[str], List[dict]]:
                - Schema directory path,
                - Schema file type,
                - Matched schema files (with labels for 'main' or 'referenced').
        """
        schema_directory_path = self.schema_directory_path
        self.logger.log_debug(f"Checking schema source folder: {schema_directory_path}")

        if not self.file_handler.directory_exists(schema_directory_path):
            self._exit_notebook(f"Schema directory does not exist: {schema_directory_path}")
            return schema_directory_path, None, []

        # Get files matching schema extensions
        matched_schema_files = [
            file.name for file in self.config.dbutils.fs.ls(schema_directory_path)
            if file.name.endswith(('.json', '.xsd'))
        ]

        if not matched_schema_files:
            self.logger.log_warning(f"No valid schema files found in directory: {schema_directory_path}")
            self._exit_notebook(f"No valid schema files found in directory: {schema_directory_path}")
            return schema_directory_path, None, []

        # Determine schema file type
        schema_file_type = "json" if any(f.endswith('.json') for f in matched_schema_files) else "xsd"

        # Label schemas as 'main' or 'referenced'
        labeled_schemas = []
        expected_main_schema_name = f"{self.config.source_datasetidentifier}_schema.{schema_file_type}"

        for file_name in matched_schema_files:
            label = "main" if file_name == expected_main_schema_name else "referenced"
            labeled_schemas.append({"name": file_name, "type": label})

        return schema_directory_path, schema_file_type, labeled_schemas

    def _log_validation_results(self):
        """
        Log validation results for paths, files, and schemas.
        """
        # Log path validation results
        self.logger.log_block("Path Validation Results", [
            f"Data directory path (data_directory_path): {self.data_directory_path}",
            f"Schema directory path (schema_directory_path): {self.schema_directory_path}" if self.config.use_schema else "Schema validation skipped",
        ])

        # Log file validation results with a limit on the number of displayed files
        if self.matched_data_files:
            file_validation_content = [
                f"Total number of files in source directory: {len(self.all_data_files)}",
                f"Number of files matching filter: {len(self.matched_data_files)}",
                f"Data File Type (data_file_type): {self.data_file_type}",
                f"Files matching pattern (source_filename): '{self.config.source_filename}':"
            ]
            self.logger.log_block("File Validation Results", file_validation_content)
            self.file_handler.log_available_files(self.matched_data_files, title="Matched Files (matched_data_files)", max_display=10)
        else:
            self.logger.log_block("File Validation Results", ["No matching files found."])

        # Log schema validation results with a limit on the number of displayed schemas
        if self.config.use_schema and self.matched_schema_files:
            self.logger.log_block("Schema Validation Results", [
                f"Number of schemas found: {len(self.matched_schema_files)}",
                f"Schema File Type (schema_file_type): {self.schema_file_type}",
                f"Main Schema Name (main_schema_name): {self.main_schema_name}",
            ])
            self.file_handler.log_available_files([f"{schema['name']} ({schema['type']})" for schema in self.matched_schema_files],title="Matched Schemas (matched_schema_files)",max_display=10)
        elif self.config.use_schema:
            self.logger.log_block("Schema Validation Results", ["No matching schemas found."])

    def check_and_exit(self):
        """
        Check for exit flag and ensure exit message is logged.
        """
        if Validator.exit_notebook:
            self.logger.log_warning(Validator.exit_notebook_message)
            # Explicitly print the message to ensure visibility in notebook output
            self.logger.log_info(f"Notebook exit flagged with message: {Validator.exit_notebook_message}")

    def _exit_notebook(self, message: str):
        """
        Set the notebook to exit with a given message.
        """
        Validator.exit_notebook = True
        Validator.exit_notebook_message = message
        self.logger.log_warning(f"Notebook flagged for exit: {message}")

    def unpack(self, namespace: dict):
        """
        Unpacks all configuration attributes into the provided namespace.
        """
        namespace.update(vars(self))