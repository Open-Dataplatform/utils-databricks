# File: custom_utils/config/config.py

import os
from pyspark.sql import SparkSession
from custom_utils.logging.logger import Logger
from custom_utils.helper import get_param_value
from custom_utils.path_utils import (
    generate_source_path,
    generate_source_file_path,
    generate_schema_path,
    generate_schema_file_path,
)

class Config:
    def __init__(self, dbutils=None, logger=None, debug=False):
        """
        Initialize the Config class with basic parameters and set up logger and Spark session.
        """
        self.dbutils = dbutils or globals().get("dbutils", None)
        self.logger = logger or Logger(debug=debug)
        self.debug = debug

        # Log the start of configuration
        self.logger.log_start("Config Initialization")

        try:
            # Core parameters (keeping it simple here)
            self.source_environment = get_param_value(self.dbutils, "SourceStorageAccount", required=True)
            self.destination_environment = get_param_value(self.dbutils, "DestinationStorageAccount", required=True)
            self.source_container = get_param_value(self.dbutils, "SourceContainer", required=True)
            self.source_datasetidentifier = get_param_value(self.dbutils, "SourceDatasetidentifier", required=True)
            self.source_filename = get_param_value(self.dbutils, "SourceFileName", "*")
            self.key_columns = get_param_value(self.dbutils, "KeyColumns", required=True).replace(" ", "")
            self.feedback_column = get_param_value(self.dbutils, "FeedbackColumn", required=True)
            self.schema_folder_name = get_param_value(self.dbutils, "SchemaFolderName", "schemachecks")

            # Construct paths (keeping the required ones)
            self.full_source_folder_path = generate_source_path(
                self.source_environment, self.source_container, self.source_datasetidentifier
            )
            self.full_source_file_path = generate_source_file_path(self.full_source_folder_path, self.source_filename)
            
            # Initialize Spark session
            self.spark = self._initialize_spark()

            # Log configuration parameters
            self._log_params()

            # Log the end of configuration
            self.logger.log_end("Config Initialization", success=True)

        except Exception as e:
            error_message = f"Failed to initialize configuration: {str(e)}"
            self.logger.log_error(error_message)
            self.logger.log_end("Config Initialization", success=False)
            raise

    def _log_params(self):
        """Logs all configuration parameters in a structured format using the logger."""
        params = [
            f"Source Environment: {self.source_environment}",
            f"Destination Environment: {self.destination_environment}",
            f"Source Container: {self.source_container}",
            f"Source Dataset Identifier: {self.source_datasetidentifier}",
            f"Source Filename: {self.source_filename}",
            f"Key Columns: {self.key_columns}",
            f"Feedback Column: {self.feedback_column}",
            f"Source Folder Path: {self.full_source_folder_path}",
        ]
        self.logger.log_block("Configuration Parameters", params)

    def _initialize_spark(self):
        """Initializes the Spark session."""
        try:
            spark = SparkSession.builder.appName(f"Data Processing Pipeline: {self.source_datasetidentifier}").getOrCreate()
            self.logger.log_message("Spark session initialized successfully.", level="info")
            return spark
        except Exception as e:
            error_message = f"Failed to initialize Spark session: {str(e)}"
            self.logger.log_error(error_message)
            self.logger.exit_notebook(error_message, self.dbutils)

    def unpack(self, namespace: dict):
        """Unpacks all configuration attributes into the provided namespace (e.g., globals())."""
        namespace.update(vars(self))