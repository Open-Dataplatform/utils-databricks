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
        self.dbutils = dbutils or globals().get("dbutils", None)
        self.logger = logger or Logger(debug=debug)
        self.debug = debug
        self.spark = None  # Spark session placeholder

        # Log the start of configuration
        self.logger.log_start("Config Initialization")

        try:
            # Core parameters fetched from widgets or environment variables
            self.source_environment = get_param_value(self.dbutils, "SourceStorageAccount", required=True)
            self.destination_environment = get_param_value(self.dbutils, "DestinationStorageAccount", required=True)
            self.source_container = get_param_value(self.dbutils, "SourceContainer", required=True)
            self.source_datasetidentifier = get_param_value(self.dbutils, "SourceDatasetidentifier", required=True)
            self.source_filename = get_param_value(self.dbutils, "SourceFileName", "*")
            self.key_columns = get_param_value(self.dbutils, "KeyColumns", required=True).replace(" ", "")
            self.feedback_column = get_param_value(self.dbutils, "FeedbackColumn", required=True)
            self.schema_folder_name = get_param_value(self.dbutils, "SchemaFolderName", "schemachecks")

            # Convert depth level to integer if provided
            depth_level_str = get_param_value(self.dbutils, "DepthLevel")
            self.depth_level = int(depth_level_str) if depth_level_str else None

            # Construct paths using utility functions
            self.source_schema_filename = f"{self.source_datasetidentifier}_schema"
            self.full_source_folder_path = generate_source_path(
                self.source_environment, self.source_container, self.source_datasetidentifier
            )
            self.full_source_schema_folder_path = generate_schema_path(
                self.source_environment, self.source_container, self.schema_folder_name, self.source_datasetidentifier
            )
            self.full_source_file_path = generate_source_file_path(self.full_source_folder_path, self.source_filename)
            self.full_schema_file_path = generate_schema_file_path(self.full_source_schema_folder_path, self.source_schema_filename)

            # Construct the destination folder path using the destination_environment
            self.full_destination_folder_path = generate_source_path(
                self.destination_environment, self.source_container, self.source_datasetidentifier
            )

            # Log all configuration parameters
            self._log_params()

            # Log the end of configuration
            self.logger.log_end("Config Initialization", success=True)

        except Exception as e:
            error_message = f"Failed to initialize configuration: {str(e)}"
            self.logger.log_message(error_message, level="error")
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
            f"Depth Level: {self.depth_level}",
            f"Source Schema Folder Path: {self.full_source_schema_folder_path}",
            f"Source Folder Path: {self.full_source_folder_path}",
            f"Destination Folder Path: {self.full_destination_folder_path}",
        ]
        self.logger.log_block("Configuration Parameters", params)

    def initialize_spark(self):
        """
        Initializes the Spark session if it's not already initialized.
        """
        if not self.spark:
            try:
                self.spark = SparkSession.builder.appName(f"Data Processing Pipeline: {self.source_datasetidentifier}").getOrCreate()
                self.logger.log_message("Spark session initialized successfully.", level="info")
            except Exception as e:
                error_message = f"Failed to initialize Spark session: {str(e)}"
                self.logger.log_message(error_message, level="error")
                self.logger.exit_notebook(error_message, self.dbutils)
                raise
        return self.spark

    def unpack(self, namespace: dict):
        """
        Unpacks all configuration attributes into the provided namespace (e.g., globals()).
        Args:
            namespace (dict): The namespace where attributes will be added (e.g., globals()).
        """
        for key, value in vars(self).items():
            if not key.startswith('_'):  # Avoid unpacking private attributes
                namespace[key] = value

    @classmethod
    def initialize(cls, dbutils=None, logger=None, debug=False):
        """
        Class method to initialize the Config object with optional dbutils, logger, and debug parameters.
        """
        return cls(dbutils=dbutils, logger=logger, debug=debug)