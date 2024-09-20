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
        # Logger initialization
        self.dbutils = dbutils or globals().get("dbutils", None)
        self.logger = logger or Logger(debug=debug)
        self.debug = debug

        # Log the start of configuration as the first line
        self.logger.log_start("Config Initialization")

        # Collect environment initialization messages
        env_init_messages = []

        try:
            # Initialize parameters
            self._initialize_parameters()

            # Construct paths (including source, destination, and schema paths)
            self.full_source_folder_path = generate_source_path(
                self.source_environment, self.source_container, self.source_datasetidentifier
            )
            self.full_source_file_path = generate_source_file_path(self.full_source_folder_path, self.source_filename)
            self.full_source_schema_folder_path = generate_schema_path(
                self.source_environment, self.source_container, self.schema_folder_name, self.source_datasetidentifier
            )
            self.full_schema_file_path = generate_schema_file_path(self.full_source_schema_folder_path, self.source_datasetidentifier + "_schema")
            
            # Construct the destination folder path
            self.full_destination_folder_path = generate_source_path(
                self.destination_environment, self.source_container, self.source_datasetidentifier
            )

            # Initialize Spark session
            self.spark = self._initialize_spark()
            env_init_messages.append("Spark session initialized successfully.")

            # Log successful logger initialization
            env_init_messages.append("Logger initialized successfully.")

            # Log environment initialization block
            self.logger.log_block("Environment Initialization", env_init_messages)

            # Log configuration parameters
            self._log_params()

            # Log the end of configuration
            self.logger.log_end("Config Initialization", success=True, additional_message="Proceeding with notebook execution.")

        except Exception as e:
            error_message = f"Failed to initialize configuration: {str(e)}"
            self.logger.log_error(error_message)
            self.logger.log_end("Config Initialization", success=False, additional_message="Check error logs for details.")
            raise

    @staticmethod
    def initialize(dbutils=None, logger=None, debug=False):
        """
        Static method to create and return a Config instance.
        """
        return Config(dbutils=dbutils, logger=logger, debug=debug)

    def _initialize_parameters(self):
        """Initialize configuration parameters."""
        # Core parameters
        self.source_environment = get_param_value(self.dbutils, "SourceStorageAccount", required=True)
        self.destination_environment = get_param_value(self.dbutils, "DestinationStorageAccount", required=True)
        self.source_container = get_param_value(self.dbutils, "SourceContainer", required=True)
        self.source_datasetidentifier = get_param_value(self.dbutils, "SourceDatasetidentifier", required=True)
        self.source_filename = get_param_value(self.dbutils, "SourceFileName", "*")
        self.key_columns = get_param_value(self.dbutils, "KeyColumns", required=True).replace(" ", "")
        self.feedback_column = get_param_value(self.dbutils, "FeedbackColumn", required=True)
        self.schema_folder_name = get_param_value(self.dbutils, "SchemaFolderName", "schemachecks")
        
        # Depth level - convert to integer if provided
        depth_level_str = get_param_value(self.dbutils, "DepthLevel", "")
        self.depth_level = int(depth_level_str) if depth_level_str else None

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
            f"Schema Folder Path: {self.full_source_schema_folder_path}",
            f"Source Folder Path: {self.full_source_folder_path}",
            f"Destination Folder Path: {self.full_destination_folder_path}"
        ]
        self.logger.log_block("Configuration Parameters", params)

    def _initialize_spark(self):
        """Initializes the Spark session."""
        try:
            spark = SparkSession.builder.appName(f"Data Processing Pipeline: {self.source_datasetidentifier}").getOrCreate()
            return spark
        except Exception as e:
            error_message = f"Failed to initialize Spark session: {str(e)}"
            self.logger.log_error(error_message)
            self.logger.exit_notebook(error_message, self.dbutils)

    def unpack(self, namespace: dict):
        """Unpacks all configuration attributes into the provided namespace (e.g., globals())."""
        namespace.update(vars(self))