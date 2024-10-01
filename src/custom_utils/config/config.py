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
    def __init__(self, dbutils=None, logger=None, debug: bool = False):
        """
        Initialize the Config class with basic parameters and set up logger and Spark session.
        """
        # Logger initialization
        self.dbutils = dbutils or globals().get("dbutils", None)
        self.logger = logger or Logger(debug=debug)  # Use passed logger or fallback to custom Logger
        self.debug = debug

        self.logger.log_start("Config Initialization")

        try:
            # Initialize parameters
            self._initialize_parameters()

            # Initialize paths
            self._initialize_paths()

            # Initialize Spark session
            self.spark = self._initialize_spark()

            # Determine if schema will be used based on whether schema_folder_name is provided
            self.use_schema = bool(self.schema_folder_name)

            # Log success and the initialized parameters
            self._log_successful_initialization()

        except Exception as e:
            self._handle_initialization_error(e)

    @staticmethod
    def initialize(dbutils=None, logger=None, debug: bool = False) -> 'Config':
        """
        Static method to create and return a Config instance.
        """
        return Config(dbutils=dbutils, logger=logger, debug=debug)

    def _initialize_parameters(self):
        """Initialize all the configuration parameters required."""
        try:
            # Required parameters
            self.source_environment = get_param_value(self.dbutils, "SourceStorageAccount", required=True)
            self.destination_environment = get_param_value(self.dbutils, "DestinationStorageAccount", required=True)
            self.source_container = get_param_value(self.dbutils, "SourceContainer", required=True)
            self.source_datasetidentifier = get_param_value(self.dbutils, "SourceDatasetidentifier", required=True)
            self.source_filename = get_param_value(self.dbutils, "SourceFileName", default_value="*")
            self.key_columns = get_param_value(self.dbutils, "KeyColumns", required=True).replace(" ", "")

            # Optional parameters
            self.feedback_column = get_param_value(self.dbutils, "FeedbackColumn", required=False)
            self.schema_folder_name = get_param_value(self.dbutils, "SchemaFolderName", required=False)

            # Depth level is optional, but we log a warning if not provided
            depth_level_str = get_param_value(self.dbutils, "DepthLevel", default_value="")
            self.depth_level = int(depth_level_str) if depth_level_str else None
            if self.depth_level is None:
                self.logger.log_warning("DepthLevel is not provided or empty. Setting it to None.")
                
        except Exception as e:
            self.logger.log_error(f"Error initializing parameters: {e}")
            raise

    def _initialize_paths(self):
        """Construct and initialize paths for source, destination, and schema."""
        try:
            # Initialize source and destination paths and ensure they start with '/dbfs'
            self.source_folder_path = f"/dbfs{generate_source_path(self.source_environment, self.source_datasetidentifier)}"
            self.full_source_file_path = generate_source_file_path(self.source_folder_path, self.source_filename)

            self.destination_folder_path = f"/dbfs{generate_source_path(self.destination_environment, self.source_datasetidentifier)}"

            # Initialize schema paths only if schema_folder_name is provided
            if self.schema_folder_name:
                self.source_schema_folder_path = f"/dbfs{generate_schema_path(self.source_environment, self.schema_folder_name, self.source_datasetidentifier)}"
                self.full_schema_file_path = generate_schema_file_path(
                    self.source_schema_folder_path, self.source_datasetidentifier + "_schema"
                )
            else:
                self.source_schema_folder_path = None
                self.full_schema_file_path = None

        except Exception as e:
            self.logger.log_error(f"Error initializing paths: {e}")
            raise

    def _initialize_spark(self) -> SparkSession:
        """Initializes and returns the Spark session."""
        try:
            return SparkSession.builder.appName(f"Data Processing Pipeline: {self.source_datasetidentifier}").getOrCreate()
        except Exception as e:
            self._handle_spark_error(e)

    def _log_successful_initialization(self):
        """Logs the success of configuration initialization and details."""
        self.logger.log_info("Spark session initialized successfully.")
        self._log_params()
        self.logger.log_end("Config Initialization", success=True, additional_message="Proceeding with notebook execution.")

    def _log_params(self):
        """Logs all the configuration parameters, including both friendly name and variable name."""
        params = [
            f"Source Environment (source_environment): {self.source_environment}",
            f"Destination Environment (destination_environment): {self.destination_environment}",
            f"Source Container (source_container): {self.source_container}",
            f"Source Dataset Identifier (source_datasetidentifier): {self.source_datasetidentifier}",
            f"Source Filename (source_filename): {self.source_filename}",
            f"Key Columns (key_columns): {self.key_columns}",
        ]

        # Only add the feedback column if it is provided
        if self.feedback_column:
            params.append(f"Feedback Column (feedback_column): {self.feedback_column}")

        # Only add schema folder path if it was initialized
        if self.source_schema_folder_path:
            params.append(f"Schema Folder Path (source_schema_folder_path): {self.source_schema_folder_path}")

        # Add DepthLevel if not None
        if self.depth_level is not None:
            params.append(f"Depth Level (depth_level): {self.depth_level}")

        params.extend([
            f"Source Folder Path (source_folder_path): {self.source_folder_path}",
            f"Destination Folder Path (destination_folder_path): {self.destination_folder_path}",
            f"Use Schema (use_schema): {self.use_schema}",
        ])

        self.logger.log_block("Configuration Parameters", params)

    def _handle_initialization_error(self, e: Exception):
        """Handles initialization errors."""
        error_message = f"Failed to initialize configuration: {str(e)}"
        self.logger.log_error(error_message)
        self.logger.log_end("Config Initialization", success=False, additional_message="Check error logs for details.")
        raise

    def _handle_spark_error(self, e: Exception):
        """Handles errors related to Spark initialization."""
        error_message = f"Failed to initialize Spark session: {str(e)}"
        self.logger.log_error(error_message)
        self.logger.exit_notebook(error_message, self.dbutils)
        raise

    def unpack(self, namespace: dict):
        """Unpacks all configuration attributes into the provided namespace (e.g., globals())."""
        namespace.update(vars(self))