
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
            self.source_environment = get_param_value(self.dbutils, "SourceStorageAccount", required=True)
            self.destination_environment = get_param_value(self.dbutils, "DestinationStorageAccount", required=True)
            self.source_container = get_param_value(self.dbutils, "SourceContainer", required=True)
            self.source_datasetidentifier = get_param_value(self.dbutils, "SourceDatasetidentifier", required=True)
            self.source_filename = get_param_value(self.dbutils, "SourceFileName", default_value="*")
            self.key_columns = get_param_value(self.dbutils, "KeyColumns", required=True).replace(" ", "")
            
            # Feedback column is optional
            self.feedback_column = get_param_value(self.dbutils, "FeedbackColumn", required=False)
            if not self.feedback_column:
                self.logger.log_warning("FeedbackColumn is not provided. Setting it to None.")
            
            # Use default_value instead of default
            self.schema_folder_name = get_param_value(self.dbutils, "SchemaFolderName", default_value="schemachecks")
            
            # Depth level is optional
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
            self.full_source_folder_path = generate_source_path(
                self.source_environment, self.source_container, self.source_datasetidentifier
            )
            self.full_source_file_path = generate_source_file_path(self.full_source_folder_path, self.source_filename)
            
            self.full_source_schema_folder_path = generate_schema_path(
                self.source_environment, self.source_container, self.schema_folder_name, self.source_datasetidentifier
            )
            self.full_schema_file_path = generate_schema_file_path(
                self.full_source_schema_folder_path, self.source_datasetidentifier + "_schema"
            )

            self.full_destination_folder_path = generate_source_path(
                self.destination_environment, self.source_container, self.source_datasetidentifier
            )
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
        """Logs all the configuration parameters."""
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
            f"Destination Folder Path: {self.full_destination_folder_path}",
        ]
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