from pyspark.sql import SparkSession
from custom_utils.logging.logger import Logger
from custom_utils.helper import get_param_value, get_adf_parameter
from custom_utils.path_utils import (
    generate_source_path,
    generate_source_file_path,
    generate_schema_path,
    generate_schema_file_path,
)
from custom_utils import dbutils  # Import dbutils from the global import in custom_utils/__init__.py

class Config:
    def __init__(
        self,
        dbutils=None,
        logger=None,
        source_environment=None,
        destination_environment=None,
        source_container=None,
        source_datasetidentifier=None,
        source_filename="*",
        key_columns="",
        feedback_column="",
        schema_folder_name="schemachecks",
        depth_level=None,
        debug=False,
    ):
        self.dbutils = dbutils or globals().get("dbutils", None)
        self.logger = logger or Logger(debug=debug)
        self.debug = debug
        self.spark = None  # Initialize spark attribute to None

        # Log the start of configuration
        self.logger.log_start("Config Initialization")

        try:
            # Core parameters fetched from widgets, environment variables, or default values
            self.source_environment = get_param_value(self.dbutils, "SourceStorageAccount", source_environment, required=True)
            self.destination_environment = get_param_value(self.dbutils, "DestinationStorageAccount", destination_environment, required=True)
            self.source_container = get_param_value(self.dbutils, "SourceContainer", source_container, required=True)
            self.source_datasetidentifier = get_param_value(self.dbutils, "SourceDatasetidentifier", source_datasetidentifier, required=True)
            self.source_filename = get_param_value(self.dbutils, "SourceFileName", source_filename)
            self.key_columns = get_param_value(self.dbutils, "KeyColumns", key_columns, required=True).replace(" ", "")
            self.feedback_column = get_param_value(self.dbutils, "FeedbackColumn", feedback_column, required=True)
            self.schema_folder_name = get_param_value(self.dbutils, "SchemaFolderName", schema_folder_name)

            # Convert depth level to integer if provided
            depth_level_str = get_param_value(self.dbutils, "DepthLevel", depth_level)
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

            # New: Construct the destination folder path using the destination_environment
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

    @classmethod
    def initialize_config(cls, dbutils=None, logger=None, depth_level=None, debug=False):
        """
        Class method to initialize the Config object.
        """
        dbutils = dbutils or globals().get("dbutils", None)

        return cls(
            dbutils=dbutils,
            logger=logger,
            source_environment=get_adf_parameter(dbutils, "SourceStorageAccount"),
            destination_environment=get_adf_parameter(dbutils, "DestinationStorageAccount"),
            source_container=get_adf_parameter(dbutils, "SourceContainer"),
            source_datasetidentifier=get_adf_parameter(dbutils, "SourceDatasetidentifier"),
            source_filename=get_adf_parameter(dbutils, "SourceFileName"),
            key_columns=get_adf_parameter(dbutils, "KeyColumns"),
            feedback_column=get_adf_parameter(dbutils, "FeedbackColumn"),
            schema_folder_name=get_adf_parameter(dbutils, "SchemaFolderName"),
            depth_level=depth_level,
            debug=debug,
        )

    def initialize_spark(self):
        """Initialize and store the Spark session in the config instance."""
        if self.spark is None:
            self.spark = SparkSession.builder.appName(f"Data Processing Pipeline: {self.source_datasetidentifier}").getOrCreate()
            self.logger.log_message("Spark session initialized successfully.", level="info")

    def initialize_notebook(self):
        """Initialize the notebook, setting up Spark and any other configurations needed."""
        try:
            # Initialize the Spark session
            self.initialize_spark()
            self._log_params()  # Log the configuration parameters if needed
            self.logger.log_end("Config Initialization", success=True)
        except Exception as e:
            error_message = f"Failed to initialize notebook: {str(e)}"
            self.logger.log_message(error_message, level="error")
            self.logger.exit_notebook(error_message, self.dbutils)

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

    def unpack(self, namespace: dict):
        """Unpacks all configuration attributes into the provided namespace (e.g., globals())."""
        namespace.update(vars(self))