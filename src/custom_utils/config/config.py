import os
import inspect
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
        Initialize the Config class with basic parameters, set up logger, and Spark session.
        """
        # Logger initialization
        self.dbutils = dbutils or globals().get("dbutils", None)
        self.logger = logger or Logger(debug=debug)  # Use passed logger or fallback to custom Logger
        self.debug = debug

        # Log initialization start
        self._log("Starting Config Initialization", level='info')

        # Log the successful logger initialization
        self._log("Logger initialized successfully.", level='info')
        
        try:
            # Initialize required parameters, paths, and Spark session
            self._initialize_parameters()
            self._initialize_paths()
            self.spark = self._initialize_spark()

            # Set if schema will be used
            self.use_schema = bool(self.schema_folder_name)

            # Log successful initialization
            self._log_successful_initialization()
        except Exception as e:
            self._handle_initialization_error(e)

    @staticmethod
    def initialize(dbutils=None, logger=None, debug: bool = False) -> 'Config':
        """
        Static method to create and return a Config instance.
        """
        return Config(dbutils=dbutils, logger=logger, debug=debug)

    def _log(self, message: str, level: str = 'info'):
        """Logs messages at the desired log level."""
        if self.logger:
            if level == 'info':
                self.logger.log_info(message)
            elif level == 'warning':
                self.logger.log_warning(message)
            elif level == 'error':
                self.logger.log_error(message)

    def _initialize_parameters(self) -> None:
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
            self.sheet_name = get_param_value(self.dbutils, "SheetName", required=False)  # Optional sheet_name
            depth_level_str = get_param_value(self.dbutils, "DepthLevel", default_value="")
            self.depth_level = int(depth_level_str) if depth_level_str else None
            if self.depth_level is None:
                self._log("DepthLevel is not provided or empty. Setting it to None.", level='warning')
        except Exception as e:
            self._log(f"Error initializing parameters: {e}", level='error')
            raise

    def _generate_dbfs_path(self, base_path: str, dataset: str) -> str:
        """Helper method to generate DBFS path."""
        return f"/dbfs{base_path}/{dataset}"

    def _initialize_paths(self):
        """Construct and initialize paths for source, destination, and schema."""
        try:
            # Initialize source folder path (without '*')
            self.source_folder_path = f"/dbfs{generate_source_path(self.source_environment, self.source_datasetidentifier)}"
            
            # Initialize full source file path using the source filename, without double '*' appending
            self.full_source_file_path = generate_source_file_path(self.source_folder_path, self.source_filename)

            # Initialize destination folder path
            self.destination_folder_path = f"/dbfs{generate_source_path(self.destination_environment, self.source_datasetidentifier)}"

            # Initialize schema paths only if schema_folder_name is provided
            if self.schema_folder_name:
                self.source_schema_folder_path = f"/dbfs{generate_schema_path(self.source_environment, self.schema_folder_name, self.source_datasetidentifier)}".rstrip('/')  # Remove extra slash
                self.full_schema_file_path = f"{self.source_schema_folder_path}/{self.source_datasetidentifier}_schema.json"  # Add the schema file explicitly
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

    def _log_successful_initialization(self) -> None:
        """Logs the success of configuration initialization and details."""
        self._log("Spark session initialized successfully.", level='info')
        self._log_params()  # Use the updated _log_params method
        self._log("Finished Config Initialization successfully. Proceeding with notebook execution.", level='info')

    def _log_params(self):
        """Logs all the configuration parameters, dividing them into required, optional, and extended sections."""
        # Required parameters block
        required_params = [
            f"Source Environment (source_environment): {self.source_environment}",
            f"Destination Environment (destination_environment): {self.destination_environment}",
            f"Source Container (source_container): {self.source_container}",
            f"Source Dataset Identifier (source_datasetidentifier): {self.source_datasetidentifier}",
            f"Source Filename (source_filename): {self.source_filename}",
            f"Key Columns (key_columns): {self.key_columns}",
        ]
        
        # Optional parameters block
        optional_params = []
        if self.feedback_column:
            optional_params.append(f"Feedback Column (feedback_column): {self.feedback_column}")
        
        if self.schema_folder_name:
            optional_params.append(f"Schema Folder Name (schema_folder_name): {self.schema_folder_name}")
        
        if self.depth_level is not None:
            optional_params.append(f"Depth Level (depth_level): {self.depth_level}")
        
        # Add optional sheet_name if exists
        if hasattr(self, 'sheet_name'):
            optional_params.append(f"Sheet Name (sheet_name): {self.sheet_name}")

        # Extended parameters block (dynamically constructed paths, etc.)
        extended_params = [
            f"Source Folder Path (source_folder_path): {self.source_folder_path}",
            f"Full Source File Path (full_source_file_path): {self.full_source_file_path}",
            f"Destination Folder Path (destination_folder_path): {self.destination_folder_path}",
        ]
        
        if self.source_schema_folder_path:
            extended_params.append(f"Source Schema Folder Path (source_schema_folder_path): {self.source_schema_folder_path}")
        
        if self.full_schema_file_path:
            extended_params.append(f"Full Schema File Path (full_schema_file_path): {self.full_schema_file_path}")
        
        extended_params.append(f"Use Schema (use_schema): {self.use_schema}")

        # Log the blocks
        self.logger.log_block("Configuration Parameters", required_params)

        if optional_params:
            self.logger.log_block("Optional Parameters", optional_params)

        self.logger.log_block("Extended Parameters", extended_params)

    def _log_block(self, header: str, params: list) -> None:
        """Logs a block of messages with a header and parameter list."""
        self.logger.log_block(header, params)

    def _handle_initialization_error(self, e: Exception) -> None:
        """Handles initialization errors."""
        error_message = f"Failed to initialize configuration: {str(e)}"
        self._log(error_message, level='error')
        self.logger.log_end("Config Initialization", success=False, additional_message="Check error logs for details.")
        raise

    def _handle_spark_error(self, e: Exception) -> None:
        """Handles errors related to Spark initialization."""
        error_message = f"Failed to initialize Spark session: {str(e)}"
        self._log(error_message, level='error')
        self.logger.exit_notebook(error_message, self.dbutils)
        raise

    def unpack(self, namespace: dict) -> None:
        """Unpacks all configuration attributes into the provided namespace (e.g., globals())."""
        namespace.update(vars(self))