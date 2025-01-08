import os
from typing import Tuple
from pyspark.sql import SparkSession
from custom_utils.logging.logger import Logger
from custom_utils.helper import get_param_value
from custom_utils.path_utils import generate_source_path, generate_schema_path

class Config:
    """
    Configuration class for initializing widgets, parameters, paths, and Spark session.
    Includes structured logging for INFO and DEBUG levels.
    """

    WIDGET_CONFIG = {
        "json": [
            {"name": "FeedbackColumn", "default": "EventTimestamp", "label": "Feedback Column"},
            {"name": "DepthLevel", "default": "1", "label": "Depth Level"},
            {"name": "SchemaFolderName", "default": "schemachecks", "label": "Schema Folder Name"},
        ],
        "xml": [
            {"name": "FeedbackColumn", "default": "timeseries_timestamp", "label": "Feedback Column"},
            {"name": "DepthLevel", "default": "1", "label": "Depth Level"},
            {"name": "SchemaFolderName", "default": "schemachecks", "label": "Schema Folder Name"},
            {"name": "XmlRootName", "default": "MyXmlRoot", "label": "XML Root Name"},
        ],
        "xlsx": [
            {"name": "SheetName", "default": "Sheet", "label": "Sheet Name"},
        ],
    }

    def __init__(self, dbutils=None, debug: bool = False):
        """
        Initialize the Config object with logging and debugging options.
        """
        self.dbutils = dbutils or globals().get("dbutils", None)
        self.debug = debug
        self.logger = Logger(debug=self.debug)

        try:
            self.logger.log_info("Starting Config Initialization")

            # Widgets Initialization Block
            self.logger.log_block("Initializing Widgets (DEBUG)", [
                "Initializing widgets..."
            ], level="debug")
            self._initialize_widgets()

            # Parameters Initialization Block
            self.logger.log_block("Initializing Parameters (DEBUG)", [
                "Initializing parameters..."
            ], level="debug")
            self._initialize_parameters()

            # Paths Initialization Block
            self.logger.log_block("Initializing Paths (DEBUG)", [
                "Initializing paths..."
            ], level="debug")
            self._initialize_paths()

            # Spark Session Initialization Block
            self.logger.log_block("Initializing Spark Session (DEBUG)", [
                "Initializing Spark session..."
            ], level="debug")
            self.spark = self._initialize_spark()

            # Schema Usage Block
            self.use_schema = bool(self.schema_folder_name)
            self.logger.log_block("Determining Schema Usage (DEBUG)", [
                f"Use Schema: {self.use_schema}"
            ], level="debug")

            # Configuration Validation Block
            self.logger.log_block("Validating Configuration (DEBUG)", [
                "Validating configuration..."
            ], level="debug")
            self._validate_config()

            # Logging Configuration Parameters Block
            self.logger.log_block("Logging All Configuration Parameters (DEBUG)", [
                "Logging all configuration parameters..."
            ], level="debug")
            self._log_params()

            self.logger.log_info("Finished Config Initialization successfully. Proceeding with notebook execution.")
        except Exception as e:
            self._handle_initialization_error(e)

    @staticmethod
    def initialize(dbutils=None, debug: bool = False) -> "Config":
        """Factory method to create and return a Config instance."""
        return Config(dbutils=dbutils, debug=debug)

    def _initialize_widgets(self):
        """Dynamically creates widgets based on the selected file type."""
        # Create the FileType dropdown widget first
        self.dbutils.widgets.dropdown("FileType", "json", ["json", "xlsx", "xml"], "File Type")
        self.dbutils.widgets.text("SourceStorageAccount", "", "Source Storage Account")
        self.file_type = get_param_value(self.dbutils, "FileType").lower()

        # Remove stale widgets after determining the current file type
        self._remove_widgets()

        # Add widgets based on the current file type
        widget_definitions = self.WIDGET_CONFIG.get(self.file_type, [])
        for widget in widget_definitions:
            self.dbutils.widgets.text(widget["name"], widget["default"], widget["label"])

        self.logger.log_info(f"Widgets initialized for file type: {self.file_type}")
        self.logger.log_debug(f"Widget Definitions: {widget_definitions}")

    def _remove_widgets(self):
        """Removes widgets that are not relevant to the current file type to avoid stale values."""
        self.logger.log_debug("Removing stale widgets...")

        # Get the current file type and the relevant widgets for it
        current_widget_names = {widget["name"] for widget in self.WIDGET_CONFIG.get(self.file_type, [])}

        for widget_group in self.WIDGET_CONFIG.values():
            for widget in widget_group:
                widget_name = widget["name"]
                if widget_name in current_widget_names:
                    # Skip widgets relevant to the current FileType
                    self.logger.log_debug(f"Widget '{widget_name}' is relevant for FileType '{self.file_type}'. Skipping removal.")
                    continue

                try:
                    # Attempt to remove unused widgets
                    self.dbutils.widgets.remove(widget_name)
                    self.logger.log_debug(f"Widget '{widget_name}' removed successfully.")
                except Exception as e:
                    # Catch and log exceptions for missing widgets
                    if "InputWidgetNotDefined" in str(e):
                        self.logger.log_debug(f"Widget '{widget_name}' not found. It might not have been defined yet.")
                    else:
                        self.logger.log_warning(f"Unexpected error while removing widget '{widget_name}': {str(e)}")

    def _initialize_parameters(self):
        """Initializes configuration parameters based on widgets."""
        self.logger.log_debug("Fetching required and optional parameters from widgets...")
        self.source_environment = get_param_value(self.dbutils, "SourceStorageAccount", required=True)
        self.destination_environment = get_param_value(self.dbutils, "DestinationStorageAccount", required=True)
        self.source_container = get_param_value(self.dbutils, "SourceContainer", required=True)
        self.source_datasetidentifier = get_param_value(self.dbutils, "SourceDatasetidentifier", required=True)
        self.source_filename = get_param_value(self.dbutils, "SourceFileName", default_value="*")
        self.key_columns = get_param_value(self.dbutils, "KeyColumns", required=True).replace(" ", "")

        # Optional parameters
        self.feedback_column = get_param_value(self.dbutils, "FeedbackColumn", required=False)
        raw_depth_level = get_param_value(self.dbutils, "DepthLevel", required=False)
        self.depth_level = int(raw_depth_level) if raw_depth_level and raw_depth_level.isdigit() else None
        self.schema_folder_name = get_param_value(self.dbutils, "SchemaFolderName", required=False) if self.file_type in ["xml", "json"] else None
        self.xml_root_name = get_param_value(self.dbutils, "XmlRootName", required=False) if self.file_type == "xml" else None
        self.sheet_name = get_param_value(self.dbutils, "SheetName", required=False) if self.file_type == "xlsx" else None

        self.logger.log_debug("Parameters initialized successfully.")

    def _initialize_paths(self):
        """Constructs paths for source, destination, and schema folders."""
        self.logger.log_debug("Generating source and destination paths...")
        self.source_data_folder_path, self.destination_data_folder_path = self._generate_data_paths()
        self.source_schema_folder_path = self._generate_schema_path()
        self.logger.log_debug("Paths initialized successfully.")

    def _initialize_spark(self) -> SparkSession:
        """Initializes the Spark session."""
        self.logger.log_debug("Initializing Spark session...")
        spark_session = SparkSession.builder.appName(f"Data Processing Pipeline: {self.source_datasetidentifier}").getOrCreate()
        self.logger.log_debug("Spark session initialized successfully.")
        return spark_session

    def _generate_data_paths(self) -> Tuple[str, str]:
        """Generates paths for source and destination data folders."""
        self.logger.log_debug("Generating paths for source and destination data folders...")
        source_path = generate_source_path(self.source_environment, self.source_datasetidentifier)
        destination_path = generate_source_path(self.destination_environment, self.source_datasetidentifier)
        self.logger.log_debug(f"Source Path: {source_path}")
        self.logger.log_debug(f"Destination Path: {destination_path}")
        return source_path, destination_path

    def _generate_schema_path(self) -> str:
        """Generates the schema folder path if applicable."""
        if self.schema_folder_name:
            schema_path = f"{generate_schema_path(self.source_environment, self.schema_folder_name, self.source_datasetidentifier)}".rstrip('/')
            self.logger.log_debug(f"Schema Path: {schema_path}")
            return schema_path
        self.logger.log_debug("No schema folder specified; skipping schema path generation.")
        return None

    def _validate_config(self):
        """Performs validation of configuration parameters."""
        self.logger.log_debug("Validating configuration parameters...")
        allowed_file_types = {"json", "xlsx", "xml"}
        if self.file_type not in allowed_file_types:
            raise ValueError(f"Invalid file type '{self.file_type}'. Allowed types: {allowed_file_types}")
        if self.depth_level is not None and self.depth_level < 0:
            raise ValueError("DepthLevel must be a non-negative integer.")
        self.logger.log_debug("Configuration validation completed successfully.")

    def _log_params(self):
        """Logs all configuration parameters in a structured format."""
        required_params = [
            f"File Type (file_type): {self.file_type}",
            f"Source Environment (source_environment): {self.source_environment}",
            f"Destination Environment (destination_environment): {self.destination_environment}",
            f"Source Container (source_container): {self.source_container}",
            f"Source Dataset Identifier (source_datasetidentifier): {self.source_datasetidentifier}",
            f"Source Filename (source_filename): {self.source_filename}",
            f"Key Columns (key_columns): {self.key_columns}",
        ]
        optional_params = [
            f"Depth Level (depth_level): {self.depth_level}" if self.depth_level is not None else None,            
            f"Feedback Column (feedback_column): {self.feedback_column}" if self.feedback_column else None,
            f"Schema Folder Name (schema_folder_name): {self.schema_folder_name}" if self.schema_folder_name else None,
            f"XML Root Name (xml_root_name): {self.xml_root_name}" if self.xml_root_name else None,
            f"Sheet Name (sheet_name): {self.sheet_name}" if self.sheet_name else None,
        ]
        extended_params = [
            f"Use Schema (use_schema): {self.use_schema}",
            f"Source Schema Folder Path (source_schema_folder_path): {self.source_schema_folder_path}" if self.source_schema_folder_path else None,
            f"Source Data Folder Path (source_data_folder_path): {self.source_data_folder_path}",
            f"Destination Folder Path (destination_data_folder_path): {self.destination_data_folder_path}",
        ]

        self.logger.log_block("Required Parameters", required_params)
        self.logger.log_block("Optional Parameters", [p for p in optional_params if p is not None])
        self.logger.log_block("Extended Parameters", [p for p in extended_params if p is not None])

    def _handle_initialization_error(self, e: Exception):
        """Handles errors during initialization by logging and raising an exception."""
        self.logger.log_error(f"Failed to initialize configuration: {str(e)}")
        raise RuntimeError(f"Failed to initialize configuration: {str(e)}")

    def unpack(self, namespace: dict):
        """Unpacks all configuration attributes into the provided namespace."""
        self.logger.log_debug("Unpacking configuration attributes into the namespace...")
        namespace.update(vars(self))
        self.logger.log_debug("Configuration attributes unpacked successfully.")