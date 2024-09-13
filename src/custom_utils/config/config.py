import os
import datetime
from pyspark.sql import SparkSession


class Config:
    """
    Configuration class to handle environment parameters, paths, and settings for data processing.

    Args:
        dbutils (object, optional): The Databricks dbutils object, if available, for reading widgets.
        helper (object, optional): A helper object for logging and message printing.
        source_environment (str, optional): Source storage account/environment.
        destination_environment (str, optional): Destination storage account/environment.
        source_container (str, optional): Container where the source data resides.
        source_datasetidentifier (str, optional): Dataset identifier for source data.
        source_filename (str, optional): Filename pattern for the source data.
        key_columns (str, optional): Columns used for unique key validation.
        feedback_column (str, optional): Column to store feedback during data validation.
        schema_folder_name (str, optional): Name of the folder that stores schema files.
        depth_level (int, optional): The depth level for processing hierarchical structures.
        debug (bool, optional): Whether to print info messages. Default is False.
    """
    
    def __init__(
        self,
        dbutils=None,
        helper=None,
        source_environment=None,
        destination_environment=None,
        source_container=None,
        source_datasetidentifier=None,
        source_filename="*",
        key_columns="",
        feedback_column="",
        schema_folder_name="schemachecks",
        depth_level=None,
        debug=False  # Optional debug flag (default is False)
    ):
        self.helper = helper
        self.dbutils = dbutils
        self.debug = debug

        # Core parameters fetched from widgets, environment variables, or default values
        self.source_environment = self._get_param("SourceStorageAccount", source_environment, required=True)
        self.destination_environment = self._get_param("DestinationStorageAccount", destination_environment, required=True)
        self.source_container = self._get_param("SourceContainer", source_container, required=True)
        self.source_datasetidentifier = self._get_param("SourceDatasetidentifier", source_datasetidentifier, required=True)
        self.source_filename = self._get_param("SourceFileName", source_filename)
        self.key_columns = self._get_param("KeyColumns", key_columns, required=True).replace(" ", "")
        self.feedback_column = self._get_param("FeedbackColumn", feedback_column, required=True)
        self.schema_folder_name = self._get_param("SchemaFolderName", schema_folder_name)

        # Convert depth level to integer if provided
        depth_level_str = self._get_param("DepthLevel", depth_level)
        self.depth_level = int(depth_level_str) if depth_level_str else None

        # Construct paths based on provided parameters
        self.source_schema_filename = f"{self.source_datasetidentifier}_schema"
        self.source_folder_path = f"{self.source_container}/{self.source_datasetidentifier}"
        self.source_schema_folder_path = f"{self.source_container}/{self.schema_folder_name}/{self.source_datasetidentifier}"
        
        # Generate full file paths
        self.full_source_file_path = self.generate_source_file_path()
        self.full_schema_file_path = self.generate_schema_file_path()

    def generate_source_file_path(self):
        """Generate full path for source files."""
        return f"/mnt/{self.source_environment}/{self.source_folder_path}/{self.source_filename}"

    def generate_schema_file_path(self):
        """Generate full path for schema files."""
        return f"/mnt/{self.source_environment}/{self.source_schema_folder_path}/{self.source_schema_filename}.json"

    def _log_message(self, message, level="info", single_info_prefix=False):
        """
        Logs a message using the helper, or falls back to print if helper is not available.
        If single_info_prefix is True, it prints '[INFO]' only once before the log block.

        Args:
            message (str): The message to log or print.
            level (str): The log level (e.g., info, warning, error). Defaults to "info".
            single_info_prefix (bool): Whether to print `[INFO]` prefix just once.
        """
        # If level is 'info' and debug is False, skip the message
        if level == "info" and not self.debug:
            return

        if single_info_prefix and level == "info":
            print("[INFO]")
            print(message)
        elif self.helper:
            # Log message with the helper if available
            self.helper.write_message(message, level)
        else:
            print(f"[{level.upper()}] {message}")

    def _get_param(self, param_name: str, default_value=None, required: bool = False):
        """
        Fetches a parameter value from Databricks widgets, environment variables, or default values.
        Raises an error if the parameter is required but missing.

        Args:
            param_name (str): The name of the parameter.
            default_value: The default value if the parameter is not found.
            required (bool): Whether this parameter is mandatory.

        Returns:
            str: The parameter value.

        Raises:
            ValueError: If a required parameter is missing.
        """
        value = None
        try:
            if self.dbutils:
                value = self.dbutils.widgets.get(param_name)
        except Exception as e:
            self._log_message(f"Could not retrieve widget '{param_name}': {str(e)}", level="warning")

        if not value:
            value = os.getenv(param_name.upper(), default_value)

        if not value and required:
            error_message = f"Required parameter '{param_name}' is missing."
            self._log_message(error_message, level="error")
            self._exit_notebook(error_message)
            raise ValueError(error_message)

        return value

    def _exit_notebook(self, message):
        """Exit the notebook with an error message."""
        if self.dbutils:
            self.dbutils.notebook.exit(f"[ERROR] {message}")
        else:
            raise SystemExit(f"[ERROR] {message}")

    def print_params(self):
        """Logs all configuration parameters in a structured format."""
        excluded_params = {"helper", "dbutils"}
        
        # Print the header info once
        self._log_message("\n=== Configuration Parameters ===", level="info", single_info_prefix=True)
        print("------------------------------")  # Manually print the separator for clean output

        # Log each parameter without `[INFO]` prefix for cleaner output
        params = [
            f"Source Environment: {self.source_environment}",
            f"Destination Environment: {self.destination_environment}",
            f"Source Container: {self.source_container}",
            f"Source Dataset Identifier: {self.source_datasetidentifier}",
            f"Source Filename: {self.source_filename}",
            f"Key Columns: {self.key_columns}",
            f"Feedback Column: {self.feedback_column}",
            f"Schema Folder Name: {self.schema_folder_name}",
            f"Depth Level: {self.depth_level}",
            f"Full Source File Path: {self.full_source_file_path}",
            f"Full Schema File Path: {self.full_schema_file_path}",
        ]
        for param in params:
            print(param)  # Avoid [INFO] prefixes here for a cleaner format

        # Closing info line
        print("------------------------------")

    def unpack(self, namespace: dict):
        """
        Unpacks all configuration attributes into the provided namespace (e.g., globals()).

        Args:
            namespace (dict): The namespace (usually globals()) where the config attributes should be unpacked.
        """
        namespace.update(vars(self))


def initialize_config(dbutils=None, helper=None, depth_level=None, debug=False):
    """
    Initializes the Config class and returns the config object.

    Args:
        dbutils (optional): The Databricks dbutils object, if available.
        helper (optional): The helper object used for logging and parameter fetching.
        depth_level (int, optional): The depth level for processing JSON structures.
        debug (bool, optional): Whether to print info messages. Default is False.

    Returns:
        Config: An instance of the Config class with all parameters set.
    """
    if dbutils is None:
        dbutils = globals().get("dbutils", None)

    # Initialize Config with fallback to environment variables if dbutils is not available
    return Config(
        dbutils=dbutils,
        helper=helper,
        source_environment=helper.get_adf_parameter(dbutils, "SourceStorageAccount"),
        destination_environment=helper.get_adf_parameter(dbutils, "DestinationStorageAccount"),
        source_container=helper.get_adf_parameter(dbutils, "SourceContainer"),
        source_datasetidentifier=helper.get_adf_parameter(dbutils, "SourceDatasetidentifier"),
        source_filename=helper.get_adf_parameter(dbutils, "SourceFileName"),
        key_columns=helper.get_adf_parameter(dbutils, "KeyColumns"),
        feedback_column=helper.get_adf_parameter(dbutils, "FeedbackColumn"),
        schema_folder_name=helper.get_adf_parameter(dbutils, "SchemaFolderName"),
        depth_level=depth_level,
        debug=debug  # Pass the debug flag
    )


def initialize_notebook(dbutils, helper, debug=False):
    """
    Initializes the notebook, including configuration and Spark session setup.

    Args:
        dbutils: The Databricks dbutils object, used for widgets and configurations.
        helper: The helper object for logging and parameter management.
        debug (bool, optional): Whether to print info messages. Default is False.

    Returns:
        spark (SparkSession): The Spark session for processing.
        config (Config): The configuration object initialized with all parameters.
    """
    try:
        # Initialize configuration object
        config = initialize_config(dbutils=dbutils, helper=helper, debug=debug)

        # Unpack the configuration into the global namespace for easy access
        config.unpack(globals())

        # Initialize the Spark session with a custom name
        spark = SparkSession.builder.appName(f"Data Processing Pipeline: {config.source_datasetidentifier}").getOrCreate()

        # Print configuration for debugging and verification
        if debug:
            config.print_params()

        return spark, config
    except Exception as e:
        error_message = f"Failed to initialize notebook: {str(e)}"
        helper.write_message(error_message, level="error")
        if dbutils:
            dbutils.notebook.exit(f"[ERROR] {error_message}")
        else:
            raise SystemExit(f"[ERROR] {error_message}")