import os
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
    ):
        self.helper = helper
        self.dbutils = dbutils

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

    def _log_message(self, message, level="info"):
        """
        Logs a message using the helper, or falls back to print if helper is not available.
        
        Args:
            message (str): The message to log or print.
            level (str): The log level (e.g., info, warning, error). Defaults to "info".
        """
        if self.helper:
            self.helper.write_message(message, level)
        else:
            print(f"[{level.upper()}] {message}")

    def _get_param(self, param_name: str, default_value=None, required: bool = False):
        """
        Fetches a parameter value from Databricks widgets, environment variables, or default values.

        Args:
            param_name (str): The name of the parameter to fetch.
            default_value: The default value to return if no value is found.
            required (bool): Whether this parameter is required (raises an error if not found).

        Returns:
            The value of the parameter, or raises an error if required and not found.
        """
        value = None
        
        # Attempt to fetch from widgets if dbutils is available
        if self.dbutils:
            try:
                value = self.dbutils.widgets.get(param_name)
            except Exception as e:
                self._log_message(f"Could not retrieve widget '{param_name}': {str(e)}", level="warning")

        # Fallback to environment variables if the widget value is not found
        if not value:
            value = os.getenv(param_name.upper(), default_value)

        # Raise an error if the value is required and still not found
        if not value and required:
            raise ValueError(f"Required parameter '{param_name}' is missing.")

        return value

    def print_params(self):
        """Logs all configuration parameters except for sensitive ones like dbutils and helper."""
        excluded_params = {"helper", "dbutils"}
        self._log_message("\nConfiguration Parameters:", level="info")
        self._log_message("-" * 30, level="info")
        for param, value in vars(self).items():
            if param not in excluded_params:
                self._log_message(f"{param}: {value}", level="info")
        self._log_message("-" * 30, level="info")

    def unpack(self, namespace: dict):
        """
        Unpacks all configuration attributes into the provided namespace (e.g., globals()).

        Args:
            namespace (dict): The namespace (usually globals()) where the config attributes should be unpacked.
        """
        namespace.update(vars(self))

def get_dbutils():
    """
    Retrieve the dbutils object from the global scope, as it is usually available in Databricks notebooks.

    Returns:
        dbutils object if found, else None.
    """
    return globals().get("dbutils", None)

def initialize_config(dbutils=None, helper=None, depth_level=None):
    """
    Initializes the Config class and returns the config object.

    Args:
        dbutils (optional): The Databricks dbutils object, if available.
        helper (optional): The helper object used for logging and parameter fetching.
        depth_level (int, optional): The depth level for processing JSON structures.

    Returns:
        Config: An instance of the Config class with all parameters set.
    """
    if dbutils is None:
        dbutils = get_dbutils()

    # Initialize Config with fallback to environment variables if dbutils is not available
    return Config(
        dbutils=dbutils,
        helper=helper,
        source_environment=helper.get_adf_parameter(dbutils, "SourceStorageAccount"),
        destination_environment=helper.get_adf_parameter(dbutils, "DestinationStorageAccount"),
        source_container=helper.get_adf_parameter(dbutils, "SourceContainer"),
        source_datasetidentifier=helper.get_adf_parameter(dbutils, "SourceDatasetidentifier"),
        depth_level=depth_level,
    )

def initialize_notebook(dbutils, helper):
    """
    Initializes the notebook, including configuration and Spark session setup.

    Args:
        dbutils: The Databricks dbutils object, used for widgets and configurations.
        helper: The helper object for logging and parameter management.

    Returns:
        spark (SparkSession): The Spark session for processing.
        config (Config): The configuration object initialized with all parameters.
    """
    # Initialize configuration object
    config = initialize_config(dbutils=dbutils, helper=helper)

    # Unpack the configuration into the global namespace for easy access
    config.unpack(globals())

    # Initialize the Spark session with a custom name
    spark = SparkSession.builder.appName(f"Data Processing Pipeline: {config.source_datasetidentifier}").getOrCreate()

    # Print configuration for debugging and verification
    config.print_params()

    return spark, config