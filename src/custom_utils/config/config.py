import os
from pyspark.sql import SparkSession
from custom_utils.logging.logger import log_message, Logger
from custom_utils.helper import exit_notebook, get_param_value
from custom_utils.path_utils import generate_source_path, generate_source_file_path, generate_schema_path, generate_schema_file_path

class Config:
    """
    Configuration class to handle environment parameters, paths, and settings for data processing.
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
        debug=False
    ):
        self.helper = helper
        self.dbutils = dbutils
        self.debug = debug

        # Core parameters fetched from widgets, environment variables, or default values
        self.source_environment = get_param_value(dbutils, "SourceStorageAccount", source_environment, required=True)
        self.destination_environment = get_param_value(dbutils, "DestinationStorageAccount", destination_environment, required=True)
        self.source_container = get_param_value(dbutils, "SourceContainer", source_container, required=True)
        self.source_datasetidentifier = get_param_value(dbutils, "SourceDatasetidentifier", source_datasetidentifier, required=True)
        self.source_filename = get_param_value(dbutils, "SourceFileName", source_filename)
        self.key_columns = get_param_value(dbutils, "KeyColumns", key_columns, required=True).replace(" ", "")
        self.feedback_column = get_param_value(dbutils, "FeedbackColumn", feedback_column, required=True)
        self.schema_folder_name = get_param_value(dbutils, "SchemaFolderName", schema_folder_name)

        # Convert depth level to integer if provided
        depth_level_str = get_param_value(dbutils, "DepthLevel", depth_level)
        self.depth_level = int(depth_level_str) if depth_level_str else None

        # Construct paths using utility functions
        self.source_schema_filename = f"{self.source_datasetidentifier}_schema"
        self.full_source_folder_path = generate_source_path(self.source_environment, self.source_container, self.source_datasetidentifier)
        self.full_source_schema_folder_path = generate_schema_path(self.source_environment, self.source_container, self.schema_folder_name, self.source_datasetidentifier)
        self.full_source_file_path = generate_source_file_path(self.full_source_folder_path, self.source_filename)
        self.full_schema_file_path = generate_schema_file_path(self.full_source_schema_folder_path, self.source_schema_filename)

    def print_params(self):
        """Logs all configuration parameters in a structured format."""
        log_message("\n=== Configuration Parameters ===", level="info", single_info_prefix=True, debug=self.debug)
        print("------------------------------")

        params = [
            f"Source Environment: {self.source_environment}",
            f"Destination Environment: {self.destination_environment}",
            f"Source Container: {self.source_container}",
            f"Source Dataset Identifier: {self.source_datasetidentifier}",
            f"Source Filename: {self.source_filename}",
            f"Key Columns: {self.key_columns}",
            f"Feedback Column: {self.feedback_column}",
            f"Depth Level: {self.depth_level}",
            f"Source Folder Path: {self.full_source_folder_path}",
            f"Source Schema Folder Path: {self.full_source_schema_folder_path}",
        ]
        for param in params:
            print(param)

        print("------------------------------")

    def unpack(self, namespace: dict):
        """
        Unpacks all configuration attributes into the provided namespace (e.g., globals()).
        """
        namespace.update(vars(self))


def initialize_config(dbutils=None, helper=None, depth_level=None, debug=False):
    """
    Initializes the Config class and returns the config object.
    """
    if dbutils is None:
        dbutils = globals().get("dbutils", None)

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
        debug=debug
    )


def initialize_notebook(dbutils, helper, debug=False):
    """
    Initializes the notebook, including configuration and Spark session setup.
    """
    try:
        config = initialize_config(dbutils=dbutils, helper=helper, debug=debug)
        config.unpack(globals())

        spark = SparkSession.builder.appName(f"Data Processing Pipeline: {config.source_datasetidentifier}").getOrCreate()

        # Initialize the Logger
        logger = Logger()

        if debug:
            config.print_params()

        return spark, config, logger
    except Exception as e:
        error_message = f"Failed to initialize notebook: {str(e)}"
        log_message(error_message, level="error", debug=debug)
        exit_notebook(error_message, dbutils)