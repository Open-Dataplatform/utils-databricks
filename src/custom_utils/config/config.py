# File: custom_utils/config/config.py

import os
from pyspark.sql import SparkSession
from custom_utils.logging.logger import Logger
from custom_utils.helper import get_param_value, get_adf_parameter
from custom_utils.path_utils import (
    generate_source_path, 
    generate_source_file_path, 
    generate_schema_path, 
    generate_schema_file_path
)

class Config:
    """
    Configuration class to handle environment parameters, paths, and settings for data processing.
    """
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
        debug=False
    ):
        self.dbutils = dbutils
        self.logger = logger or Logger(debug=debug)
        self.debug = debug

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

    def print_params(self):
        """Logs all configuration parameters in a structured format."""
        self.logger.log_message("\n=== Configuration Parameters ===", level="info", single_info_prefix=True)
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


def initialize_config(dbutils=None, logger=None, depth_level=None, debug=False):
    """
    Initializes the Config class and returns the config object.
    """
    if dbutils is None:
        dbutils = globals().get("dbutils", None)  # Attempt to get dbutils from globals if not provided

    return Config(
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
        debug=debug
    )


def initialize_notebook(logger=None, debug=False):
    """
    Initializes the notebook, including configuration and Spark session setup.
    """
    try:
        # Initialize logger if not provided
        logger = logger or Logger(debug=debug)

        # Initialize dbutils if not in the function's parameters
        dbutils = globals().get("dbutils", None)

        print(dbutils)
        
        # Initialize configuration object
        config = initialize_config(dbutils=dbutils, logger=logger, debug=debug)
        config.unpack(globals())

        # Initialize the Spark session
        spark = SparkSession.builder.appName(f"Data Processing Pipeline: {config.source_datasetidentifier}").getOrCreate()

        if debug:
            config.print_params()

        return spark, config, logger
    except Exception as e:
        error_message = f"Failed to initialize notebook: {str(e)}"
        logger.log_message(error_message, level="error")
        logger.exit_notebook(error_message, dbutils)