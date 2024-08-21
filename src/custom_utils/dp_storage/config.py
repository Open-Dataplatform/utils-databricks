from custom_utils import helper
import os

class Config:
    def __init__(self, dbutils=None, helper=None, source_environment=None, destination_environment=None, source_container=None, source_datasetidentifier=None, 
                 source_filename='*', key_columns='', feedback_column='', schema_folder_name='schemachecks', depth_level=None):
        """
        Initializes and fetches configuration parameters.

        This class retrieves parameters either from Databricks widgets or uses default values, and handles any missing required parameters.
        """
        self.helper = helper
        self.dbutils = dbutils

        # Fetch core parameters using either widget values or defaults
        self.source_environment = self._get_param('SourceStorageAccount', source_environment, required=True)
        self.destination_environment = self._get_param('DestinationStorageAccount', destination_environment, required=True)
        self.source_container = self._get_param('SourceContainer', source_container, required=True)
        self.source_datasetidentifier = self._get_param('SourceDatasetidentifier', source_datasetidentifier, required=True)
        self.source_filename = self._get_param('SourceFileName', source_filename)
        self.key_columns = self._get_param('KeyColumns', key_columns, required=True).replace(' ', '')
        self.feedback_column = self._get_param('FeedbackColumn', feedback_column, required=True)
        self.schema_folder_name = self._get_param('SchemaFolderName', schema_folder_name, required=True)

        # Convert depth level to an integer if provided, otherwise leave as None
        depth_level_str = self._get_param('DepthLevel', depth_level)
        self.depth_level = int(depth_level_str) if depth_level_str else None

        # Derived paths based on provided/fetched parameters
        self.source_schema_filename = f"{self.source_datasetidentifier}_schema"
        self.source_folder_path = f"{self.source_container}/{self.source_datasetidentifier}"
        self.source_schema_folder_path = f"{self.source_container}/{self.schema_folder_name}/{self.source_datasetidentifier}"

    def _get_param(self, param_name: str, default_value=None, required: bool = False):
        """
        Fetches a parameter value, either from ADF (Azure Data Factory) or defaults.

        Args:
            param_name (str): The name of the parameter to fetch.
            default_value: The default value if the parameter is not found.
            required (bool): If True, raises an exception if the parameter is missing.

        Returns:
            The parameter value or the default value.
        """
        if self.dbutils:
            value = self.helper.get_adf_parameter(self.dbutils, param_name)
        else:
            # Fallback: Fetch from environment variables or use provided default
            value = os.getenv(param_name.upper(), default_value)
        
        if not value and required:
            raise ValueError(f"Required parameter '{param_name}' is missing.")
        return value or default_value

    def print_params(self):
        """Log all configuration parameters except helper and dbutils."""
        excluded_params = {"helper", "dbutils"}  # Parameters to exclude from printing
        self.helper.write_message("\nConfiguration Parameters:")
        self.helper.write_message("-" * 30)
        for param, value in vars(self).items():
            if param not in excluded_params:
                self.helper.write_message(f"{param}: {value}")
        self.helper.write_message("-" * 30)

    def unpack(self, namespace: dict):
        """Unpacks all configuration attributes into the provided namespace (e.g., globals())."""
        namespace.update(vars(self))

def get_dbutils():
    """
    Retrieve the dbutils object from the global scope.
    This function works in Databricks notebooks where dbutils is available by default.
    """
    try:
        return globals().get('dbutils', None)
    except KeyError:
        return None  # Safely return None if dbutils is not available

def initialize_config(depth_level=None):
    """
    Initializes the Config class and returns the config object.

    This function fetches parameters dynamically and handles default values.

    Args:
        depth_level (int, optional): The depth level for processing JSON structures.

    Returns:
        Config: An instance of the Config class with all parameters set.
    """
    dbutils = get_dbutils()

    # Initialize Config with fallback to environment variables if dbutils is not available
    return Config(
        dbutils=dbutils,
        helper=helper,
        source_environment=helper.get_adf_parameter(dbutils, 'SourceStorageAccount') if dbutils else os.getenv('SOURCESTORAGEACCOUNT'),
        destination_environment=helper.get_adf_parameter(dbutils, 'DestinationStorageAccount') if dbutils else os.getenv('DESTINATIONSTORAGEACCOUNT'),
        source_container=helper.get_adf_parameter(dbutils, 'SourceContainer') if dbutils else os.getenv('SOURCECONTAINER'),
        source_datasetidentifier=helper.get_adf_parameter(dbutils, 'SourceDatasetidentifier') if dbutils else os.getenv('SOURCEDATASETIDENTIFIER'),
        depth_level=depth_level
    )
