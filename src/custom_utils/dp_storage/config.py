from custom_utils import helper

class Config:
    def __init__(self, dbutils, helper, source_environment=None, destination_environment=None, source_container=None, source_datasetidentifier=None, 
                 source_filename='*', key_columns='', feedback_column='', schema_folder_name='schemachecks', depth_level=None):
        """
        Initializes and fetches configuration parameters.

        This class retrieves parameters either from Databricks widgets or uses default values, and handles any missing required parameters.

        Args:
            dbutils: Databricks utility object to interact with DBFS, widgets, secrets, etc.
            helper: Helper object used for logging and parameter fetching.
            source_environment (str): The source storage account/environment.
            destination_environment (str): The destination storage account/environment.
            source_container (str): The container where source files are stored.
            source_datasetidentifier (str): The dataset identifier (usually a folder or dataset name).
            source_filename (str, optional): The pattern or name of the source files. Defaults to '*'.
            key_columns (str, optional): Comma-separated key columns used for identifying records. Defaults to ''.
            feedback_column (str, optional): The column used for feedback or timestamp tracking. Defaults to ''.
            schema_folder_name (str, optional): The folder where schema files are stored. Defaults to 'schemachecks'.
            depth_level (int, optional): The depth level for processing JSON structures. Defaults to None.

        Derived Attributes:
            source_schema_filename (str): The name of the schema file based on the dataset identifier.
            source_folder_path (str): The full path to the source files based on the container and dataset identifier.
            source_schema_folder_path (str): The full path to the schema files.
        """
        self.helper = helper
        self.dbutils = dbutils

        # Fetch core parameters, using either widget values or defaults
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
        Fetches a parameter value, either from ADF or defaults.

        Args:
            param_name (str): The name of the parameter to fetch.
            default_value: The default value if the parameter is not found.
            required (bool): If True, raises an exception if the parameter is missing.

        Returns:
            The parameter value or the default value.
        """
        value = self.helper.get_adf_parameter(self.dbutils, param_name)
        if not value and required:
            raise ValueError(f"Required parameter '{param_name}' is missing.")
        return value or default_value

    def print_params(self):
        """Logger alle konfigurationsparametre undtagen helper og dbutils."""
        excluded_params = {"helper", "dbutils"}  # Parametre der ikke skal printes
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
        return globals()['dbutils']
    except KeyError:
        raise RuntimeError("dbutils is not available in the current environment.")

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

    # Fetch parameters dynamically using helper functions
    return Config(
        dbutils=dbutils,
        helper=helper,
        source_environment=helper.get_adf_parameter(dbutils, 'SourceStorageAccount'),
        destination_environment=helper.get_adf_parameter(dbutils, 'DestinationStorageAccount'),
        source_container=helper.get_adf_parameter(dbutils, 'SourceContainer'),
        source_datasetidentifier=helper.get_adf_parameter(dbutils, 'SourceDatasetidentifier'),
        depth_level=depth_level
    )