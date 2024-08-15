from custom_utils import helper

class Config:
    def __init__(self, dbutils, helper, source_environment, destination_environment, source_container, source_datasetidentifier, 
                 source_filename='*', key_columns='', feedback_column='', schema_folder_name='schemachecks', depth_level=None):
        """
        Initialize the Config class and fetch necessary parameters.
        
        This class fetches parameters either from widgets or default values and handles any missing required parameters.

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
        # Fetch parameters with default values where applicable
        self.source_environment = self._get_param(helper, dbutils, 'SourceStorageAccount', source_environment, required=True)
        self.destination_environment = self._get_param(helper, dbutils, 'DestinationStorageAccount', destination_environment, required=True)
        self.source_container = self._get_param(helper, dbutils, 'SourceContainer', source_container, required=True)
        self.source_datasetidentifier = self._get_param(helper, dbutils, 'SourceDatasetidentifier', source_datasetidentifier, required=True)
        self.source_filename = self._get_param(helper, dbutils, 'SourceFileName', source_filename)
        self.key_columns = self._get_param(helper, dbutils, 'KeyColumns', key_columns, required=True).replace(' ', '')  # Remove any extra spaces
        self.feedback_column = self._get_param(helper, dbutils, 'FeedbackColumn', feedback_column, required=True)
        self.schema_folder_name = self._get_param(helper, dbutils, 'SchemaFolderName', schema_folder_name, required=True)
        
        # Convert depth level to an integer if provided; otherwise, it can be left as None.
        depth_level_str = self._get_param(helper, dbutils, 'DepthLevel', depth_level)
        self.depth_level = int(depth_level_str) if depth_level_str else None

        # Derived parameters are constructed from the fetched parameters
        self.source_schema_filename = f"{self.source_datasetidentifier}_schema"
        self.source_folder_path = f"{self.source_container}/{self.source_datasetidentifier}"
        self.source_schema_folder_path = f"{self.source_container}/{self.schema_folder_name}/{self.source_datasetidentifier}"

    def _get_param(self, helper, dbutils, param_name: str, default_value=None, required: bool = False):
        """
        Helper function to fetch parameters with optional default values and error handling.
        
        If the parameter is not found and is required, an error is raised.
        Otherwise, it returns the parameter value or a provided default.

        Args:
            helper: Helper object used for logging and parameter fetching.
            dbutils: Databricks utility object to interact with DBFS, widgets, secrets, etc.
            param_name (str): The name of the parameter to fetch.
            default_value: The default value to use if the parameter is not found.
            required (bool): Whether the parameter is required. Defaults to False.

        Returns:
            The parameter value or the default value.
        """
        value = helper.get_adf_parameter(dbutils, param_name)
        if not value and required:
            raise ValueError(f"Required parameter '{param_name}' is missing.")
        return value or default_value

    def print_params(self):
        """
        Print all configuration parameters for easy debugging and verification.
        """
        print("\nConfiguration Parameters:")
        print("-" * 30)
        for param, value in vars(self).items():
            print(f"{param}: {value}")
        print("-" * 30)

def initialize_config(dbutils, helper, source_environment, destination_environment, source_container, source_datasetidentifier, 
                      source_filename='*', key_columns='', feedback_column='', schema_folder_name='schemachecks', depth_level=None):
    """
    Function to initialize the Config class with input parameters and return the config object.
    
    Parameters:
    - dbutils, helper: Required to fetch the parameters.
    - source_environment, destination_environment, source_container, source_datasetidentifier: Core settings.
    - source_filename, key_columns, feedback_column, schema_folder_name: Optional settings with default values.
    - depth_level: Can be left as None if not provided, defaults will be handled in the class.

    Returns:
        Config: An instance of the Config class with all parameters set.
    """
    return Config(
        dbutils=dbutils,
        helper=helper,
        source_environment=source_environment,
        destination_environment=destination_environment,
        source_container=source_container,
        source_datasetidentifier=source_datasetidentifier,
        source_filename=source_filename,
        key_columns=key_columns,
        feedback_column=feedback_column,
        schema_folder_name=schema_folder_name,
        depth_level=depth_level
    )
