class Config:
    def __init__(self, dbutils, helper, source_environment, destination_environment, source_container, source_datasetidentifier, source_filename, key_columns, feedback_column, schema_folder_name, depth_level):
        # Fetch parameters with default values where applicable
        self.source_environment = self._get_param(helper, dbutils, 'SourceStorageAccount', source_environment, required=True)
        self.destination_environment = self._get_param(helper, dbutils, 'DestinationStorageAccount', destination_environment, required=True)
        self.source_container = self._get_param(helper, dbutils, 'SourceContainer', source_container, required=True)
        self.source_datasetidentifier = self._get_param(helper, dbutils, 'SourceDatasetidentifier', source_datasetidentifier, required=True)
        self.source_filename = self._get_param(helper, dbutils, 'SourceFileName', source_filename)
        self.key_columns = self._get_param(helper, dbutils, 'KeyColumns', key_columns, required=True).replace(' ', '')
        self.feedback_column = self._get_param(helper, dbutils, 'FeedbackColumn', feedback_column, required=True)
        self.schema_folder_name = self._get_param(helper, dbutils, 'SchemaFolderName', schema_folder_name, required=True)
        
        # Handle depth level conversion to int if provided
        depth_level_str = self._get_param(helper, dbutils, 'DepthLevel', depth_level)
        self.depth_level = int(depth_level_str) if depth_level_str else None

        # Define derived parameters
        self.source_schema_filename = f"{self.source_datasetidentifier}_schema"
        self.source_folder_path = f"{self.source_container}/{self.source_datasetidentifier}"
        self.source_schema_folder_path = f"{self.source_container}/{self.schema_folder_name}/{self.source_datasetidentifier}"

    def _get_param(self, helper, dbutils, param_name, default_value=None, required=False):
        """Helper function to fetch parameters with optional default values and error handling."""
        value = helper.get_adf_parameter(dbutils, param_name)
        if not value and required:
            raise ValueError(f"Required parameter '{param_name}' is missing.")
        return value or default_value

    def print_params(self):
        """Print each parameter."""
        for param, value in vars(self).items():
            print(f"{param}: {value}")

def initialize_config(dbutils, helper, source_environment, destination_environment, source_container, source_datasetidentifier, source_filename='*', key_columns='', feedback_column='', schema_folder_name='schemachecks', depth_level=None):
    """
    Function to initialize the Config class with input parameters and return the config object.
    """
    config = Config(
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
    return config
