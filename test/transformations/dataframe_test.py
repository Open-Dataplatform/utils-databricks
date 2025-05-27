import pytest
from pathlib import Path

from custom_utils import DataFrameTransformer, Config
#from ..test_utils import dbutils
from databricks.sdk.runtime import dbutils

class TestDataFrameTransformer:
    def setup_method(self):
        self.temp_path: Path = Path('/mnt')
        
        self.dbutils: dbutils = dbutils
        self.dbutils.widgets.dropdown("FileType", "json", ["json", "xml", "xlsx"], "File Type")

        # Step 2: Define Source and Destination Storage Accounts
        self.dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
        self.dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")

        # Step 3: Configure Source Container and Dataset Identifier
        self.dbutils.widgets.text("SourceContainer", "landing", "Source Container")
        self.dbutils.widgets.text("SourceDatasetidentifier", "custom_utils_test_data", "Source Datasetidentifier")

        # Step 4: Specify Source File Name and Key Columns
        self.dbutils.widgets.text("SourceFileName", "custom_utils_test_data*", "Source File Name")
        self.dbutils.widgets.text("KeyColumns", "uuid", "Key Columns")

        # Step 5: Set Additional Parameters for Feedback Column, Depth Level, Schema Folder
        self.dbutils.widgets.text("DepthLevel", "", "Depth Level (Leave blank for no limit)")
        self.dbutils.widgets.text("SchemaFolderName", "schemachecks", "Schema Folder Name")
            
        self.config = Config(dbutils=self.dbutils)
        self.transformer: DataFrameTransformer = DataFrameTransformer(config=self.config)
        
    def teardown_method(self):
        del self.transformer
        
    def test_process_and_flatten_data(self):
        depth_level: int = 1
        df_init, df_flat = self.transformer.process_and_flatten_data(depth_level=depth_level)
        assert df_init == 1

if __name__ == "__main__":
    dbutils: dbutils = dbutils
    config: Config = Config(dbutils=dbutils)
    transformer: DataFrameTransformer = DataFrameTransformer(config=config)
    df_init, df_flat = transformer.process_and_flatten_data(depth_level=1)
    print(df_init)
    print(df_flat)