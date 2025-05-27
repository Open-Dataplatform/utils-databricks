import pytest
from pathlib import Path

from custom_utils import DataFrameTransformer, Config
from databricks.sdk.runtime import dbutils

class TestDataFrameTransformer:
    def setup_method(self):
        self.temp_path: Path = Path('/mnt')
        
        self.dbutils: dbutils = dbutils
        
        self.dbutils.widgets.dropdown("FileType", "json", ["json", "xml", "xlsx"])
        self.dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest")
        self.dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest")
        self.dbutils.widgets.text("SourceContainer", "landing")
        self.dbutils.widgets.text("SourceDatasetidentifier", "custom_utils_test_data")
        self.dbutils.widgets.text("SourceFileName", "custom_utils_test_data*")
        self.dbutils.widgets.text("KeyColumns", "A")
        self.dbutils.widgets.text("DepthLevel", "")
        self.dbutils.widgets.text("SchemaFolderName", "schemachecks")
            
        self.config = Config(dbutils=self.dbutils)
        self.config.unpack(globals())
        self.transformer: DataFrameTransformer = DataFrameTransformer(config=self.config)
        
    def teardown_method(self):
        del self.transformer
        
    def test_process_and_flatten_data(self):
        depth_level: int = 1
        print(self.dbutils.widgets.get("SourceDatasetidentifier"))
        print(self.dbutils.widgets.get("SourceContainer"))
        df_init, df_flat = self.transformer.process_and_flatten_data(depth_level=depth_level)
        print(df_init)
        print(df_flat)
        assert df_init == 1

if __name__ == "__main__":
    dbutils: dbutils = dbutils
    config: Config = Config(dbutils=dbutils)
    dbutils.widgets.dropdown("FileType", "json", ["json", "xml", "xlsx"], "File Type")
    dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
    dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")
    dbutils.widgets.text("SourceContainer", "landing", "Source Container")
    dbutils.widgets.text("SourceDatasetidentifier", "custom_utils_test_data", "Source Datasetidentifier")
    dbutils.widgets.text("SourceFileName", "custom_utils_test_data*", "Source File Name")
    dbutils.widgets.text("KeyColumns", "uuid", "Key Columns")
    dbutils.widgets.text("DepthLevel", "", "Depth Level (Leave blank for no limit)")
    dbutils.widgets.text("SchemaFolderName", "schemachecks", "Schema Folder Name")
    
    transformer: DataFrameTransformer = DataFrameTransformer(config=config)
    df_init, df_flat = transformer.process_and_flatten_data(depth_level=1)
    print(df_init)
    print(df_flat)