import pytest
from pathlib import Path

from custom_utils import DataFrameTransformer, Config
#from ..test_utils import dbutils
from databricks.sdk.runtime import dbutils

class TestDataFrameTransformer:
    def setup_method(self):
        self.temp_path: Path = Path('/mnt')
        
        self.dbutils: dbutils = dbutils
        # self.widget_subdirs: list[str] = [
        #     "source_schema_folder_path",
        #     "source_data_folder_path",
        #     "destination_data_folder_path",
        # ]
        # self.widgets: dict[str, str] = {}
        # for subdir in self.widget_subdirs:
        #     dir_path: Path = self.temp_path/subdir
        #     dir_path.mkdir(parents=True, exist_ok=True)
        #     label: str = subdir.replace("_", " ")
        #     self.dbutils.widgets.text(name=subdir, default=dir_path, label=label)
            
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