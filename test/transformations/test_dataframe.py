import pytest

from pathlib import Path
from custom_utils import DataFrameTransformer, Config
from databricks.sdk.runtime import dbutils
from shutil import rmtree
from pyspark.sql import DataFrame, Row, SparkSession
from typing import Any

from ..test_utils.data_generation import generate_files, get_schema
from ..test_utils.filesystem import fs

def get_df() -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    data: list[dict[str, Any]] = [
            {
                "A": 0.942,
                "B": 0.421,
                "C": 33,
                "D": 3.366,
                "E": "milk",
                "F": "str_6",
                "G": "2023-01-02T00:00:00",
                "H": {
                    "id": "ebc8d44d-f77a-4b95-a4e8-3781234823c1",
                    "value": 1
                }
            },
            {
                "A": -1.397,
                "B": 0.618,
                "C": 32,
                "D": 4.363,
                "E": "water",
                "F": "str_9",
                "G": "2023-01-03T00:00:00",
                "H": {
                    "id": "e6254f19-ba12-46d9-af54-788f195a6f50",
                    "value": 9
                }
            },
            {
                "A": -0.43,
                "B": 0.553,
                "C": 56,
                "D": 2.005,
                "E": "oil",
                "F": "str_0",
                "G": "2023-01-04T00:00:00",
                "H": {
                    "id": "ceeb8cea-0317-48d7-a6b5-d3c8aba0009c",
                    "value": 7
                }
            },
            {
                "A": 1.288,
                "B": 0.764,
                "C": 32,
                "D": 0.125,
                "E": "sugar",
                "F": "str_7",
                "G": "2023-01-02T00:00:00",
                "H": {
                    "id": "78a661c9-3518-4c07-a4d5-636e9bc3c400",
                    "value": 7
                }
            },
            {
                "A": 1.449,
                "B": 0.266,
                "C": 29,
                "D": 0.894,
                "E": "flour",
                "F": "str_5",
                "G": "2023-01-03T00:00:00",
                "H": {
                    "id": "070506a6-8a02-40e1-a1af-37f86cb90787",
                    "value": 3
                }
            },
            {
                "A": 0.203,
                "B": 0.553,
                "C": 44,
                "D": 0.262,
                "E": "egg",
                "F": "str_7",
                "G": "2023-01-04T00:00:00",
                "H": {
                    "id": "f34aed05-6ad6-4a8e-aca4-192fa1feb9dc",
                    "value": 4
                }
            }
        ]
    
    df: DataFrame = spark.createDataFrame(data)
    return df
class TestDataFrameTransformer:
    def setup_method(self):
        
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

        self.data_path : Path = Path(__file__).parent/self.dbutils.widgets.get("SourceDatasetidentifier")/self.dbutils.widgets.get("SourceStorageAccount") \
            /self.dbutils.widgets.get("SourceDatasetidentifier")

        self.dbutils.widgets.text("unittest_data_path", str(self.data_path.parent.parent))
        # Overwriting ls and head with local implementations, whos outputs are the same as dbutils, but customized to local environment.
        self.dbutils.fs.ls = fs().ls
        self.dbutils.fs.head = fs().head
        generate_files(self.data_path)
        self.config = Config(dbutils=self.dbutils)
        self.config.unpack(globals())
        self.transformer: DataFrameTransformer = DataFrameTransformer(config=self.config, debug=True)
        
    def teardown_method(self):
        del self.transformer
        #rmtree(self.data_path.parent.parent)
        
        
    def test_process_and_flatten_data(self):
        depth_level: int = 1
        df_init, df_flat = self.transformer.process_and_flatten_data(depth_level=depth_level)
        df_result: DataFrame = get_df()
        
        assert isinstance(df_init, DataFrame) and isinstance(df_flat, DataFrame)
        assert df_init.drop("input_file_name") == df_result

