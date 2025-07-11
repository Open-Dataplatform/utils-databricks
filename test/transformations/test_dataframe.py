import pytest

from pathlib import Path
from custom_utils import DataFrameTransformer, Config
from databricks.sdk.runtime import dbutils
from shutil import rmtree
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    ArrayType, StructField, StringType, BooleanType, DoubleType, IntegerType, LongType,
    TimestampType, DecimalType, DateType, BinaryType, StructType, FloatType, DataType
)
from typing import Any, Generator
import json

from ..test_utils.data_generation import generate_files, get_schema
from ..test_utils.filesystem import fs
from ..test_utils.widgets import set_getAll

def equal_dataframes(df1: DataFrame, df2: DataFrame) -> bool:
    """Checks if two dataframes are equal

    Args:
        df1 (DataFrame): First dataframe.
        df2 (DataFrame): Second dataframe.

    Returns:
        bool: True if dataframes are equal, else False.
    """
    return (df1.schema == df2.schema) and (df1.orderBy(df1.A).collect() == df2.orderBy(df2.A).collect())

def count_json_rows(data: list|dict) -> int:
    """Counts flattened structure of json data

    Args:
        data (list | dict): list or dict containing json data.

    Returns:
        int: Number of rows in flattened structure.
    """
    count = 0
    if isinstance(data, dict):
        for val in data.values():
            if isinstance(val, dict):
                count += count_json_rows(val)
            elif isinstance(val, list):
                count += count_json_rows(val)
        if count == 0:
            count += 1
    elif isinstance(data, list):
        if len(data) == 0:
            count += 1
        elif isinstance(data[0], dict):
            for i in data:
                count += count_json_rows(i)
        else:
            count += len(data)
    return count

def get_init_df() -> DataFrame:
    """Gets static inital test data

    Returns:
        DataFrame: Static test data
    """
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
                "H": Row(id= "ebc8d44d-f77a-4b95-a4e8-3781234823c1", value= 1)
            },
            {
                "A": -1.397,
                "B": 0.618,
                "C": 32,
                "D": 4.363,
                "E": "water",
                "F": "str_9",
                "G": "2023-01-03T00:00:00",
                "H": Row(id= "e6254f19-ba12-46d9-af54-788f195a6f50", value= 9)
            },
            {
                "A": -0.43,
                "B": 0.553,
                "C": 56,
                "D": 2.005,
                "E": "oil",
                "F": "str_0",
                "G": "2023-01-04T00:00:00",
                "H": Row(id= "ceeb8cea-0317-48d7-a6b5-d3c8aba0009c", value= 7)
            },
            {
                "A": 1.288,
                "B": 0.764,
                "C": 32,
                "D": 0.125,
                "E": "sugar",
                "F": "str_7",
                "G": "2023-01-02T00:00:00",
                "H": Row(id= "78a661c9-3518-4c07-a4d5-636e9bc3c400", value= 7)
            },
            {
                "A": 1.449,
                "B": 0.266,
                "C": 29,
                "D": 0.894,
                "E": "flour",
                "F": "str_5",
                "G": "2023-01-03T00:00:00",
                "H": Row(id= "070506a6-8a02-40e1-a1af-37f86cb90787", value= 3)
            },
            {
                "A": 0.203,
                "B": 0.553,
                "C": 44,
                "D": 0.262,
                "E": "egg",
                "F": "str_7",
                "G": "2023-01-04T00:00:00",
                "H": Row(id= "f34aed05-6ad6-4a8e-aca4-192fa1feb9dc", value= 4)
            }
        ]
    schema: StructType = StructType([StructField('A', DoubleType(), True), 
                         StructField('B', DoubleType(), True), 
                         StructField('C', LongType(), True), 
                         StructField('D', DoubleType(), True), 
                         StructField('E', StringType(), True), 
                         StructField('F', StringType(), True), 
                         StructField('G', StringType(), True), 
                         StructField('H', StructType([StructField('id', StringType(), True), 
                                                      StructField('value', LongType(), True)]), True)])
    df: DataFrame = spark.createDataFrame(data, schema)
    return df
def get_flat_df() -> DataFrame:
    """Gets static flattened test data

    Returns:
        DataFrame: Static test data
    """
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
                "H_id": "ebc8d44d-f77a-4b95-a4e8-3781234823c1",
                "H_value": 1
            },
            {
                "A": -1.397,
                "B": 0.618,
                "C": 32,
                "D": 4.363,
                "E": "water",
                "F": "str_9",
                "G": "2023-01-03T00:00:00",
                "H_id": "e6254f19-ba12-46d9-af54-788f195a6f50",
                "H_value": 9
            },
            {
                "A": -0.43,
                "B": 0.553,
                "C": 56,
                "D": 2.005,
                "E": "oil",
                "F": "str_0",
                "G": "2023-01-04T00:00:00",
                "H_id": "ceeb8cea-0317-48d7-a6b5-d3c8aba0009c",
                "H_value": 7
            },
            {
                "A": 1.288,
                "B": 0.764,
                "C": 32,
                "D": 0.125,
                "E": "sugar",
                "F": "str_7",
                "G": "2023-01-02T00:00:00",
                "H_id": "78a661c9-3518-4c07-a4d5-636e9bc3c400",
                "H_value": 7
            },
            {
                "A": 1.449,
                "B": 0.266,
                "C": 29,
                "D": 0.894,
                "E": "flour",
                "F": "str_5",
                "G": "2023-01-03T00:00:00",
                "H_id": "070506a6-8a02-40e1-a1af-37f86cb90787",
                "H_value": 3
            },
            {
                "A": 0.203,
                "B": 0.553,
                "C": 44,
                "D": 0.262,
                "E": "egg",
                "F": "str_7",
                "G": "2023-01-04T00:00:00",
                "H_id": "f34aed05-6ad6-4a8e-aca4-192fa1feb9dc",
                "H_value": 4
            }
        ]
    schema: StructType = StructType([StructField('A', DoubleType(), True), 
                         StructField('B', DoubleType(), True), 
                         StructField('C', LongType(), True), 
                         StructField('D', DoubleType(), True), 
                         StructField('E', StringType(), True), 
                         StructField('F', StringType(), True), 
                         StructField('G', StringType(), True), 
                         StructField('H_id', StringType(), True), 
                         StructField('H_value', LongType(), True)])
    df: DataFrame = spark.createDataFrame(data, schema)
    df = df.withColumn("A",df.A.cast(DecimalType(38,18)))
    df = df.withColumn("B",df.B.cast(DecimalType(38,18)))
    df = df.withColumn("D",df.D.cast(DecimalType(38,18)))
    return df

class TestDataFrameTransformer:
    def setup_method(self):
        
        self.dbutils: dbutils = dbutils
        set_getAll(self.dbutils)
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
        rmtree(self.data_path.parent.parent)
        
        
    def test_process_and_flatten_data(self):
        depth_level: int = ''
        df_init, df_flat = self.transformer.process_and_flatten_data(depth_level=depth_level)
        df_init_static: DataFrame = get_init_df()
        df_flat_static: DataFrame = get_flat_df()
        
        assert isinstance(df_init, DataFrame) and isinstance(df_flat, DataFrame)
        df_init_test: DataFrame = df_init.drop("input_file_name")
        df_flat_test: DataFrame = df_flat.drop("input_file_name")
        assert equal_dataframes(df_init_test, df_init_static)
        assert equal_dataframes(df_flat_test, df_flat_static)
        assert self._test_input_file_name(df_init)
        assert self._test_input_file_name(df_flat)
    
    def _test_input_file_name(self, df: DataFrame) -> bool:
        assert "input_file_name" in df.columns
        paths: Generator[Path] = Path(self.data_path).glob('**/*.json')
        local_file_prefix: str = "file:///"
        for path in paths:
            with open(path, "rb") as f:
                data = json.load(f)

            json_count: int = count_json_rows(data)
            df_count: int = df.where(df.input_file_name == local_file_prefix+str(path).replace("\\", "/")).count()
            print(path)
            print(df.select("input_file_name").first())
            print("----------------------------")
            print(json_count, df_count)
            if json_count != df_count:
                return False
        return True
