import pytest
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, functions
from uuid import uuid4
from custom_utils import DataStorageManager, DataQualityManager, Logger

from ..test_utils.dbutils_mocker import dbutils_mocker, dbutils
from ..transformations.utils import get_flat_df
class TestDataStorageManager:
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        self.storage_manager: DataStorageManager = DataStorageManager(logger=None)
        self.dbutils: dbutils_mocker = dbutils
        self.spark: SparkSession = SparkSession.builder.getOrCreate()

    def teardown_method(self, method: callable):
        print(f"Tearing down {method}")
        del self.storage_manager
        del self.spark
        del self.dbutils
    
    def test_ensure_path_exists(self):
        existing_filepath: Path = Path(str(__file__))
        existing_dirpath: Path = existing_filepath.parent
        unexisting_path: Path = existing_dirpath/str(uuid4())
        while unexisting_path.exists():
            unexisting_path: Path = existing_dirpath/str(uuid4())

        runtime_error_path = None # Used to provoke a runtime error.
        # Ensure file exists.
        assert None == self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                            destination_path=str(existing_filepath))
        # Ensure diretory exists.
        assert None == self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                           destination_path=str(existing_dirpath))
        
        # Ensure exception is thrown, when path does not exist.
        with pytest.raises(RuntimeError) as exc_info:
            self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                    destination_path=runtime_error_path)

    @pytest.mark.parametrize("key_columns, expected_returns", [('hello, there', ['hello', 'there']), (['hello', 'there'], ['hello', 'there'])])
    def test_normalize_key_columns(self, key_columns, expected_returns):
        return_list: list[str] = self.storage_manager.normalize_key_columns(key_columns)
        assert return_list == expected_returns
        
    def test_check_if_table_exists(self):
        database_name: str = "test"
        table_name: str = "unittest"
        
        
    def test_create_or_replace_tabe(self):
        database_name: str = "test"
        table_name: str = "unittest"
        df: DataFrame = get_flat_df()
        df = df.withColumn("input_file_name", functions.lit("20230101.json"))
        quality_manager: DataQualityManager = DataQualityManager(logger=Logger())
        cleaned_view: str = quality_manager.perform_data_quality_checks(
            spark=self.spark,
            df=df,
            key_columns=["data_A"],
            order_by="data_A",
            feedback_column="data_A",
            join_column="data_A",
            columns_to_exclude=["data_A"]
        )
        self.storage_manager.create_or_replace_table(self.spark, database_name=database_name, table_name=table_name, destination_path=".", cleaned_data_view=cleaned_view)
        table_count: int = self.storage_manager.check_if_table_exists(self.spark, database_name=database_name, table_name=table_name)
        assert table_count == 1