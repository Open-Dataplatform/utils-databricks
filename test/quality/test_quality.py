import pytest
from custom_utils import DataQualityManager, Logger
from pyspark.sql import DataFrame, SparkSession, functions
from ..transformations.utils import get_flat_df

class TestDataQualityManager:
    def setup_method(self):
        self.logger: Logger = Logger()
        self.quality_manager: DataQualityManager = DataQualityManager(logger=self.logger)
        self.spark: SparkSession = SparkSession.builder.getOrCreate()

    def teardown_method(self):
        del self.logger
        del self.quality_manager

    @pytest.mark.parametrize("key_columns", ["uuid, timestamp, area", ["uuid", "timestamp", "area"]])
    def test_parse_key_columns(self, key_columns: str|list[str]):
        return_values: list[str] = self.quality_manager.parse_key_columns(key_columns=key_columns)
        assert return_values == ["uuid", "timestamp", "area"]
        
        with pytest.raises(ValueError) as valueerror_info:
            self.quality_manager.parse_key_columns(None)
            
    def test_perform_data_quality(self):
        df: DataFrame = get_flat_df()
        df = df.withColumn("input_file_name", functions.lit("20230101.json"))
        cleaned_view: str = self.quality_manager.perform_data_quality_checks(
            spark=self.spark,
            df=df,
            key_columns=["data_A"],
            order_by="data_A",
            feedback_column="data_A",
            join_column="data_A",
            columns_to_exclude=["data_A"]
        )
        assert cleaned_view == "cleaned_data_view"