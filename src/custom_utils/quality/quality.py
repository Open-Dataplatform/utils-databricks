# File: custom_utils/quality/quality.py

import json
from typing import List, Dict, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StructType, StringType
from pyspark.sql.utils import AnalysisException
from custom_utils.logging.logger import Logger

class Quality:
    def __init__(self, logger, debug=False):
        self.logger = logger
        self.debug = debug

    def _raise_error(self, message: str):
        self.logger.log_message(message, level="error")
        if self.debug:
            self.logger.log_message(f"Debug mode: Aborting with error - {message}", level="error")
        raise RuntimeError(message)

    def check_for_duplicates(self, df: DataFrame, key_columns: List[str]):
        self.logger.log_message(f"Checking for duplicates based on columns: {key_columns}", level="info")
        duplicate_count = df.groupBy(key_columns).count().filter('count > 1').count()
        if duplicate_count > 0:
            self._raise_error(f"Data Quality Check Failed: Found {duplicate_count} duplicates based on columns {key_columns}.")
        else:
            self.logger.log_message("Data Quality Check Passed: No duplicates found.", level="info")

    def check_for_nulls(self, df: DataFrame, critical_columns: List[str]):
        self.logger.log_message(f"Checking for null values in columns: {critical_columns}", level="info")
        for col in critical_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                self._raise_error(f"Data Quality Check Failed: Column '{col}' has {null_count} missing values.")
            else:
                self.logger.log_message(f"Column '{col}' has no missing values.", level="info")

    def check_value_ranges(self, df: DataFrame, column_ranges: Dict[str, Tuple[float, float]]):
        self.logger.log_message(f"Checking value ranges for columns: {list(column_ranges.keys())}", level="info")
        for col, (min_val, max_val) in column_ranges.items():
            out_of_range_count = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
            if out_of_range_count > 0:
                self._raise_error(f"Data Quality Check Failed: Column '{col}' has {out_of_range_count} values out of range [{min_val}, {max_val}].")
            else:
                self.logger.log_message(f"Column '{col}' values are within the specified range [{min_val}, {max_val}].", level="info")

    def check_referential_integrity(self, df: DataFrame, reference_df: DataFrame, join_column: str):
        self.logger.log_message(f"Checking referential integrity on column '{join_column}'", level="info")
        unmatched_count = df.join(reference_df, df[join_column] == reference_df[join_column], "left_anti").count()
        if unmatched_count > 0:
            self._raise_error(f"Data Quality Check Failed: {unmatched_count} records in '{join_column}' do not match the reference data.")
        else:
            self.logger.log_message(f"Referential integrity check passed for column '{join_column}'.", level="info")

    def check_consistency_between_fields(self, df: DataFrame, consistency_pairs: List[Tuple[str, str]]):
        self.logger.log_message(f"Checking consistency between field pairs: {consistency_pairs}", level="info")
        for col1, col2 in consistency_pairs:
            inconsistency_count = df.filter(F.col(col1) > F.col(col2)).count()
            if inconsistency_count > 0:
                self._raise_error(f"Data Quality Check Failed: {inconsistency_count} records have '{col1}' greater than '{col2}'.")
            else:
                self.logger.log_message(f"Consistency check passed for '{col1}' and '{col2}'.", level="info")

    def apply_all_checks(self, df: DataFrame, key_columns: List[str], critical_columns: List[str],
                         column_ranges: Dict[str, Tuple[float, float]], reference_df: DataFrame,
                         join_column: str, consistency_pairs: List[Tuple[str, str]]):
        try:
            self.check_for_duplicates(df, key_columns)
            self.check_for_nulls(df, critical_columns)
            self.check_value_ranges(df, column_ranges)
            self.check_referential_integrity(df, reference_df, join_column)
            self.check_consistency_between_fields(df, consistency_pairs)
            self.logger.log_message("All quality checks completed successfully.", level="info")
        except Exception as e:
            self._raise_error(f"Data Quality Check Failed: {e}")