# File: custom_utils/quality/quality.py

import json
from typing import List, Dict, Tuple, Optional
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StructType, StringType
from pyspark.sql.utils import AnalysisException
from custom_utils.logging.logger import Logger

class Quality:
    def __init__(self, logger, debug=False):
        self.logger = logger
        self.debug = debug

    def _log_section(self, section_title: str):
        """Logs the section title in a formatted way for better readability."""
        self.logger.log_message(f"\n=== {section_title} ===\n{'-' * 30}", level="info")

    def _raise_error(self, message: str):
        self.logger.log_message(message, level="error")
        if self.debug:
            self.logger.log_message(f"Debug mode: Aborting with error - {message}", level="error")
        raise RuntimeError(message)

    def check_for_duplicates(self, spark: SparkSession, df: DataFrame, key_columns: List[str]):
        """
        Checks for duplicates using all columns, including 'input_file_name'.
        """
        self._log_section("Duplicate Check")
        all_columns = key_columns + ['input_file_name']
        self.logger.log_message(f"Checking for duplicates based on columns: {all_columns}", level="info")
        
        # Construct and execute the duplicate check query
        df.createOrReplaceTempView("temp_view_check_duplicates")
        duplicate_check_query = f"""
            SELECT 
                COUNT(*) AS duplicate_count, 
                {', '.join(all_columns)}
            FROM temp_view_check_duplicates
            GROUP BY {', '.join(all_columns)}
            HAVING COUNT(*) > 1
        """
        
        duplicates_df = spark.sql(duplicate_check_query)
        duplicate_count = duplicates_df.count()
        
        if duplicate_count > 0:
            self._raise_error(f"Data Quality Check Failed: Found {duplicate_count} duplicates based on columns {all_columns}.")
        else:
            self.logger.log_message("Data Quality Check Passed: No duplicates found.", level="info")

    def check_for_nulls(self, df: DataFrame, critical_columns: List[str]):
        self._log_section("Null Values Check")
        self.logger.log_message(f"Checking for null values in columns: {critical_columns}", level="info")
        for col in critical_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                self._raise_error(f"Data Quality Check Failed: Column '{col}' has {null_count} missing values.")
            else:
                self.logger.log_message(f"Column '{col}' has no missing values.", level="info")

    def check_value_ranges(self, df: DataFrame, column_ranges: Dict[str, Tuple[float, float]]):
        self._log_section("Value Range Check")
        self.logger.log_message(f"Checking value ranges for columns: {list(column_ranges.keys())}", level="info")
        for col, (min_val, max_val) in column_ranges.items():
            out_of_range_count = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
            if out_of_range_count > 0:
                self._raise_error(f"Data Quality Check Failed: Column '{col}' has {out_of_range_count} values out of range [{min_val}, {max_val}].")
            else:
                self.logger.log_message(f"Column '{col}' values are within the specified range [{min_val}, {max_val}].", level="info")

    def check_referential_integrity(self, df: DataFrame, reference_df: DataFrame, join_column: str):
        self._log_section("Referential Integrity Check")
        self.logger.log_message(f"Checking referential integrity on column '{join_column}'", level="info")
        unmatched_count = df.join(reference_df, df[join_column] == reference_df[join_column], "left_anti").count()
        if unmatched_count > 0:
            self._raise_error(f"Data Quality Check Failed: {unmatched_count} records in '{join_column}' do not match the reference data.")
        else:
            self.logger.log_message(f"Referential integrity check passed for column '{join_column}'.", level="info")

    def check_consistency_between_fields(self, df: DataFrame, consistency_pairs: List[Tuple[str, str]]):
        self._log_section("Field Consistency Check")
        self.logger.log_message(f"Checking consistency between field pairs: {consistency_pairs}", level="info")
        for col1, col2 in consistency_pairs:
            inconsistency_count = df.filter(F.col(col1) > F.col(col2)).count()
            if inconsistency_count > 0:
                self._raise_error(f"Data Quality Check Failed: {inconsistency_count} records have '{col1}' greater than '{col2}'.")
            else:
                self.logger.log_message(f"Consistency check passed for '{col1}' and '{col2}'.", level="info")

    def apply_all_checks(
        self,
        spark,
        df: DataFrame,
        key_columns: List[str],
        critical_columns: List[str] = None,
        column_ranges: Dict[str, Tuple[float, float]] = None,
        reference_database: str = None,
        reference_table_name: str = None,
        join_column: str = None,
        consistency_pairs: List[Tuple[str, str]] = None,
        columns_to_exclude: List[str] = None,
        temp_view_name: str = None,
    ) -> str:
        try:
            # Load reference DataFrame if both database and table name are provided
            reference_df = None
            if reference_database and reference_table_name:
                reference_df = spark.table(f"{reference_database}.{reference_table_name}")
                self.logger.log_message(f"Loaded reference table: {reference_database}.{reference_table_name}", level="info")

            # Perform quality checks based on provided parameters
            self.check_for_duplicates(df, key_columns)
            if critical_columns:
                self.check_for_nulls(df, critical_columns)
            if column_ranges:
                self.check_value_ranges(df, column_ranges)
            if reference_df and join_column:
                self.check_referential_integrity(df, reference_df, join_column)
            if consistency_pairs:
                self.check_consistency_between_fields(df, consistency_pairs)

            # Select the most recent version for duplicates based on input_file_name
            recent_data_query = f"""
                CREATE OR REPLACE TEMPORARY VIEW temp_recent_data AS
                SELECT {', '.join([col for col in df.columns if col != 'rnr'])}  -- Exclude rnr column
                FROM (
                    SELECT t.*, 
                        row_number() OVER (PARTITION BY {', '.join(key_columns)} 
                                            ORDER BY input_file_name DESC, EventTimestamp DESC) AS rnr
                    FROM {df.createOrReplaceTempView("temp_view_for_dedup")}
                ) x
                WHERE rnr = 1
            """
            
            spark.sql(recent_data_query)
            df = spark.sql("SELECT * FROM temp_recent_data")  # Exclude rnr column in this view

            # Drop columns to be excluded before creating the final view
            if columns_to_exclude:
                self.logger.log_message(f"Excluded columns: {columns_to_exclude}", level="info")
                df = df.drop(*columns_to_exclude)

            # Create the final temporary view
            if temp_view_name:
                df.createOrReplaceTempView(temp_view_name)
                self.logger.log_message(f"New temporary view '{temp_view_name}' created with excluded columns.", level="info")
            else:
                temp_view_name = "default_temp_view"
                df.createOrReplaceTempView(temp_view_name)
                self.logger.log_message(f"New temporary view '{temp_view_name}' created with excluded columns.", level="info")

            # Log success message
            self.logger.log_message("All quality checks completed successfully.", level="info")
            return temp_view_name

        except Exception as e:
            self._raise_error(f"Data Quality Check Failed: {e}")
