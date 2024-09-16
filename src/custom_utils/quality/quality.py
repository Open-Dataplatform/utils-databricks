# File: custom_utils/quality/quality.py

import json
from typing import List, Dict, Tuple, Optional
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from custom_utils.logging.logger import Logger

class Quality:
    def __init__(self, logger, debug=False):
        self.logger = logger
        self.debug = debug

    def _log_message(self, message: str, level="info"):
        """
        Logs a message using the logger if debug mode is on or the log level is not 'info'.

        Args:
            message (str): The message to log.
            level (str): The log level (e.g., 'info', 'warning', 'error').
        """
        if self.debug or level != "info":
            self.logger.log_message(message, level=level)

    def _log_block(self, header: str, content_lines: list):
        """
        Logs a block of messages using the logger if debug mode is on.

        Args:
            header (str): Header of the block.
            content_lines (list): List of lines to include in the block.
        """
        if self.debug:
            self.logger.log_block(header, content_lines)

    def _raise_error(self, message: str):
        self._log_message(message, level="error")
        if self.debug:
            self._log_message(f"Debug mode: Aborting with error - {message}", level="error")
        raise RuntimeError(message)

    def check_for_duplicates(self, spark: SparkSession, df: DataFrame, key_columns: List[str]):
        """Checks for duplicates using all columns, including 'input_file_name'."""
        all_columns = key_columns + ['input_file_name']
        content_lines = [f"Checking for duplicates based on columns: {all_columns}"]

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
            content_lines.append(f"Data Quality Check Failed: Found {duplicate_count} duplicates based on columns {all_columns}.")
            self._log_block("Duplicate Check", content_lines)
            self._raise_error(content_lines[-1])
        else:
            content_lines.append("Data Quality Check Passed: No duplicates found.")
            self._log_block("Duplicate Check", content_lines)

    def check_for_nulls(self, df: DataFrame, critical_columns: List[str]):
        content_lines = [f"Checking for null values in columns: {critical_columns}"]
        for col in critical_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                content_lines.append(f"Data Quality Check Failed: Column '{col}' has {null_count} missing values.")
                self._log_block("Null Values Check", content_lines)
                self._raise_error(content_lines[-1])
            else:
                content_lines.append(f"Column '{col}' has no missing values.")
        self._log_block("Null Values Check", content_lines)

    def check_value_ranges(self, df: DataFrame, column_ranges: Dict[str, Tuple[float, float]]):
        content_lines = [f"Checking value ranges for columns: {list(column_ranges.keys())}"]
        for col, (min_val, max_val) in column_ranges.items():
            out_of_range_count = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
            if out_of_range_count > 0:
                content_lines.append(f"Data Quality Check Failed: Column '{col}' has {out_of_range_count} values out of range [{min_val}, {max_val}].")
                self._log_block("Value Range Check", content_lines)
                self._raise_error(content_lines[-1])
            else:
                content_lines.append(f"Column '{col}' values are within the specified range [{min_val}, {max_val}].")
        self._log_block("Value Range Check", content_lines)

    def check_referential_integrity(self, df: DataFrame, reference_df: DataFrame, join_column: str):
        content_lines = [f"Checking referential integrity on column '{join_column}'"]
        unmatched_count = df.join(reference_df, df[join_column] == reference_df[join_column], "left_anti").count()
        if unmatched_count > 0:
            content_lines.append(f"Data Quality Check Failed: {unmatched_count} records in '{join_column}' do not match the reference data.")
            self._log_block("Referential Integrity Check", content_lines)
            self._raise_error(content_lines[-1])
        else:
            content_lines.append(f"Referential integrity check passed for column '{join_column}'.")
        self._log_block("Referential Integrity Check", content_lines)

    def check_consistency_between_fields(self, df: DataFrame, consistency_pairs: List[Tuple[str, str]]):
        content_lines = [f"Checking consistency between field pairs: {consistency_pairs}"]
        for col1, col2 in consistency_pairs:
            inconsistency_count = df.filter(F.col(col1) > F.col(col2)).count()
            if inconsistency_count > 0:
                content_lines.append(f"Data Quality Check Failed: {inconsistency_count} records have '{col1}' greater than '{col2}'.")
                self._log_block("Field Consistency Check", content_lines)
                self._raise_error(content_lines[-1])
            else:
                content_lines.append(f"Consistency check passed for '{col1}' and '{col2}'.")
        self._log_block("Field Consistency Check", content_lines)

    def apply_all_checks(
        self,
        spark: SparkSession,
        df: DataFrame,
        key_columns: List[str],
        critical_columns: Optional[List[str]] = None,
        column_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
        reference_df: Optional[DataFrame] = None,
        join_column: Optional[str] = None,
        consistency_pairs: Optional[List[Tuple[str, str]]] = None,
        columns_to_exclude: Optional[List[str]] = None,
        temp_view_name: Optional[str] = "cleaned_data_view"
    ) -> str:
        """
        Applies all data quality checks on the provided DataFrame.
        """
        try:
            # Handle multiple files and keep the most recent version based on input_file_name and EventTimestamp
            content_lines = ["Handling multiple files and keeping the most recent version based on 'input_file_name' and 'EventTimestamp'."]
            self._log_block("Handling Multiple Files", content_lines)

            df.createOrReplaceTempView("temp_original_data")
            recent_data_query = f"""
                CREATE OR REPLACE TEMPORARY VIEW temp_recent_data AS
                SELECT *
                FROM (
                    SELECT t.*, 
                           ROW_NUMBER() OVER (PARTITION BY Guid 
                                              ORDER BY input_file_name DESC, EventTimestamp DESC) AS rnr
                    FROM temp_original_data t
                ) x
                WHERE rnr = 1
            """
            spark.sql(recent_data_query)
            df = spark.sql("SELECT * FROM temp_recent_data").drop("rnr")

            # Perform quality checks
            self.check_for_duplicates(spark, df, key_columns)
            
            if critical_columns:
                self.check_for_nulls(df, critical_columns)
            
            if column_ranges:
                self.check_value_ranges(df, column_ranges)
            
            if reference_df and join_column:
                self.check_referential_integrity(df, reference_df, join_column)
            
            if consistency_pairs:
                self.check_consistency_between_fields(df, consistency_pairs)

            # Exclude specified columns before returning the final view
            if columns_to_exclude:
                content_lines = [f"Excluded columns: {columns_to_exclude}"]
                df = df.drop(*columns_to_exclude)
                self._log_block("Excluding Columns", content_lines)

            # Create the final view
            df.createOrReplaceTempView(temp_view_name)
            self._log_message(f"New temporary view '{temp_view_name}' created.", level="info")
            
            self._log_message("All quality checks completed successfully.", level="info")
            return temp_view_name

        except Exception as e:
            self._raise_error(f"Data Quality Check Failed: {e}")