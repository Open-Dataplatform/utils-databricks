# File: custom_utils/quality/quality.py

import pyspark.sql.functions as F
import sqlparse
from typing import List, Dict, Tuple, Optional, Union
from pyspark.sql import DataFrame, SparkSession, Window

class DataQualityManager:
    def __init__(self, logger, debug=False):
        """
        Initializes the DataQualityManager with logging and debugging capabilities.

        Args:
            logger (Logger): Logger object for logging messages.
            debug (bool): If True, enables debug-level logging.
        """
        self.logger = logger
        self.debug = debug

    def _log_message(self, message: str, level: str = "info"):
        """Logs a message using the logger."""
        if self.debug or level != "info":
            self.logger.log_message(message, level=level)

    def _log_block(self, header: str, content_lines: List[str]):
        """Logs a block of messages under a common header using the logger."""
        if self.debug:
            self.logger.log_block(header, content_lines)

    def _raise_error(self, message: str):
        """Logs an error message and raises a RuntimeError."""
        self._log_message(message, level="error")
        raise RuntimeError(message)

    def _format_sql_query(self, query: str) -> str:
        """Formats SQL queries using sqlparse and adds custom indentation."""
        sql_indent = " " * 7  # Indentation to match "[INFO] "
        formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
        indented_query = "\n".join([sql_indent + line if line.strip() else line for line in formatted_query.splitlines()])
        return f"{indented_query}\n"

    def _list_available_checks(self):
        """Lists all available checks with a description of their parameters."""
        return {
            "Handling Multiple Files": "Triggered by 'key_columns'. Uses input_file_name to keep the latest record per key.",
            "Duplicate Check": "Triggered by 'key_columns'. Checks for duplicate records based on specified columns.",
            "Null Values Check": "Triggered by 'critical_columns'. Checks for null values in specified columns.",
            "Value Range Check": "Triggered by 'column_ranges'. Checks if values in specified columns are within the provided ranges.",
            "Referential Integrity Check": "Triggered by 'reference_df' and 'join_column'. Checks if all records have matching references in another dataset.",
            "Field Consistency Check": "Triggered by 'consistency_pairs'. Checks if values in specified column pairs are consistent (e.g., start date < end date).",
            "Exclude Columns": "Triggered by 'columns_to_exclude'. Drops specified columns from the DataFrame."
        }

    def _list_checks_to_perform(self, **kwargs) -> List[str]:
        """Generates a list of checks to be performed based on input arguments."""
        checks_to_perform = []
        if kwargs.get('key_columns'):
            checks_to_perform.append('Handling Multiple Files')
            checks_to_perform.append('Duplicate Check')
        if kwargs.get('critical_columns'):
            checks_to_perform.append('Null Values Check')
        if kwargs.get('column_ranges'):
            checks_to_perform.append('Value Range Check')
        if kwargs.get('reference_df') and kwargs.get('join_column'):
            checks_to_perform.append('Referential Integrity Check')
        if kwargs.get('consistency_pairs'):
            checks_to_perform.append('Field Consistency Check')
        if kwargs.get('columns_to_exclude'):
            checks_to_perform.append('Exclude Columns')
        return checks_to_perform

    def describe_available_checks(self):
        """Logs the available checks and the parameters required to trigger them."""
        available_checks = self._list_available_checks()
        self._log_block("Available Quality Checks", [f"{check}: {description}" for check, description in available_checks.items()])

    def _handle_multiple_files(self, spark: SparkSession, df: DataFrame, key_columns: [str, List[str]], order_by: Optional[Union[str, List[str]]] = None, use_sql: bool = False) -> DataFrame:
        """
        Handles multiple files by keeping the latest record per key based on 'input_file_name' and the specified 'order_by' columns.

        Args:
            df (DataFrame): Input DataFrame.
            key_columns (str or List[str]): Columns to use for partitioning (always includes 'input_file_name').
            order_by (Optional[str or List[str]]): Columns to use for ordering within partitions. Defaults to 'input_file_name'.
            use_sql (bool): Whether to use SQL or DataFrame operations.
        
        Returns:
            DataFrame: DataFrame with the latest record per key.
        """
        # Ensure key_columns is a list and include 'input_file_name'
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        key_columns = list(set(['input_file_name'] + key_columns))

        # Default to ordering by 'input_file_name' if order_by is not provided
        if not order_by:
            order_by = ['input_file_name']
        elif isinstance(order_by, str):
            order_by = [order_by]
        
        # Construct the order_by clause for SQL
        order_by_clause = ", ".join([f"{col} DESC" for col in ['input_file_name'] + order_by if col not in ['input_file_name']])

        self._log_block("Handling Multiple Files", [f"Handling multiple files using key columns: {key_columns} and ordering by: {order_by}"])
        try:
            if use_sql:
                df.createOrReplaceTempView("temp_original_data")
                recent_data_query = f"""
                    CREATE OR REPLACE TEMPORARY VIEW temp_recent_data AS
                    SELECT *
                    FROM (
                        SELECT t.*, ROW_NUMBER() OVER (PARTITION BY {', '.join(key_columns)} ORDER BY {order_by_clause}) AS rnr
                        FROM temp_original_data t
                    ) x
                    WHERE rnr = 1
                """
                formatted_query = self._format_sql_query(recent_data_query)
                self._log_message(f"The following SQL query is used to handle multiple files:\n{formatted_query}\n")
                spark.sql(recent_data_query)
                df = spark.sql("SELECT * FROM temp_recent_data").drop("rnr")
            else:
                # Create window specification for DataFrame operations
                window_spec = Window.partitionBy(key_columns).orderBy(*[F.col(col).desc() for col in order_by])
                df = df.withColumn('rnr', F.row_number().over(window_spec)).filter(F.col('rnr') == 1).drop('rnr')
                self._log_message(f"DataFrame operation used: Applied row_number() with partition on {key_columns} and ordered by {order_by_clause}.")

            self._log_message("Handling multiple files completed successfully.")
            return df

        except AnalysisException as e:
            self._raise_error(f"Failed to handle multiple files: {e}")

    def _check_for_duplicates(self, spark: SparkSession, df: DataFrame, key_columns: [str, List[str]], use_sql: bool = False):
        # Ensure key_columns is a list
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        self._log_block("Duplicate Check", [f"Checking for duplicates using key columns: {key_columns}"])
        try:
            if use_sql:
                df.createOrReplaceTempView("temp_view_check_duplicates")
                duplicate_check_query = f"""
                    SELECT 
                        COUNT(*) AS duplicate_count, {', '.join(key_columns)}
                    FROM temp_view_check_duplicates
                    GROUP BY {', '.join(key_columns)}
                    HAVING COUNT(*) > 1
                """
                formatted_query = self._format_sql_query(duplicate_check_query)
                self._log_message(f"The following SQL query is used to check for duplicates:\n{formatted_query}\n")
                duplicates_df = spark.sql(duplicate_check_query)
                duplicate_count = duplicates_df.count()
            else:
                duplicates_df = df.groupBy(key_columns).count().filter(F.col('count') > 1)
                self._log_message(f"DataFrame operation used: GroupBy on {key_columns} and filter where count > 1.")
                duplicate_count = duplicates_df.count()

            if duplicate_count > 0:
                self._raise_error(f"Duplicate check failed: Found {duplicate_count} duplicates based on key columns {key_columns}.")
            else:
                self._log_message("Duplicate check passed: No duplicates found.")
        except Exception as e:
            self._raise_error(f"Failed to check for duplicates: {e}")

    def _check_for_nulls(self, spark: SparkSession, df: DataFrame, critical_columns: List[str], use_python: bool = False):
        """Checks for null values in the specified critical columns using either SQL or Python."""
        self._log_block("Null Values Check", [f"Checking for null values in columns: {critical_columns}"])
        try:
            if use_python:
                for col in critical_columns:
                    # Python approach
                    null_count = df.filter(F.col(col).isNull()).count()
                    python_code = f"df.filter(F.col('{col}').isNull()).count()"
                    self._log_message(f"Python code: {python_code}")

                    if null_count > 0:
                        self._raise_error(f"Null values check failed: Column '{col}' has {null_count} missing values.")
                    else:
                        self._log_message(f"Column '{col}' has no missing values.")
            else:
                # SQL approach
                df.createOrReplaceTempView("temp_view_check_nulls")
                null_check_query = " UNION ALL ".join([
                    f"SELECT COUNT(*) AS null_count, '{col}' AS column_name FROM temp_view_check_nulls WHERE {col} IS NULL"
                    for col in critical_columns
                ])
                formatted_query = self._format_sql_query(null_check_query)
                self._log_message(f"The following SQL query is used to check for null values:\n{formatted_query}\n")

                nulls_df = spark.sql(null_check_query)
                for row in nulls_df.collect():
                    if row['null_count'] > 0:
                        self._raise_error(f"Null values check failed: Column '{row['column_name']}' has {row['null_count']} missing values.")
                self._log_message("Null values check passed: No missing values found in specified columns.")
        except Exception as e:
            self._raise_error(f"Failed to check for null values: {e}")

    def _check_value_ranges(self, spark: SparkSession, df: DataFrame, column_ranges: Dict[str, Tuple[float, float]], use_python: bool = False):
        """Checks if values in the specified columns are within the provided ranges using either SQL or Python."""
        self._log_block("Value Range Check", [f"Checking value ranges for columns: {list(column_ranges.keys())}"])
        try:
            if use_python:
                for col, (min_val, max_val) in column_ranges.items():
                    # Python approach
                    out_of_range_count = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
                    python_code = f"df.filter((F.col('{col}') < {min_val}) | (F.col('{col}') > {max_val})).count()"
                    self._log_message(f"Python code: {python_code}")

                    if out_of_range_count > 0:
                        self._raise_error(f"Value range check failed: Column '{col}' has {out_of_range_count} values out of range [{min_val}, {max_val}].")
                    else:
                        self._log_message(f"Column '{col}' values are within the specified range [{min_val}, {max_val}].")
            else:
                # SQL approach
                df.createOrReplaceTempView("temp_view_check_ranges")
                range_check_query = " UNION ALL ".join([
                    f"SELECT COUNT(*) AS out_of_range_count, '{col}' AS column_name FROM temp_view_check_ranges WHERE {col} < {min_val} OR {col} > {max_val}"
                    for col, (min_val, max_val) in column_ranges.items()
                ])
                formatted_query = self._format_sql_query(range_check_query)
                self._log_message(f"The following SQL query is used to check value ranges:\n{formatted_query}\n")

                ranges_df = spark.sql(range_check_query)
                for row in ranges_df.collect():
                    if row['out_of_range_count'] > 0:
                        self._raise_error(f"Value range check failed: Column '{row['column_name']}' has {row['out_of_range_count']} values out of range.")
                self._log_message("Value range check passed: All values are within the specified ranges.")
        except Exception as e:
            self._raise_error(f"Failed to check value ranges: {e}")

    def _check_referential_integrity(self, spark: SparkSession, df: DataFrame, reference_df: DataFrame, join_column: str, use_python: bool = False):
        """Checks if all records in the DataFrame have matching references in the reference DataFrame using either SQL or Python."""
        self._log_block("Referential Integrity Check", [f"Checking referential integrity on column '{join_column}'"])
        try:
            if use_python:
                # Python approach
                unmatched_count = df.join(reference_df, df[join_column] == reference_df[join_column], "left_anti").count()
                python_code = f"df.join(reference_df, df['{join_column}'] == reference_df['{join_column}'], 'left_anti').count()"
                self._log_message(f"Python code: {python_code}")

                if unmatched_count > 0:
                    self._raise_error(f"Referential integrity check failed: {unmatched_count} records in '{join_column}' do not match the reference data.")
                else:
                    self._log_message(f"Referential integrity check passed for column '{join_column}'.")
            else:
                # SQL approach
                df.createOrReplaceTempView("temp_view_check_referential")
                reference_df.createOrReplaceTempView("reference_view")
                referential_query = f"""
                    SELECT COUNT(*) AS unmatched_count
                    FROM temp_view_check_referential t
                    LEFT JOIN reference_view r ON t.{join_column} = r.{join_column}
                    WHERE r.{join_column} IS NULL
                """
                formatted_query = self._format_sql_query(referential_query)
                self._log_message(f"The following SQL query is used to check referential integrity:\n{formatted_query}\n")

                referential_df = spark.sql(referential_query)
                unmatched_count = referential_df.collect()[0]['unmatched_count']

                if unmatched_count > 0:
                    self._raise_error(f"Referential integrity check failed: {unmatched_count} records in '{join_column}' do not match the reference data.")
                else:
                    self._log_message(f"Referential integrity check passed for column '{join_column}'.")
        except Exception as e:
            self._raise_error(f"Failed to check referential integrity: {e}")

    def _check_consistency_between_fields(self, spark: SparkSession, df: DataFrame, consistency_pairs: List[Tuple[str, str]], use_python: bool = False):
        """Checks consistency between specified field pairs using either SQL or Python."""
        self._log_block("Field Consistency Check", [f"Checking consistency between field pairs: {consistency_pairs}"])
        try:
            if use_python:
                for col1, col2 in consistency_pairs:
                    # Python approach
                    inconsistency_count = df.filter(F.col(col1) > F.col(col2)).count()
                    python_code = f"df.filter(F.col('{col1}') > F.col('{col2}')).count()"
                    self._log_message(f"Python code: {python_code}")

                    if inconsistency_count > 0:
                        self._raise_error(f"Consistency check failed: {inconsistency_count} records have '{col1}' greater than '{col2}'.")
                    else:
                        self._log_message(f"Consistency check passed for '{col1}' and '{col2}'.")
            else:
                # SQL approach
                df.createOrReplaceTempView("temp_view_check_consistency")
                consistency_queries = " UNION ALL ".join([
                    f"SELECT COUNT(*) AS inconsistency_count, '{col1}' AS column_1, '{col2}' AS column_2 FROM temp_view_check_consistency WHERE {col1} > {col2}"
                    for col1, col2 in consistency_pairs
                ])
                formatted_query = self._format_sql_query(consistency_queries)
                self._log_message(f"The following SQL query is used to check field consistency:\n{formatted_query}\n")

                consistency_df = spark.sql(consistency_queries)
                for row in consistency_df.collect():
                    if row['inconsistency_count'] > 0:
                        self._raise_error(f"Consistency check failed: {row['inconsistency_count']} records have '{row['column_1']}' greater than '{row['column_2']}'.")
                self._log_message("Consistency check passed for all specified pairs.")
        except Exception as e:
            self._raise_error(f"Failed to check field consistency: {e}")

    def run_all_checks(
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
        use_sql: bool = False
    ) -> str:
        """Executes all data quality checks based on the provided parameters."""
        try:
            # Handling multiple files
            df = self._handle_multiple_files(df, key_columns, use_sql=use_sql)

            # Check for duplicates
            self._check_for_duplicates(spark, df, key_columns, use_sql=use_sql)

            # Check for null values
            if critical_columns:
                self._check_for_nulls(df, critical_columns)

            # Check value ranges
            if column_ranges:
                self._check_value_ranges(df, column_ranges)

            # Check referential integrity
            if reference_df and join_column:
                self._check_referential_integrity(df, reference_df, join_column)

            # Check field consistency
            if consistency_pairs:
                self._check_consistency_between_fields(df, consistency_pairs)

            # Exclude columns
            if columns_to_exclude:
                df = df.drop(*columns_to_exclude)
                self._log_block("Excluding Columns", [f"Excluded columns: {columns_to_exclude}"])

            # Create the final view
            temp_view_name = "cleaned_data_view"
            df.createOrReplaceTempView(temp_view_name)
            self._log_block("Finishing Results", [f"New temporary view '{temp_view_name}' created.", "All quality checks completed successfully."])
            return temp_view_name

        except Exception as e:
            self._raise_error(f"Data quality checks failed: {e}")

    def perform_data_quality_checks(
        self,
        spark: SparkSession,
        df: DataFrame,
        key_columns: Union[str, List[str]],
        critical_columns: Optional[List[str]] = None,
        column_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
        reference_df: Optional[DataFrame] = None,
        join_column: Optional[str] = None,
        consistency_pairs: Optional[List[Tuple[str, str]]] = None,
        columns_to_exclude: Optional[List[str]] = None,
        order_by: Optional[Union[str, List[str]]] = None,  # Add order_by parameter
        use_python: Optional[bool] = None  # Optional parameter, defaults to None
    ) -> str:
        """
        Main method to start the data quality process.

        Args:
            spark (SparkSession): Spark session.
            df (DataFrame): DataFrame to perform quality checks on.
            key_columns (Union[str, List[str]]): Key columns for partitioning.
            critical_columns (Optional[List[str]]): Columns to check for null values.
            column_ranges (Optional[Dict[str, Tuple[float, float]]]): Value ranges for columns.
            reference_df (Optional[DataFrame]): Reference DataFrame for integrity check.
            join_column (Optional[str]): Column to use for referential integrity check.
            consistency_pairs (Optional[List[Tuple[str, str]]]): Column pairs for consistency check.
            columns_to_exclude (Optional[List[str]]): Columns to exclude from the final DataFrame.
            order_by (Optional[Union[str, List[str]]]): Columns to use for ordering. Defaults to 'input_file_name'.
            use_python (bool): If True, uses Python syntax for operations. Defaults to False, which uses SQL.

        Returns:
            str: Name of the temporary view created.
        """
        # Log start of the process
        self.logger.log_start("Data Quality Check Process")

        # Get the list of checks to perform
        checks_to_perform = self._list_checks_to_perform(
            key_columns=key_columns,
            critical_columns=critical_columns,
            column_ranges=column_ranges,
            reference_df=reference_df,
            join_column=join_column,
            consistency_pairs=consistency_pairs,
            columns_to_exclude=columns_to_exclude
        )

        self._log_block("Quality Checks to Perform", [f"Checks to be performed: {', '.join(checks_to_perform)}"])

        try:
            # Handling multiple files
            if 'Handling Multiple Files' in checks_to_perform:
                df = self._handle_multiple_files(spark, df, key_columns, order_by=order_by, use_sql=not use_python)

            # Check for duplicates
            if 'Duplicate Check' in checks_to_perform:
                self._check_for_duplicates(spark, df, key_columns, use_sql=not use_python)

            # Check for null values
            if 'Null Values Check' in checks_to_perform:
                self._check_for_nulls(spark, df, critical_columns, use_python=use_python)

            # Check value ranges
            if 'Value Range Check' in checks_to_perform:
                self._check_value_ranges(spark, df, column_ranges, use_python=use_python)

            # Check referential integrity
            if 'Referential Integrity Check' in checks_to_perform:
                self._check_referential_integrity(spark, df, reference_df, join_column, use_python=use_python)

            # Check field consistency
            if 'Field Consistency Check' in checks_to_perform:
                self._check_consistency_between_fields(spark, df, consistency_pairs, use_python=use_python)

            # Exclude columns
            if 'Exclude Columns' in checks_to_perform:
                df = df.drop(*columns_to_exclude)
                self._log_block("Excluding Columns", [f"Excluded columns: {columns_to_exclude}"])

            # Create the final view
            temp_view_name = "cleaned_data_view"
            df.createOrReplaceTempView(temp_view_name)
            self._log_block("Finishing Results", [f"New temporary view '{temp_view_name}' created.", "All quality checks completed successfully."])
            
            # Log end of the process
            self.logger.log_end("Data Quality Check Process", success=True, additional_message="Proceeding with notebook execution.")
            
            return temp_view_name

        except Exception as e:
            self.logger.log_end("Data Quality Check Process", success=False)
            self._raise_error(f"Data quality checks failed: {e}")