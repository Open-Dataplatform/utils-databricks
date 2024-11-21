import pyspark.sql.functions as F
import sqlparse
from typing import List, Dict, Tuple, Optional, Union
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.utils import AnalysisException

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

    @staticmethod
    def parse_key_columns(key_columns: Union[str, List[str]]) -> List[str]:
        """
        Parses key_columns input to ensure it is a list of strings.

        Args:
            key_columns (Union[str, List[str]]): The key columns input, either as a string or a list.

        Returns:
            List[str]: A list of column names.
        """
        if isinstance(key_columns, str):
            # Split the string by commas and strip whitespace
            return [col.strip() for col in key_columns.split(',')]
        elif isinstance(key_columns, list):
            return key_columns  # Already in the correct format
        else:
            raise ValueError(f"Invalid type for key_columns: {type(key_columns)}. Expected str or list.")

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
        self.logger.log_error(message)

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

    def _handle_multiple_files(
        self, 
        spark: SparkSession, 
        df: DataFrame, 
        key_columns: Union[str, List[str]], 
        order_by: Optional[Union[str, List[str]]] = None, 
        use_sql: bool = False
    ) -> DataFrame:
        """
        Handles multiple files by keeping the latest record per key based on 'input_file_name' and the specified 'order_by' columns.

        Args:
            df (DataFrame): Input DataFrame.
            key_columns (Union[str, List[str]]): Columns to use for partitioning.
            order_by (Optional[Union[str, List[str]]]): Columns to use for ordering within partitions.
            use_sql (bool): Whether to use SQL or DataFrame operations.
        
        Returns:
            DataFrame: DataFrame with the latest record per key.
        """
        # Parse key_columns to ensure consistency
        key_columns = self.parse_key_columns(key_columns)

        # Include 'input_file_name' in key_columns
        key_columns = list(set(['input_file_name'] + key_columns))

        # Parse order_by
        order_by = self.parse_key_columns(order_by or 'input_file_name')

        # Construct the order_by clause for SQL
        order_by_clause = ", ".join([f"{col} DESC" for col in order_by])

        self._log_block("Handling Multiple Files", [f"Key columns: {key_columns}", f"Order by: {order_by}"])

        try:
            if use_sql:
                df.createOrReplaceTempView("temp_original_data")
                query = f"""
                    CREATE OR REPLACE TEMP VIEW temp_recent_data AS
                    SELECT *
                    FROM (
                        SELECT t.*, ROW_NUMBER() OVER (PARTITION BY {', '.join(key_columns)} ORDER BY {order_by_clause}) AS rnr
                        FROM temp_original_data t
                    ) x WHERE rnr = 1
                """
                self._log_message(f"SQL Query:\n{query}")
                spark.sql(query)
                df = spark.sql("SELECT * FROM temp_recent_data").drop("rnr")
            else:
                window_spec = Window.partitionBy(key_columns).orderBy(*[F.col(col).desc() for col in order_by])
                df = df.withColumn("rnr", F.row_number().over(window_spec)).filter(F.col("rnr") == 1).drop("rnr")
            self._log_message("Handling multiple files completed.")
            return df
        except AnalysisException as e:
            self._raise_error(f"Failed to handle multiple files: {e}")

    def _check_for_duplicates(
        self,
        spark: SparkSession,
        df: DataFrame,
        key_columns: Union[str, List[str]],
        feedback_column: Optional[str] = None,
        remove_duplicates: bool = False,
        use_sql: bool = False
    ) -> DataFrame:
        """
        Checks for duplicate rows in the DataFrame.

        Args:
            spark (SparkSession): Spark session.
            df (DataFrame): Input DataFrame.
            key_columns (Union[str, List[str]]): Columns to check for duplicates.
            feedback_column (Optional[str]): Column for ordering during duplicate removal.
            remove_duplicates (bool): Whether to remove duplicates.
            use_sql (bool): Whether to use SQL or DataFrame operations.

        Returns:
            DataFrame: Processed DataFrame.
        """
        # Parse key_columns
        key_columns = self.parse_key_columns(key_columns)
        self._log_block("Duplicate Check", [f"Key columns: {key_columns}"])

        try:
            if use_sql:
                # SQL-based duplicate check
                df.createOrReplaceTempView("temp_duplicates")
                query = f"""
                    SELECT {', '.join(key_columns)}, COUNT(*) AS duplicate_count
                    FROM temp_duplicates
                    GROUP BY {', '.join(key_columns)}
                    HAVING COUNT(*) > 1
                """
                self._log_message(f"SQL Query:\n{query}")
                duplicates_df = spark.sql(query)
                duplicate_count = duplicates_df.count()
            else:
                # DataFrame-based duplicate check
                duplicates_df = df.groupBy(key_columns).count().filter(F.col("count") > 1)
                duplicate_count = duplicates_df.count()
                self._log_message(f"Duplicate check identified {duplicate_count} duplicate groups based on key columns {key_columns}.")

            if duplicate_count > 0:
                # Define columns to select for duplicate details
                if feedback_column:
                    columns_to_select = ["input_file_name"] + key_columns + [feedback_column]
                else:
                    columns_to_select = ["input_file_name"] + key_columns

                # Remove duplicates in columns_to_select by converting to a set and back to a list
                columns_to_select = list(dict.fromkeys(columns_to_select))

                # Join and select columns explicitly to avoid duplicates in output
                detailed_duplicates_df = (
                    df.alias("original")
                    .join(duplicates_df.alias("duplicates"), on=key_columns, how="inner")
                    .select(*[F.col(f"original.{col}").alias(col) for col in columns_to_select])
                )

                # Display duplicate details
                self._log_block("Duplicate Records Found", [
                    f"Details of duplicates based on {key_columns}:",
                    f"Total duplicate groups: {duplicate_count}"
                ])
                self._log_message("Duplicate details table (showing first few records):")
                detailed_duplicates_df.show(truncate=False)

                if remove_duplicates:
                    # Remove duplicates and retain the latest record
                    order_column = feedback_column if feedback_column else key_columns[0]
                    message = (
                        f"Duplicates found: Removing duplicates by keeping the latest record "
                        f"based on '{order_column}' with order by '{order_column} DESC'."
                    )
                    self._log_message(message)

                    if use_sql:
                        # SQL-based deduplication
                        deduplicate_query = f"""
                            CREATE OR REPLACE TEMPORARY VIEW temp_deduplicated_data AS
                            SELECT *
                            FROM (
                                SELECT t.*, ROW_NUMBER() OVER (
                                    PARTITION BY {', '.join(key_columns)} 
                                    ORDER BY {order_column} DESC
                                ) AS rnr
                                FROM temp_duplicates t
                            ) x
                            WHERE rnr = 1
                        """
                        formatted_query = self._format_sql_query(deduplicate_query)
                        self._log_message(f"SQL query used to remove duplicates:\n{formatted_query}\n")
                        spark.sql(deduplicate_query)
                        df = spark.sql("SELECT * FROM temp_deduplicated_data").drop("rnr")
                    else:
                        # DataFrame-based deduplication
                        window_spec = Window.partitionBy(key_columns).orderBy(F.col(order_column).desc())
                        df = df.withColumn("rnr", F.row_number().over(window_spec)).filter(F.col("rnr") == 1).drop("rnr")
                    self._log_message("Duplicates removed successfully. Only the latest records are retained.")
                else:
                    self._raise_error(f"Duplicate check failed: Found {duplicate_count} duplicates based on key columns {key_columns}.")
            else:
                self._log_message("Duplicate check passed: No duplicates found.")
            return df
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
        order_by: Optional[Union[str, List[str]]] = None,
        feedback_column: Optional[str] = None,  # Changed to None as default value
        use_python: Optional[bool] = None,
        remove_duplicates: Optional[bool] = True  # New parameter for removing duplicates
    ) -> str:
        """
        Main method to start the data quality process, performing various checks on the input DataFrame.

        Args:
            spark (SparkSession): Spark session.
            df (DataFrame): DataFrame to perform quality checks on.
            key_columns (Union[str, List[str]]): Key columns for partitioning and duplicate checking.
            critical_columns (Optional[List[str]]): Columns to check for null values.
            column_ranges (Optional[Dict[str, Tuple[float, float]]]): Value ranges for columns.
            reference_df (Optional[DataFrame]): Reference DataFrame for referential integrity check.
            join_column (Optional[str]): Column to use for referential integrity check.
            consistency_pairs (Optional[List[Tuple[str, str]]]): Column pairs for field consistency check.
            columns_to_exclude (Optional[List[str]]): Columns to exclude from the final DataFrame.
            order_by (Optional[Union[str, List[str]]]): Columns to use for ordering within partitions.
            feedback_column (Optional[str]): Column to use for ordering duplicates; falls back to `key_columns` if None.
            use_python (Optional[bool]): If True, uses Python syntax for operations; defaults to SQL.
            remove_duplicates (Optional[bool]): If True, removes duplicate rows found during the Duplicate Check.
                                                Keeps the latest record based on `feedback_column` or `key_columns`.

        Returns:
            str: Name of the temporary view created after processing.
        """
        # Log start of the process
        self.logger.log_start("Data Quality Check Process")

        # Parse key_columns
        key_columns = self.parse_key_columns(key_columns)
        self._log_block("Data Quality Checks", [f"Key columns: {key_columns}"])

        # Get the list of checks to perform based on provided parameters
        checks_to_perform = self._list_checks_to_perform(
            key_columns=key_columns,
            critical_columns=critical_columns,
            column_ranges=column_ranges,
            reference_df=reference_df,
            join_column=join_column,
            consistency_pairs=consistency_pairs,
            columns_to_exclude=columns_to_exclude
        )

        # Log the quality checks that will be performed
        self._log_block("Quality Checks to Perform", [f"Checks to be performed: {', '.join(checks_to_perform)}"])

        try:
            # Handling multiple files: Keeps the latest record based on `input_file_name` and `order_by`
            if 'Handling Multiple Files' in checks_to_perform:
                df = self._handle_multiple_files(spark, df, key_columns, order_by=order_by, use_sql=not use_python)

            # Check for duplicates and optionally remove them
            # If feedback_column is provided, use it for ordering during duplicate removal, else fallback to order_by
            duplicate_order_column = feedback_column if feedback_column else order_by
            if 'Duplicate Check' in checks_to_perform:
                try:
                    # Attempt to handle duplicates and potentially remove them
                    df = self._check_for_duplicates(
                        spark, df, key_columns, feedback_column=duplicate_order_column,
                        remove_duplicates=remove_duplicates, use_sql=not use_python
                    )
                except RuntimeError as dup_error:
                    # Catch and log duplicate error without re-raising it if remove_duplicates is enabled
                    if remove_duplicates:
                        self._log_message("Duplicates were found and removed as per the remove_duplicates parameter.")
                    else:
                        raise dup_error

            # Check for null values in specified critical columns
            if 'Null Values Check' in checks_to_perform:
                self._check_for_nulls(spark, df, critical_columns, use_python=use_python)

            # Check if values in specified columns fall within the defined ranges
            if 'Value Range Check' in checks_to_perform:
                self._check_value_ranges(spark, df, column_ranges, use_python=use_python)

            # Verify referential integrity by checking for matching records in `reference_df`
            if 'Referential Integrity Check' in checks_to_perform:
                self._check_referential_integrity(spark, df, reference_df, join_column, use_python=use_python)

            # Check consistency between specified column pairs (e.g., start_date < end_date)
            if 'Field Consistency Check' in checks_to_perform:
                self._check_consistency_between_fields(spark, df, consistency_pairs, use_python=use_python)

            # Exclude specified columns from the final DataFrame
            if 'Exclude Columns' in checks_to_perform:
                df = df.drop(*columns_to_exclude)
                self._log_block("Excluding Columns", [f"Excluded columns: {columns_to_exclude}"])

            # Create the final view of the processed DataFrame
            temp_view_name = "cleaned_data_view"
            df.createOrReplaceTempView(temp_view_name)
            self._log_block("Finishing Results", [f"New temporary view '{temp_view_name}' created.",
                                                "All quality checks completed successfully."])

            # Log successful end of the process
            self.logger.log_end("Data Quality Check Process", success=True, additional_message="Proceeding with notebook execution.")

            return temp_view_name

        except Exception as e:
            # Handle any errors during the process and log them
            error_message = f"Data quality checks failed: {e}"
            self.logger.log_error(error_message)
            self.logger.log_end("Data Quality Check Process", success=False)
            raise RuntimeError(error_message)