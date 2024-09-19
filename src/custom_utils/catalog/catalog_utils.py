# File: custom_utils/catalog/catalog_utils.py

from custom_utils.dp_storage import writer
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional, Union
from custom_utils.logging.logger import Logger
import sqlparse

class DataStorageManager:
    def __init__(self, logger: Optional[Logger] = None, debug: bool = False):
        """
        Initialize the DataStorageManager.
        """
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug
        self.sections_logged = set()  # Track logged sections to avoid duplicates

    def _log_message(self, message: str, level: str = "info"):
        """
        Logs a message using the logger if debug mode is on or the log level is not 'info'.
        """
        if self.debug or level != "info":
            self.logger.log_message(message, level=level)

    def _log_section(self, section_title: str, content_lines: Optional[List[str]] = None):
        """
        Logs a section with the provided title and optional content lines using `log_block`
        if debug mode is on and the section hasn't been logged before.
        """
        if self.debug and section_title not in self.sections_logged:
            content_lines = content_lines if content_lines else []
            self.logger.log_block(section_title, content_lines)
            self.sections_logged.add(section_title)

    def _format_sql_query(self, query: str) -> str:
        """Formats SQL queries using sqlparse and adds custom indentation."""
        sql_indent = " " * 7  # Indentation to match "[INFO] "
        formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
        indented_query = "\n".join([sql_indent + line if line.strip() else line for line in formatted_query.splitlines()])
        return f"{indented_query}\n"

    def get_destination_details(self, spark: SparkSession, destination_environment: str, source_datasetidentifier: str):
        """
        Retrieves the destination path, database name, and table name.
        """
        try:
            destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
            database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
            
            content_lines = [
                f"Destination Path: {destination_path}",
                f"Database: {database_name}",
                f"Table: {table_name}"
            ]
            self._log_section("Destination Details", content_lines)
            return destination_path, database_name, table_name
        except Exception as e:
            self._log_message(f"Error retrieving destination details: {e}", level="error")
            raise

    def generate_feedback_timestamps(self, spark: SparkSession, cleaned_data_view: str, feedback_column: str) -> str:
        """
        Orchestrates the entire process of calculating and returning feedback timestamps.
        """
        # Generate the feedback SQL query
        feedback_sql = self.construct_feedback_sql(view_name, feedback_column)

        # Execute the feedback SQL query
        df_min_max = self.execute_feedback_sql(spark, feedback_sql)

        # Handle the result and return the feedback output as JSON
        return self.handle_feedback_result(df_min_max, view_name)

    def execute_feedback_sql(self, spark: SparkSession, feedback_sql: str) -> DataFrame:
        """
        Executes the SQL query to get the minimum and maximum timestamps.
        """
        try:
            return spark.sql(feedback_sql)
        except Exception as e:
            self._log_message(f"Error executing SQL query: {e}", level="error")
            raise

    def handle_feedback_result(self, df_min_max: DataFrame, view_name: str) -> str:
        """
        Handles the result of the feedback SQL query and converts it to JSON.
        """
        if df_min_max.head(1):  # Efficient way to check if the DataFrame is empty
            notebook_output = df_min_max.toJSON().first()
            self._log_message(f"Notebook output: {notebook_output}")
            return notebook_output
        else:
            error_message = f"No data found in {view_name} to calculate the feedback timestamps."
            self._log_message(error_message, level="error")
            raise ValueError(error_message)

    def construct_feedback_sql(self, view_name: str, feedback_column: str) -> str:
        """
        Constructs an SQL query to get the minimum and maximum timestamps for the feedback column.
        """
        feedback_sql = f"""
        SELECT
            MIN({feedback_column}) AS from_datetime,
            MAX({feedback_column}) AS to_datetime
        FROM {view_name}
        """
        formatted_sql = self._format_sql_query(feedback_sql)  # Use the imported formatting method
        self._log_message(f"Executing SQL query:\n{formatted_sql}")
        return feedback_sql

    def ensure_path_exists(self, dbutils, destination_path: str):
        """
        Ensures the destination path exists in DBFS.
        """
        try:
            try:
                dbutils.fs.ls(destination_path)
                self._log_section("Path Validation", [f"Path already exists: {destination_path}"])
            except Exception as e:
                if "java.io.FileNotFoundException" in str(e):
                    dbutils.fs.mkdirs(destination_path)
                    self._log_section("Path Validation", [f"Path did not exist. Created path: {destination_path}"])
                else:
                    raise
        except Exception as e:
            self._log_message(f"Error while ensuring path exists: {e}", level="error")
            raise

    def create_or_replace_table(self, spark: SparkSession, database_name: str, table_name: str, destination_path: str, cleaned_data_view: str, use_python: Optional[bool] = False):
        """
        Creates or replaces a Databricks Delta table.
        """
        try:
            df = spark.table(cleaned_data_view)
            schema_str = ",\n".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])

            if use_python:
                # Python DataFrame-based table creation
                self._log_section("Table Creation Process", [
                    f"Writing DataFrame to Delta at path '{destination_path}' and registering table '{database_name}.{table_name}'."
                ])
                df.write.format("delta").mode("overwrite").option("path", destination_path).saveAsTable(f"{database_name}.{table_name}")
                self._log_message(f"Table {database_name}.{table_name} created using DataFrame operations.")
            else:
                # SQL-based table creation
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    {schema_str}
                )
                USING DELTA
                LOCATION 'dbfs:{destination_path}/'
                """
                formatted_query = self._format_sql_query(create_table_sql.strip())  # Apply SQL formatting here
                self._log_section("Table Creation Process", [f"Creating table with query:\n{formatted_query}"])

                if not self.check_if_table_exists(spark, database_name, table_name):
                    spark.sql(create_table_sql)
                    self._log_message(f"Table {database_name}.{table_name} created.")

                    # Write data to the newly created table
                    df.write.format("delta").mode("overwrite").save(destination_path)
                    self._log_message(f"Data written to {destination_path}.")
                else:
                    self._log_message(f"Table {database_name}.{table_name} already exists.")
        except Exception as e:
            self._log_message(f"Error creating or replacing table: {e}", level="error")
            raise

    def check_if_table_exists(self, spark: SparkSession, database_name: str, table_name: str) -> bool:
        """
        Checks if a Databricks Delta table exists in the specified database.

        Args:
            spark (SparkSession): The Spark session.
            database_name (str): The name of the database.
            table_name (str): The name of the table to check for existence.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            # Fetch the list of tables in the specified database
            table_check = spark.sql(f"SHOW TABLES IN {database_name}").collect()
            # Check if the specified table exists
            table_exists = any(row["tableName"] == table_name for row in table_check)

            # Remove table existence logs
            # if table_exists:
            #     self._log_message(f"Table {database_name}.{table_name} exists.")
            # else:
            #     self._log_message(f"Table {database_name}.{table_name} does not exist.")
            
            return table_exists
        except Exception as e:
            self._log_message(f"Error checking table existence: {e}", level="error")
            raise

    def generate_merge_sql(self, spark: SparkSession, cleaned_data_view: str, database_name: str, table_name: str, key_columns: Union[str, List[str]]) -> str:
        """
        Constructs the SQL query for the MERGE operation.
        """
        try:
            # Normalize key_columns to a list
            if isinstance(key_columns, str):
                key_columns = [key_columns]

            target_table_columns = [field.name for field in spark.table(f"{database_name}.{table_name}").schema]
            all_columns = [col for col in spark.table(cleaned_data_view).columns if col in target_table_columns and col not in key_columns]

            match_sql = ' AND '.join([f"s.`{col}` = t.`{col}`" for col in key_columns])
            update_sql = ',\n           '.join([f"t.`{col}` = s.`{col}`" for col in all_columns])  # Indent for readability
            insert_columns = key_columns + all_columns
            insert_values = [f"s.`{col}`" for col in insert_columns]

            merge_sql = f"""
            MERGE INTO {database_name}.{table_name} AS t
            USING {cleaned_data_view} AS s
            ON {match_sql}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_sql}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join([f'`{col}`' for col in insert_columns])})
                VALUES ({', '.join(insert_values)})
            """

            # Apply formatting using _format_sql_query
            formatted_query = self._format_sql_query(merge_sql.strip())
            self._log_section("Data Merge", [f"MERGE SQL:\n{formatted_query}"])
            return merge_sql
        except Exception as e:
            self._log_message(f"Error generating merge SQL: {e}", level="error")
            raise

    def execute_merge(self, spark: SparkSession, database_name: str, table_name: str, cleaned_data_view: str, key_columns: Union[str, List[str]], use_python: Optional[bool] = False):
        """
        Executes the MERGE operation using either SQL or Python operations.
        """
        try:
            # Normalize key_columns to a list
            if isinstance(key_columns, str):
                key_columns = [key_columns]

            # Remove key columns logging
            # self._log_message(f"Key columns for merge: {key_columns}")
            
            target_df = spark.table(f"{database_name}.{table_name}")
            source_df = spark.table(cleaned_data_view)

            # Remove DataFrame column logs
            # self._log_message(f"Target DataFrame columns: {target_df.columns}")
            # self._log_message(f"Source DataFrame columns: {source_df.columns}")

            # Check if key_columns are valid in both DataFrames
            target_columns = [col.strip() for col in target_df.columns]
            source_columns = [col.strip() for col in source_df.columns]

            for col in key_columns:
                if col not in target_columns or col not in source_columns:
                    raise ValueError(f"Column '{col}' not found in target or source DataFrame columns. Available columns: {target_columns} (target), {source_columns} (source)")

            if use_python:
                # Python DataFrame-based merge
                self._log_section("Data Merge", [
                    f"Performing DataFrame merge using key columns: {key_columns}"
                ])

                # Define the merge condition
                merge_condition = [target_df[col] == source_df[col] for col in key_columns]
                merged_df = target_df.alias('t').join(source_df.alias('s'), on=merge_condition, how='outer')

                # Update and Insert logic (this part needs to be implemented based on business logic)
                # Example: merged_df.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")

                self._log_message(f"Data merged into {database_name}.{table_name} using DataFrame operations.")
            else:
                # SQL-based merge
                merge_sql = self.generate_merge_sql(spark, cleaned_data_view, database_name, table_name, key_columns)
                spark.sql(merge_sql)
                self._log_message(f"Data merged into {database_name}.{table_name} using SQL.")
        except Exception as e:
            self._log_message(f"Error during data merge: {e}", level="error")
            raise

    def manage_data_operation(self, spark: SparkSession, dbutils, destination_environment: str, source_datasetidentifier: str, cleaned_data_view: str, key_columns: Union[str, List[str]], use_python: Optional[bool] = False):
        """
        Main method to manage table creation and data merge operations.
        """
        try:
            # Normalize key_columns to a list
            if isinstance(key_columns, str):
                key_columns = [key_columns]

            # Log the input key_columns for debugging (REMOVE THIS LINE)
            # self._log_message(f"Received key_columns: {key_columns}")

            destination_path, database_name, table_name = self.get_destination_details(spark, destination_environment, source_datasetidentifier)
            self.ensure_path_exists(dbutils, destination_path)
            self.create_or_replace_table(spark, database_name, table_name, destination_path, cleaned_data_view, use_python=use_python)
            self.execute_merge(spark, database_name, table_name, cleaned_data_view, key_columns, use_python=use_python)
        except Exception as e:
            self._log_message(f"Error managing data operation: {e}", level="error")
            raise
