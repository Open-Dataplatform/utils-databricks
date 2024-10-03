from custom_utils.dp_storage import writer
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional, Union
from custom_utils.logging.logger import Logger
import sqlparse

class DataStorageManager:
    def __init__(self, 
                 logger: Optional[Logger] = None, 
                 debug: bool = False, 
                 destination_environment: Optional[str] = None, 
                 source_datasetidentifier: Optional[str] = None, 
                 destination_folder_path: Optional[str] = None):
        """
        Initialize the DataStorageManager with logger, debug mode, and optional config attributes.
        """
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug
        
        # Set default/fallback values from config (these will be your config values)
        self.destination_environment = destination_environment
        self.source_datasetidentifier = source_datasetidentifier
        self.destination_folder_path = destination_folder_path
        
        # Initialize sections_logged as an empty set to keep track of logged sections
        self.sections_logged = set()

    def _log_message(self, message: str, level: str = "info"):
        if self.debug or level != "info":
            self.logger.log_message(message, level=level)

    def _log_section(self, section_title: str, content_lines: Optional[List[str]] = None):
        if self.debug and section_title not in self.sections_logged:
            content_lines = content_lines if content_lines else []
            self.logger.log_block(section_title, content_lines)
            self.sections_logged.add(section_title)

    def ensure_path_exists(self, dbutils, destination_path: str):
        try:
            dbutils.fs.ls(destination_path)
            self._log_section("Path Validation", [f"Path already exists: {destination_path}"])
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                dbutils.fs.mkdirs(destination_path)
                self._log_section("Path Validation", [f"Path did not exist. Created path: {destination_path}"])
            else:
                self._log_message(f"Error while ensuring path exists: {e}", level="error")
                raise

    def normalize_key_columns(self, key_columns: Union[str, List[str]]) -> List[str]:
        if isinstance(key_columns, str):
            key_columns = [col.strip() for col in key_columns.split(',')]
        return key_columns
    
    def check_if_table_exists(self, spark: SparkSession, database_name: str, table_name: str) -> bool:
        """
        Checks if a Databricks Delta table exists in the specified database.
        """
        try:
            tables_df = spark.sql(f"SHOW TABLES IN {database_name}").filter(f"tableName = '{table_name}'")
            table_exists = tables_df.count() > 0
            return table_exists
        except Exception as e:
            self._log_message(f"Error checking table existence: {e}", level="error")
            raise

    def create_or_replace_table(self, spark: SparkSession, database_name: str, table_name: str, destination_path: str, cleaned_data_view: str, use_python: Optional[bool] = False):
        try:
            # Get the DataFrame from the cleaned data view
            df = spark.table(cleaned_data_view)

            # Create the schema string with indentation for better readability
            schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])

            # Create the SQL query for table creation, ensuring proper formatting
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                {schema_str}
            )
            USING DELTA
            LOCATION '{destination_path}/'
            """

            # Log block with the SQL query using the logger
            self.logger.log_block("Table Creation Process", sql_query=create_table_sql)

            if use_python:
                # Log the section for Python DataFrame operations
                self._log_section("Table Creation Process", [f"Writing DataFrame to Delta at path '{destination_path}' and registering table '{database_name}.{table_name}'."])
                
                # Write the DataFrame to Delta format and register the table
                df.write.format("delta").mode("overwrite").option("path", destination_path).saveAsTable(f"{database_name}.{table_name}")
                
                # Log the completion message
                self._log_message(f"Table {database_name}.{table_name} created using DataFrame operations.")
            else:
                # Check if the table already exists
                if not self.check_if_table_exists(spark, database_name, table_name):
                    # Execute the SQL query to create the table
                    spark.sql(create_table_sql)

                    # Log messages for table creation and data writing
                    self._log_message(f"Table {database_name}.{table_name} created.")
                    
                    # Write the data to the Delta table
                    df.write.format("delta").mode("overwrite").save(destination_path)
                    self._log_message(f"Data written to {destination_path}.")
                else:
                    # Log a message indicating the table already exists
                    self._log_message(f"Table {database_name}.{table_name} already exists.")
        except Exception as e:
            # Log any errors during the process
            self._log_message(f"Error creating or replacing table: {e}", level="error")
            raise

    def generate_merge_sql(self, spark: SparkSession, cleaned_data_view: str, database_name: str, table_name: str, key_columns: Union[str, List[str]]) -> str:
        """
        Constructs the SQL query for the MERGE operation.
        """
        try:
            key_columns = self.normalize_key_columns(key_columns)

            target_table_columns = [field.name for field in spark.table(f"{database_name}.{table_name}").schema]
            all_columns = [col for col in spark.table(cleaned_data_view).columns if col in target_table_columns and col not in key_columns]

            match_sql = ' AND '.join([f"s.`{col}` = t.`{col}`" for col in key_columns])
            update_sql = ', '.join([f"t.`{col}` = s.`{col}`" for col in all_columns])
            insert_columns = key_columns + all_columns
            insert_values = [f"s.`{col}`" for col in insert_columns]

            merge_sql = f"""
            MERGE INTO {database_name}.{table_name} AS t
            USING {cleaned_data_view} AS s
            ON {match_sql}
            WHEN MATCHED THEN
                UPDATE SET {update_sql}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join([f'`{col}`' for col in insert_columns])})
                VALUES ({', '.join(insert_values)})
            """

            return merge_sql
        except Exception as e:
            self._log_message(f"Error generating merge SQL: {e}", level="error")
            raise

    def execute_merge(self, spark: SparkSession, database_name: str, table_name: str, cleaned_data_view: str, key_columns: Union[str, List[str]], use_python: Optional[bool] = False) -> int:
        """
        Executes the MERGE operation and logs the SQL query and results using logger methods.
        """
        try:
            key_columns = self.normalize_key_columns(key_columns)

            if use_python:
                pass
            else:
                # Generate the MERGE SQL query
                merge_sql = self.generate_merge_sql(spark, cleaned_data_view, database_name, table_name, key_columns)

                # Log the block for data merge with the SQL query inside
                self.logger.log_block("Data Merge", sql_query=merge_sql)

                # Execute the merge and calculate affected rows
                initial_row_count = spark.sql(f"SELECT * FROM {database_name}.{table_name}").count()
                spark.sql(merge_sql)
                final_row_count = spark.sql(f"SELECT * FROM {database_name}.{table_name}").count()
                affected_rows = final_row_count - initial_row_count

                # Log the outcome
                self._log_message(f"Data merged into {database_name}.{table_name} using SQL.")
                self._log_message(f"Number of affected rows: {affected_rows}")

            return affected_rows
        except Exception as e:
            self.logger.log_end("Data Merge Process", success=False, additional_message=f"Error: {e}")
            self._log_message(f"Error during data merge: {e}", level="error")
            raise

    def generate_feedback_timestamps(self, spark: SparkSession, view_name: str, feedback_column: str) -> str:
        """
        Orchestrates the entire process of calculating and returning feedback timestamps.
        """
        # Add a block title to make the output more readable
        self._log_section("Feedback Timestamps Generation", [
            f"View Name: {view_name}",
            f"Feedback Column: {feedback_column}"
        ])

        try:
            # Generate the feedback SQL query
            feedback_sql = self.construct_feedback_sql(view_name, feedback_column)
            
            # Execute the feedback SQL query
            df_min_max = self.execute_feedback_sql(spark, feedback_sql)
            
            # Handle the result and return the feedback output as JSON
            return self.handle_feedback_result(df_min_max, view_name)

        except Exception as e:
            self._log_message(f"Error in feedback timestamps generation: {e}", level="error")
            raise

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
        self.logger.log_sql_query(feedback_sql)
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

    def manage_data_operation(self, 
                              spark: SparkSession, 
                              dbutils, 
                              cleaned_data_view: str, 
                              key_columns: Union[str, List[str]], 
                              use_python: Optional[bool] = False, 
                              destination_folder_path: Optional[str] = None,  
                              destination_environment: Optional[str] = None, 
                              source_datasetidentifier: Optional[str] = None):
        """
        Manage the data operation, allowing optional overriding of default config parameters.
        
        Args:
            spark (SparkSession): The active Spark session.
            dbutils: Databricks utilities object.
            cleaned_data_view (str): The name of the cleaned data view.
            key_columns (Union[str, List[str]]): Columns used for merging data.
            use_python (Optional[bool]): Flag to use Python DataFrame operations or SQL (default is SQL).
            destination_folder_path (Optional[str]): Optional override for destination folder path.
            destination_environment (Optional[str]): Optional override for the destination database/environment.
            source_datasetidentifier (Optional[str]): Optional override for the source dataset/table identifier.
        """
        self.logger.log_start("Manage Data Operation Process")

        try:
            # Normalize the key columns
            key_columns = self.normalize_key_columns(key_columns)

            # Use the provided parameters or fallback to the config values
            destination_folder_path = destination_folder_path or self.destination_folder_path
            destination_environment = destination_environment or self.destination_environment
            source_datasetidentifier = source_datasetidentifier or self.source_datasetidentifier

            # Ensure that required parameters are available
            if not destination_folder_path or not destination_environment or not source_datasetidentifier:
                raise ValueError("The 'destination_folder_path', 'destination_environment', and 'source_datasetidentifier' must be provided either via arguments or configuration.")

            # Log destination details
            self._log_section("Destination Details", [
                f"Destination Path: {destination_folder_path}",
                f"Database: {destination_environment}",
                f"Table: {source_datasetidentifier}"
            ])

            # Path validation and table creation
            self.ensure_path_exists(dbutils, destination_folder_path)
            self.create_or_replace_table(spark, destination_environment, source_datasetidentifier, destination_folder_path, cleaned_data_view, use_python=use_python)

            # Perform the merge operation
            merge_result = self.execute_merge(spark, destination_environment, source_datasetidentifier, cleaned_data_view, key_columns, use_python=use_python)

            # Log merge result
            if merge_result == 0:
                self._log_message("Merge completed, but no new rows were affected.", level="info")
            else:
                self._log_message(f"Merge completed successfully. {merge_result} rows were affected.", level="info")

        except Exception as e:
            self._log_message(f"Error managing data operation: {e}", level="error")
            raise