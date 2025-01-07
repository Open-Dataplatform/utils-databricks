from custom_utils.dp_storage import writer
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional, Union
from custom_utils.logging.logger import Logger

class DataStorageManager:
    def __init__(self, 
                 logger: Optional[Logger] = None, 
                 debug: bool = False, 
                 destination_environment: Optional[str] = None, 
                 source_datasetidentifier: Optional[str] = None, 
                 destination_folder_path: Optional[str] = None):
        """
        Initialize the DataStorageManager with optional logger, debug mode, and configuration attributes.
        """
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug
        
        # Default/fallback config values
        self.destination_environment = destination_environment
        self.source_datasetidentifier = source_datasetidentifier
        self.destination_folder_path = destination_folder_path
        
        # Track logged sections to avoid redundant log entries
        #self.sections_logged = set()

    def ensure_path_exists(self, dbutils, destination_path: str):
        """
        Ensures that a path exists in DBFS. If it does not, the path is created.
        """
        try:
            dbutils.fs.ls(destination_path)
            self.logger.log_block("Path Validation", [f"Path already exists: {destination_path}"])
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                dbutils.fs.mkdirs(destination_path)
                self.logger.log_block("Path Validation", [f"Path did not exist. Created path: {destination_path}"])
            else:
                self.logger.log_error(f"Error while ensuring path exists: {e}")
                raise RuntimeError(f"Error while ensuring path exists: {e}")

    def normalize_key_columns(self, key_columns: Union[str, List[str]]) -> List[str]:
        """
        Normalizes the key columns by converting a comma-separated string into a list of column names.
        """
        if isinstance(key_columns, str):
            key_columns = [col.strip() for col in key_columns.split(',')]
        return key_columns
    
    def check_if_table_exists(self, spark: SparkSession, database_name: str, table_name: str) -> bool:
        """
        Checks if a Delta table exists in the specified database.
        """
        try:
            tables_df = spark.sql(f"SHOW TABLES IN {database_name}").filter(f"tableName = '{table_name}'")
            table_exists = tables_df.count() > 0
            return table_exists
        except Exception as e:
            self.logger.log_error(f"Error checking table existence: {e}")
            raise RuntimeError(f"Error checking table existence: {e}")

    def create_or_replace_table(self, spark: SparkSession, database_name: str, table_name: str, destination_path: str, cleaned_data_view: str, use_python: Optional[bool] = False):
        """
        Creates or replaces a Delta table at the specified path, optionally using Python DataFrame operations.
        """
        try:
            # Access DataFrame from the specified view
            df = spark.table(cleaned_data_view)

            # Construct schema string for readability in SQL
            schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])

            # Create SQL query for table creation
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                {schema_str}
            )
            USING DELTA
            LOCATION '{destination_path}/'
            """

            # Log the SQL query used for table creation
            self.logger.log_block("Table Creation Process", sql_query=create_table_sql)

            if use_python:
                # Log DataFrame operation for table creation
                self.logger.log_sql_query("Table Creation Process", [f"Writing DataFrame to Delta at path '{destination_path}' and registering table '{database_name}.{table_name}'."])
                
                # Write DataFrame to Delta format and register the table
                df.write.format("delta").mode("overwrite").option("path", destination_path).saveAsTable(f"{database_name}.{table_name}")
                
                self.logger.log_message(f"Table {database_name}.{table_name} created using DataFrame operations.")
            else:
                # Check if table already exists before creation
                if not self.check_if_table_exists(spark, database_name, table_name):
                    spark.sql(create_table_sql)
                    self.logger.log_message(f"Table {database_name}.{table_name} created.")
                    
                    # Write data to the Delta table
                    df.write.format("delta").mode("overwrite").save(destination_path)
                    self.logger.log_message(f"Data written to {destination_path}.")
                else:
                    self.logger.log_message(f"Table {database_name}.{table_name} already exists.")
        except Exception as e:
            self.logger.log_error(f"Error creating or replacing table: {e}")
            raise RuntimeError(f"Error creating or replacing table: {e}")

    def generate_merge_sql(self, spark: SparkSession, cleaned_data_view: str, database_name: str, table_name: str, key_columns: Union[str, List[str]]) -> str:
        """
        Constructs the SQL query for a Delta MERGE operation, using specified key columns.
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
            self.logger.log_error(f"Error generating merge SQL: {e}")
            raise RuntimeError(f"Error generating merge SQL: {e}")

    def execute_merge(self, spark: SparkSession, database_name: str, table_name: str, cleaned_data_view: str, key_columns: Union[str, List[str]], use_python: Optional[bool] = False) -> int:
        """
        Executes the MERGE SQL operation and logs the SQL query and results.
        """
        try:
            key_columns = self.normalize_key_columns(key_columns)

            if use_python:
                raise NotImplementedError("Python-based merge is not yet implemented.")
            else:
                # Generate the merge SQL query
                merge_sql = self.generate_merge_sql(spark, cleaned_data_view, database_name, table_name, key_columns)
                self.logger.log_block("Executing Data Merge", sql_query=merge_sql)

                # Execute the merge operation and capture the results
                merge_result = spark.sql(merge_sql)

                # Extract merge statistics
                merge_stats = merge_result.collect()[0].asDict()
                deleted_count = merge_stats.get("num_deleted_rows", 0)
                updated_count = merge_stats.get("num_updated_rows", 0)
                inserted_count = merge_stats.get("num_inserted_rows", 0)

                # Log the results
                self.logger.log_block("Merge Summary", [
                    f"Deleted Records: {deleted_count}",
                    f"Updated Records: {updated_count}",
                    f"Inserted Records: {inserted_count}",
                ])
                total_affected_rows = deleted_count + updated_count + inserted_count
                self.logger.log_message(f"Total number of affected rows: {total_affected_rows}")

                return total_affected_rows
        except Exception as e:
            self.logger.log_end("Data Merge Process", success=False, additional_message=f"Error: {e}")
            self.logger.log_error(f"Error during data merge: {e}")
            raise RuntimeError(f"Error during data merge: {e}")

    def generate_feedback_timestamps(self, spark: SparkSession, view_name: str, feedback_column: Optional[str] = None, key_columns: Optional[Union[str, List[str]]] = None) -> str:
        """
        Calculates and returns feedback timestamps by either `feedback_column` or the first column in `key_columns`.
        """
        # Use feedback_column as key_columns if it contains a comma
        if feedback_column and "," in feedback_column:
            key_columns = self.normalize_key_columns(feedback_column)
            feedback_column = None
        
        # Initialize key_columns as empty list if not defined
        if key_columns is None:
            key_columns = []
        else:
            key_columns = self.normalize_key_columns(key_columns)

        # Use feedback_column if defined, otherwise use the first key_columns element
        column_to_use = feedback_column.strip() if feedback_column else (key_columns[0] if key_columns else None)
        
        if not column_to_use:
            raise ValueError("Either 'feedback_column' or 'key_columns' must be provided and non-empty.")
        
        # Log section to confirm the chosen column for the operation
        self.logger.log_block("Feedback Timestamps Generation", [
            f"View Name: {view_name}",
            f"Using Column: {column_to_use}"
        ])

        try:
            feedback_sql = self.construct_feedback_sql(view_name, column_to_use)
            df_min_max = spark.sql(feedback_sql)
            return self.handle_feedback_result(df_min_max, view_name)

        except Exception as e:
            error_message = f"Error in feedback timestamps generation: {e}"
            self.logger.log_error(error_message)
            raise RuntimeError(error_message)

    def execute_feedback_sql(self, spark: SparkSession, feedback_sql: str) -> DataFrame:
        """
        Executes a SQL query to retrieve the minimum and maximum timestamps.
        """
        try:
            return spark.sql(feedback_sql)
        except Exception as e:
            error_message = f"Error executing SQL query: {e}"
            self.logger.log_error(error_message)
            raise RuntimeError(error_message)

    def handle_feedback_result(self, df_min_max: DataFrame, view_name: str) -> str:
        """
        Processes the result of the feedback SQL query and converts it to JSON format.
        """
        if df_min_max.head(1):
            notebook_output = df_min_max.toJSON().first()
            self.logger.log_message(f"Notebook output: {notebook_output}")
            return notebook_output
        else:
            error_message = f"No data found in {view_name} to calculate the feedback timestamps."
            self.logger.log_error(error_message)
            raise ValueError(error_message)

    def construct_feedback_sql(self, view_name: str, feedback_column: str) -> str:
        """
        Creates an SQL query to calculate the minimum and maximum timestamps for the specified column.
        """
        feedback_sql = f"""
        SELECT
            MIN({feedback_column}) AS from_datetime,
            MAX({feedback_column}) AS to_datetime
        FROM {view_name}
        """
        self.logger.log_sql_query(feedback_sql)
        return feedback_sql

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
            self.logger.log_block("Destination Details", [
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
                self.logger.log_message("Merge completed, but no new rows were affected.")
            else:
                self.logger.log_message(f"Merge completed successfully. {merge_result} rows were affected.")

        except Exception as e:
            self.logger.log_error(f"Error managing data operation: {e}")
            raise RuntimeError(f"Error managing data operation: {e}")