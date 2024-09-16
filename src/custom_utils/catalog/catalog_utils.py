from custom_utils.dp_storage import writer  # Import writer module
from pyspark.sql import SparkSession, DataFrame
from typing import List
from custom_utils.logging.logger import Logger

class DataStorageManager:
    def __init__(self, logger=None, debug=False):
        """
        Initialize the DataStorageManager.
        """
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug
        self.sections_logged = set()  # Track logged sections to avoid duplicates

    def _log_message(self, message, level="info"):
        """
        Logs a message using the logger.
        
        Args:
            message (str): The message to log.
            level (str): The log level (e.g., 'info', 'warning', 'error'). Defaults to 'info'.
        """
        self.logger.log_message(message, level=level)

    def _log_section(self, section_title: str, content_lines: list = None):
        """
        Logs a section with the provided title and optional content lines using `log_block`.
        
        Args:
            section_title (str): The title of the section to log.
            content_lines (list, optional): Lines of content to log under the section.
        """
        if section_title not in self.sections_logged:
            content_lines = content_lines if content_lines else []
            self.logger.log_block(section_title, content_lines)
            self.sections_logged.add(section_title)

    # ========================
    # Feedback Management Methods (Merged from feedback_management.py)
    # ========================

    def construct_feedback_sql(self, view_name: str, feedback_column: str) -> str:
        """
        Constructs an SQL query to get the minimum and maximum timestamps for the feedback column.

        Args:
            view_name (str): The name of the view containing the new data.
            feedback_column (str): The name of the feedback timestamp column.

        Returns:
            str: The constructed SQL query to fetch the minimum and maximum feedback timestamps.
        """
        feedback_sql = f"""
        SELECT
            MIN({feedback_column}) AS from_datetime,
            MAX({feedback_column}) AS to_datetime
        FROM {view_name}
        """
        self._log_message(f"Executing SQL query: {feedback_sql}")
        return feedback_sql

    def execute_feedback_sql(self, spark: SparkSession, feedback_sql: str) -> DataFrame:
        """
        Executes the SQL query to get the minimum and maximum timestamps.

        Args:
            spark (SparkSession): The active Spark session.
            feedback_sql (str): The SQL query to execute.

        Returns:
            DataFrame: A DataFrame containing the result of the SQL query.

        Raises:
            Exception: If the SQL query fails.
        """
        try:
            return spark.sql(feedback_sql)
        except Exception as e:
            self._log_message(f"Error executing SQL query: {e}", level="error")
            raise

    def handle_feedback_result(self, df_min_max: DataFrame, view_name: str) -> str:
        """
        Handles the result of the feedback SQL query and converts it to JSON.

        Args:
            df_min_max (DataFrame): The DataFrame containing the minimum and maximum timestamps.
            view_name (str): The name of the view for logging purposes.

        Returns:
            str: The JSON string output to be used for exiting the notebook.

        Raises:
            ValueError: If no data is found in the DataFrame.
        """
        if df_min_max.head(1):  # Efficient way to check if the DataFrame is empty
            notebook_output = df_min_max.toJSON().first()
            self._log_message(f"Notebook output: {notebook_output}")
            return notebook_output
        else:
            error_message = f"No data found in {view_name} to calculate the feedback timestamps."
            self._log_message(error_message, level="error")
            raise ValueError(error_message)

    def generate_feedback_timestamps(self, spark: SparkSession, view_name: str, feedback_column: str) -> str:
        """
        Orchestrates the entire process of calculating and returning feedback timestamps.

        Args:
            spark (SparkSession): The active Spark session.
            view_name (str): The name of the view containing the new data.
            feedback_column (str): The name of the feedback timestamp column.

        Returns:
            str: The JSON string output containing the feedback timestamps.
        """
        # Generate the feedback SQL query
        feedback_sql = self.construct_feedback_sql(view_name, feedback_column)

        # Execute the feedback SQL query
        df_min_max = self.execute_feedback_sql(spark, feedback_sql)

        # Handle the result and return the feedback output as JSON
        return self.handle_feedback_result(df_min_max, view_name)

    # ========================
    # Destination Details Handling
    # ========================
    
    def get_destination_details(self, spark, destination_environment, source_datasetidentifier):
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

    # ========================
    # File System and Table Management
    # ========================

    def ensure_path_exists(self, dbutils, destination_path):
        """
        Ensures the destination path exists in DBFS.
        """
        try:
            content_lines = []
            try:
                dbutils.fs.ls(destination_path)
                content_lines.append(f"Path already exists: {destination_path}")
            except Exception as e:
                if "java.io.FileNotFoundException" in str(e):
                    dbutils.fs.mkdirs(destination_path)
                    content_lines.append(f"Path did not exist. Created path: {destination_path}")
                else:
                    raise
            self._log_section("Path Validation", content_lines)
        except Exception as e:
            self._log_message(f"Error while ensuring path exists: {e}", level="error")
            raise

    def create_or_replace_table(self, spark, database_name, table_name, destination_path, temp_view_name):
        """
        Creates a Databricks Delta table if it does not exist and writes data to it.
        """
        try:
            df = spark.table(temp_view_name)
            schema_str = ",\n    ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields])

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                {schema_str}
            )
            USING DELTA
            LOCATION 'dbfs:{destination_path}/'
            """

            self._log_section("Table Creation Process", [f"Creating table with query:\n{create_table_sql.strip()}"])

            if not self.check_if_table_exists(spark, database_name, table_name):
                spark.sql(create_table_sql)
                self._log_message(f"Table {database_name}.{table_name} created.")
                df.write.format("delta").mode("overwrite").save(destination_path)
                self._log_message(f"Data written to {destination_path}.")
            else:
                self._log_message(f"Table {database_name}.{table_name} already exists.")
        except Exception as e:
            self._log_message(f"Error creating or replacing table: {e}", level="error")
            raise

    def check_if_table_exists(self, spark, database_name, table_name) -> bool:
        """
        Checks if a Databricks Delta table exists in the specified database.
        """
        try:
            table_check = spark.sql(f"SHOW TABLES IN {database_name}").collect()
            table_exists = any(row["tableName"] == table_name for row in table_check)

            if not table_exists:
                self._log_message(f"Table {database_name}.{table_name} does not exist.")
            return table_exists
        except Exception as e:
            self._log_message(f"Error checking table existence: {e}", level="error")
            raise

    # ========================
    # Merge Operations
    # ========================

    def generate_merge_sql(self, spark, temp_view_name: str, database_name: str, table_name: str, key_columns):
        """
        Constructs the SQL query for the MERGE operation, ensuring only existing columns in the target table are used.
        """
        try:
            if isinstance(key_columns, str):
                key_columns = [key_columns]
            elif not isinstance(key_columns, list) or not all(isinstance(col, str) for col in key_columns):
                raise TypeError("key_columns must be a string or a list of strings.")

            target_table_columns = [field.name for field in spark.table(f"{database_name}.{table_name}").schema]
            all_columns = [
                col for col in spark.table(temp_view_name).columns 
                if col not in key_columns and col in target_table_columns
            ]
            insert_columns = key_columns + all_columns
            if 'CreatedDate' in target_table_columns:
                insert_columns.append('CreatedDate')
            insert_values = [f"s.`{col.strip()}`" for col in insert_columns]
            match_sql = ' AND '.join([f"s.`{col.strip()}` = t.`{col.strip()}`" for col in key_columns])
            update_sql = ', '.join([f"t.`{col.strip()}` = s.`{col.strip()}`" for col in all_columns])

            merge_sql = f"""
            MERGE INTO {database_name}.{table_name} AS t
            USING {temp_view_name} AS s
            ON {match_sql}
            WHEN MATCHED THEN
                UPDATE SET {update_sql}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join([f'`{col.strip()}`' for col in insert_columns])}) 
                VALUES ({', '.join(insert_values)})
            """

            self._log_section("Data Merge", [f"MERGE SQL:\n{merge_sql.strip()}"])
            return merge_sql
        except Exception as e:
            self._log_message(f"Error generating merge SQL: {e}", level="error")
            raise

    def execute_merge(self, spark, database_name: str, table_name: str, merge_sql: str):
        """
        Executes the MERGE SQL statement.
        """
        try:
            spark.sql(merge_sql)
            self._log_message(f"Data merged into {database_name}.{table_name}.")
        except Exception as e:
            self._log_message(f"Error during data merge: {e}", level="error")
            raise

    # ========================
    # Main Operations
    # ========================

    def manage_data_operation(self, spark, dbutils, destination_environment, source_datasetidentifier, temp_view_name, key_columns: List[str]):
        """
        Main method to manage both table creation and data merge operations.
        """
        try:
            destination_path, database_name, table_name = self.get_destination_details(spark, destination_environment, source_datasetidentifier)
            self.ensure_path_exists(dbutils, destination_path)
            self.create_or_replace_table(spark, database_name, table_name, destination_path, temp_view_name)
            merge_sql = self.generate_merge_sql(spark, temp_view_name, database_name, table_name, key_columns)
            self.execute_merge(spark, database_name, table_name, merge_sql)
        except Exception as e:
            self._log_message(f"Error managing data operation: {e}", level="error")
            raise