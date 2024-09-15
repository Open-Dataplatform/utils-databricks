# File: custom_utils/catalog/catalog_utils.py

from custom_utils.dp_storage import writer  # Import writer module
from pyspark.sql import SparkSession
from typing import List
from custom_utils.logging.logger import Logger

class DataStorageManager:
    def __init__(self, logger=None, debug=False):
        """
        Initialize the DataStorageManager.
        """
        self.logger = logger if logger else Logger(debug=debug)
        self.debug = debug

    def _log_message(self, message: str, level: str = "info"):
        """
        Logs a message using the logger.
        """
        self.logger.log_message(message, level=level)

    # ========================
    # DESTINATION DETAILS HANDLING
    # ========================

    def get_destination_details(self, spark, destination_environment, source_datasetidentifier):
        """
        Retrieves the destination path, database name, and table name.
        """
        try:
            destination_path = writer.get_destination_path_extended(destination_environment, source_datasetidentifier)
            database_name, table_name = writer.get_databricks_table_info_extended(destination_environment, source_datasetidentifier)
            
            if self.debug:
                self._log_message(f"Destination Path: {destination_path}\nDatabase: {database_name}\nTable: {table_name}")

            return destination_path, database_name, table_name
        except Exception as e:
            self._log_message(f"Error retrieving destination details: {e}", level="error")
            raise

    # ========================
    # DUPLICATE HANDLING AND VALIDATION
    # ========================

    def delete_or_filter_duplicates(self, spark: SparkSession, temp_view_name: str, key_columns: List[str]) -> str:
        """
        Deletes or filters out duplicate records in the temporary view based on key columns.
        """
        key_columns_str = ', '.join(key_columns)
        filtered_view_name = f"{temp_view_name}_deduped"
        dedupe_query = f"""
        WITH ranked_data AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_columns_str} ORDER BY {key_columns_str}) AS row_num
            FROM {temp_view_name}
        )
        SELECT * FROM ranked_data WHERE row_num = 1
        """

        try:
            deduped_df = spark.sql(dedupe_query)
            deduped_df.createOrReplaceTempView(filtered_view_name)
            self._log_message(f"Created new view without duplicates: {filtered_view_name}")
            return filtered_view_name
        except Exception as e:
            self._log_message(f"Error during duplicate filtering: {e}", level="error")
            raise

    def validate_and_create_duplicate_view(self, spark: SparkSession, temp_view_name: str, key_columns: List[str], remove_duplicates: bool = False) -> str:
        """
        Validates the data for duplicates based on the key columns and creates a view if found.
        """
        key_columns_str = ', '.join(key_columns)
        duplicate_keys_query = f"""
        SELECT {key_columns_str}, COUNT(*) AS duplicate_count
        FROM {temp_view_name}
        GROUP BY {key_columns_str}
        HAVING COUNT(*) > 1
        """

        try:
            duplicates_df = spark.sql(duplicate_keys_query)
            if duplicates_df.count() > 0:
                duplicates_view_name = f"view_duplicates_{temp_view_name}"
                duplicates_df.createOrReplaceTempView(duplicates_view_name)
                
                self._log_message(f"Duplicate records found. View created: {duplicates_view_name}")
                display(spark.sql(f"SELECT * FROM {duplicates_view_name} ORDER BY duplicate_count DESC LIMIT 100"))

                if remove_duplicates:
                    self._log_message(f"Duplicates were found and removed. Continuing with the creation.")
                    return self.delete_or_filter_duplicates(spark, temp_view_name, key_columns)
                else:
                    self._log_message(f"Duplicate keys found in {temp_view_name}. Set 'remove_duplicates=True' to automatically remove duplicates.", level="error")
                    raise ValueError(f"Duplicate keys found in {temp_view_name}. Operation aborted.")
            else:
                self._log_message(f"No duplicates found in {temp_view_name}.")
                return temp_view_name
        except Exception as e:
            self._log_message(f"Error validating duplicates: {e}", level="error")
            raise

    # ========================
    # FILE SYSTEM AND TABLE MANAGEMENT
    # ========================

    def ensure_path_exists(self, dbutils, destination_path):
        """
        Ensures the destination path exists in DBFS.
        """
        try:
            dbutils.fs.ls(destination_path)
            self._log_message(f"Path already exists: {destination_path}")
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                dbutils.fs.mkdirs(destination_path)
                self._log_message(f"Path did not exist. Created path: {destination_path}")
            else:
                self._log_message(f"Error while ensuring path exists: {e}", level="error")
                raise

    def recreate_delta_parquet(self, spark, destination_path, temp_view_name):
        """
        Recreates Delta Parquet files from the data in the specified temp view.
        """
        try:
            df = spark.table(temp_view_name)
            self._log_message(f"Recreating Delta Parquet at {destination_path}")
            
            if self.debug:
                df.show()

            df.write.format("delta").mode("overwrite").save(destination_path)
            self._log_message(f"Delta Parquet files written to: {destination_path}")
        except Exception as e:
            self._log_message(f"Error recreating Delta Parquet: {e}", level="error")
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

            self._log_message(f"Creating table with query:\n{create_table_sql.strip()}")

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

    # ========================
    # MERGE OPERATIONS
    # ========================

    def generate_merge_sql(self, spark, temp_view_name: str, database_name: str, table_name: str, key_columns: List[str]):
        """
        Constructs the SQL query for the MERGE operation, ensuring only existing columns in the target table are used.

        Args:
            spark (SparkSession): The active Spark session.
            temp_view_name (str): Name of the temporary view containing source data.
            database_name (str): Name of the target database.
            table_name (str): Name of the target table.
            key_columns (list): List of key columns for matching.

        Returns:
            str: The MERGE SQL query.
        """
        # Get the list of columns from the target table
        target_table_columns = [field.name for field in spark.table(f"{database_name}.{table_name}").schema]

        # Fetch all columns from the temporary view, excluding key_columns and 'CreatedDate'
        all_columns = [
            col for col in spark.table(temp_view_name).columns 
            if col not in key_columns and col != 'CreatedDate' and col in target_table_columns
        ]

        # Create the match condition based on the key columns
        match_sql = ' AND '.join([f"s.{col.strip()} = t.{col.strip()}" for col in key_columns])

        # Create the update statement by specifying each column explicitly, ensuring they exist in the target table
        update_sql = ', '.join([f"t.{col.strip()} = s.{col.strip()}" for col in all_columns])

        # Create the insert statement by specifying the columns and their values, including 'CreatedDate' for insert
        insert_columns = ', '.join([f"{col.strip()}" for col in key_columns + all_columns + ['CreatedDate'] if col in target_table_columns])
        insert_values = ', '.join([f"s.{col.strip()}" for col in key_columns + all_columns + ['CreatedDate'] if col in target_table_columns])

        # Generate the full MERGE SQL statement
        merge_sql = f"""
        MERGE INTO {database_name}.{table_name} AS t
        USING {temp_view_name} AS s
        ON {match_sql}
        WHEN MATCHED THEN
        UPDATE SET {update_sql}
        WHEN NOT MATCHED THEN
        INSERT ({insert_columns}) VALUES ({insert_values})
        """

        self._log_message(f"MERGE SQL:\n{merge_sql.strip()}")
        return merge_sql
    
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
    # MAIN OPERATIONS
    # ========================

    def manage_data_operation(self, spark, dbutils, destination_environment, source_datasetidentifier, temp_view_name, key_columns: List[str], operation_type="create", remove_duplicates=False):
        """
        Main method to manage table creation or merge operations.
        """
        try:
            destination_path, database_name, table_name = self.get_destination_details(spark, destination_environment, source_datasetidentifier)
            self.ensure_path_exists(dbutils, destination_path)
            temp_view_name = self.validate_and_create_duplicate_view(spark, temp_view_name, key_columns, remove_duplicates)

            if operation_type == "create":
                self.create_or_replace_table(spark, database_name, table_name, destination_path, temp_view_name)
            elif operation_type == "merge":
                merge_sql = self.generate_merge_sql(spark, temp_view_name, database_name, table_name, key_columns)
                self.execute_merge(spark, database_name, table_name, merge_sql)
            else:
                raise ValueError(f"Invalid operation type: {operation_type}. Supported types are 'create' and 'merge'.")

        except Exception as e:
            self._log_message(f"Error managing data operation: {e}", level="error")
            raise