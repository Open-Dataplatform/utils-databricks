# File: custom_utils/logging/logger.py

import time  # To track execution time

class Logger:
    def __init__(self, debug=False):
        """
        Initialize the Logger.

        Args:
            debug (bool): Flag to enable or disable debug logging.
        """
        self.debug = debug

    def log_message(self, message, level="info", single_info_prefix=False):
        """
        Log a message.

        Args:
            message (str): The message to log.
            level (str): The log level (e.g., 'info', 'warning', 'error'). Defaults to 'info'.
            single_info_prefix (bool): If True, only print '[INFO]' once at the start.
        """
        if level == "info" and not self.debug:
            return

        prefix = f"[{level.upper()}] " if not (single_info_prefix and level == "info") else ""
        print(f"{prefix}{message}")

    def log_block(self, header, content_lines, level="info"):
        """
        Utility method to log blocks of messages with a header and separators.

        Args:
            header (str): Header of the block.
            content_lines (list): List of lines to include in the block.
            level (str): Log level for the block.
        """
        self.log_message(f"\n=== {header} ===", level=level, single_info_prefix=True)
        print("------------------------------")
        for line in content_lines:
            self.log_message(line, level=level)
        print("------------------------------")

    def log_final_status(self, process_name, total_rows, output_path=None, execution_time=None):
        """
        Log the final status of the process.

        Args:
            process_name (str): The name of the process that was completed.
            total_rows (int): The total number of rows processed.
            output_path (str, optional): The path where the final output was saved. Defaults to None.
            execution_time (float, optional): Total time taken for the process in seconds. Defaults to None.
        """
        content_lines = [
            f"Process '{process_name}' completed successfully.",
            f"Total rows processed: {total_rows}"
        ]

        if output_path:
            content_lines.append(f"Output saved to: {output_path}")

        if execution_time:
            content_lines.append(f"Total execution time: {execution_time:.2f} seconds")

        self.log_block("Final Status", content_lines)

    def log_path_validation(self, schema_directory_path, source_directory_path, number_of_files):
        """
        Log the results of the path validation.

        Args:
            schema_directory_path (str): The path to the schema directory.
            source_directory_path (str): The path to the source directory.
            number_of_files (int): The number of files found in the source directory.
        """
        schema_directory_path = schema_directory_path if not schema_directory_path.startswith('/dbfs/') else schema_directory_path[5:]

        content_lines = [
            f"Schema directory path: {schema_directory_path}",
            f"Source directory path: {source_directory_path}",
            "Expected minimum files: 1",
            f"Actual number of files found: {number_of_files}"
        ]
        self.log_block("Path Validation Results", content_lines)

    def log_file_validation(self, schema_file_name, matched_files, file_type, source_filename):
        """
        Log the results of the file validation.

        Args:
            schema_file_name (str): The name of the schema file.
            matched_files (list): List of files that match the source filename pattern.
            file_type (str): The type of the schema file (e.g., 'json' or 'xml').
            source_filename (str): The filename pattern used for matching.
        """
        num_files = len(matched_files)
        files_to_display = matched_files[:10] if num_files > 10 else matched_files
        content_lines = [
            f"File Type: {file_type}",
            f"Schema file name: {schema_file_name}",
            f"Number of files found: {num_files} {'(showing top 10)' if num_files > 10 else ''}",
            f"Files found matching the pattern '{source_filename}':"
        ] + [f"- {file}" for file in files_to_display]

        self.log_block("File Validation Results", content_lines)

    def log_error(self, message):
        """
        Log an error message.

        Args:
            message (str): The error message to log.
        """
        self.log_message(message, level="error")

    def exit_notebook(self, message, dbutils=None):
        """
        Exit the notebook with an error message.

        Args:
            message (str): The error message to display.
            dbutils (object, optional): Databricks dbutils object for notebook exit.
        """
        self.log_error(message)
        if dbutils:
            dbutils.notebook.exit(f"[ERROR] {message}")
        else:
            raise SystemExit(f"[ERROR] {message}")