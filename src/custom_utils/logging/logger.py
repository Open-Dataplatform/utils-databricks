# File: custom_utils/logging/logger.py

class Logger:
    def __init__(self, debug=False):
        """
        Initialize the Logger.

        Args:
            debug (bool): Flag to enable or disable debug logging.
        """
        self.debug = debug

    def log(self, message, level="info", single_info_prefix=False):
        """
        Log a message.

        Args:
            message (str): The message to log.
            level (str): The log level (e.g., 'info', 'warning', 'error'). Defaults to 'info'.
            single_info_prefix (bool): If True, only print '[INFO]' once at the start.
        """
        if level == "info" and not self.debug:
            return

        if single_info_prefix and level == "info":
            print("[INFO]")
            print(message)
        else:
            print(f"[{level.upper()}] {message}")

    def log_path_validation(self, schema_file_path, source_directory_path, number_of_files):
        """
        Log the results of the path validation.

        Args:
            schema_file_path (str): The path to the schema file.
            source_directory_path (str): The path to the source directory.
            number_of_files (int): The number of files found in the source directory.
        """
        self.log("\n=== Path Validation Results ===", level="info", single_info_prefix=True)
        print("------------------------------")
        print(f"Schema directory path: {schema_file_path}")
        print(f"Source directory path: {source_directory_path}")
        print(f"Expected minimum files: 1")
        print(f"Actual number of files found: {number_of_files}")
        print("------------------------------")

    def log_file_validation(self, matched_files, file_type, source_filename):
        """
        Log the results of the file validation.

        Args:
            matched_files (list): List of files that match the source filename pattern.
            file_type (str): The type of the schema file (e.g., 'json' or 'xml').
            source_filename (str): The filename pattern used for matching.
        """
        self.log("\n=== File Validation Results ===", level="info", single_info_prefix=True)
        print("------------------------------")
        print(f"File Type: {file_type}")
        
        num_files = len(matched_files)
        
        # Show the total number of files found, with a note if displaying only the top 10
        if num_files > 10:
            print(f"Number of files found: {num_files} (showing top 10)")
            files_to_display = matched_files[:10]
        else:
            print(f"Number of files found: {num_files}")
            files_to_display = matched_files

        # Always print the list of matching files, regardless of the source_filename pattern
        print(f"Files found matching the pattern '{source_filename}':")
        for file in files_to_display:
            print(f"- {file}")
        
        print("------------------------------")

    def exit_notebook(self, message, dbutils=None):
        """
        Exit the notebook with an error message.

        Args:
            message (str): The error message to display.
            dbutils (object, optional): Databricks dbutils object for notebook exit.
        """
        if dbutils:
            dbutils.notebook.exit(f"[ERROR] {message}")
        else:
            raise SystemExit(f"[ERROR] {message}")