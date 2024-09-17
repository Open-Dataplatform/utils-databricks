# File: custom_utils/logging/logger.py

import datetime
import functools

class Logger:
    def __init__(self, debug=False, log_to_file=None):
        """
        Initialize the Logger.

        Args:
            debug (bool): Flag to enable or disable debug logging.
            log_to_file (str): Optional file path to log messages to. If None, logging is only done to the console.
        """
        self.debug = debug
        self.log_to_file = log_to_file

    def _get_log_prefix(self, level):
        """Helper to get the log prefix based on the level and current timestamp."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"[{level.upper()}] {timestamp} - "

    def _write_log(self, message):
        """Helper method to write log messages to a file if log_to_file is set."""
        if self.log_to_file:
            with open(self.log_to_file, 'a') as log_file:
                log_file.write(f"{message}\n")

    def log_message(self, message, level="info", single_info_prefix=False):
        """
        Log a message.

        Args:
            message (str): The message to log.
            level (str): The log level (e.g., 'info', 'warning', 'error', 'debug'). Defaults to 'info'.
            single_info_prefix (bool): If True, only print '[INFO]' once at the start.
        """
        # Skip logging info and debug messages if debug mode is off
        if level in ["info", "debug"] and not self.debug:
            return

        prefix = self._get_log_prefix(level) if not (single_info_prefix and level == "info") else ""
        full_message = f"{prefix}{message}"
        print(full_message)
        self._write_log(full_message)

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

    def log_error(self, message):
        """Log an error message."""
        self.log_message(message, level="error")

    def log_warning(self, message):
        """Log a warning message."""
        self.log_message(message, level="warning")

    def log_critical(self, message):
        """Log a critical message."""
        self.log_message(message, level="critical")

    def log_debug(self, message):
        """Log a debug message."""
        self.log_message(message, level="debug")

    def log_start(self, process_name):
        """
        Log the start of a process.

        Args:
            process_name (str): The name of the process starting.
        """
        self.log_message(f"Starting {process_name}...", level="info")

    def log_end(self, process_name, success=True):
        """
        Log the end of a process.

        Args:
            process_name (str): The name of the process ending.
            success (bool): Whether the process completed successfully.
        """
        status = "successfully" if success else "with errors"
        self.log_message(f"Finished {process_name} {status}.", level="info")

    def log_function_entry_exit(self, func):
        """
        Decorator to log the entry and exit of a function, including arguments and return value.

        Args:
            func (callable): The function to decorate.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.log_debug(f"Entering {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            self.log_debug(f"Exiting {func.__name__} with result: {result}")
            return result
        return wrapper

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