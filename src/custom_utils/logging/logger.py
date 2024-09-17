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

    def _write_log(self, message):
        """Helper method to write log messages to a file if log_to_file is set."""
        if self.log_to_file:
            with open(self.log_to_file, 'a') as log_file:
                log_file.write(f"{message}\n")

    def log_message(self, message, level="info", single_info_prefix=False, include_timestamp=False):
        """
        Log a message.

        Args:
            message (str): The message to log.
            level (str): The log level (e.g., 'info', 'warning', 'error', 'debug'). Defaults to 'info'.
            single_info_prefix (bool): If True, only print '[INFO]' once at the start.
            include_timestamp (bool): If True, includes a timestamp in the log message.
        """
        # Skip logging info and debug messages if debug mode is off
        if level in ["info", "debug"] and not self.debug:
            return

        # Avoid logging empty messages
        if not message.strip():
            return

        # Construct the log prefix
        prefix = f"[{level.upper()}] " if not (single_info_prefix and level == "info") else "[INFO] "
        timestamp = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - " if include_timestamp else ""
        full_message = f"{prefix}{timestamp}{message}".strip()
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
        # Directly print the block header without using log_message to avoid the prefix
        print(f"\n=== {header} ===")
        self._write_log(f"=== {header} ===")

        # Print separator
        print("------------------------------")

        # Log each content line
        for line in content_lines:
            if line.strip():
                self.log_message(f"{line}", level=level, single_info_prefix=False)

        # End with a separator
        print("------------------------------")

    def log_start(self, method_name):
        """Log the start of a method, including a timestamp."""
        self.log_message(f"Starting {method_name}...", include_timestamp=True)

    def log_end(self, method_name, success=True, additional_message=""):
        """
        Log the end of a method, including a timestamp.

        Args:
            method_name (str): The name of the method.
            success (bool): Whether the method completed successfully.
            additional_message (str): Additional message to log.
        """
        status = "successfully" if success else "with errors"
        end_message = f"Finished {method_name} {status}. {additional_message}"
        self.log_message(end_message, include_timestamp=True)

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