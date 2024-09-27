import logging
import datetime
import functools

class Logger:
    def __init__(self, debug=False, log_to_file=None):
        """
        Initialize the Logger using Python's built-in logging.
        """
        self.debug = debug
        self.logger = logging.getLogger("custom_logger")
        self.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if self.debug else logging.INFO)
        
        # Formatter for logs
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        self.logger.addHandler(console_handler)

        # File handler (if provided)
        if log_to_file:
            file_handler = logging.FileHandler(log_to_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def log_message(self, message, level="info"):
        """
        Log a message using Python's logging module.
        """
        if level == "debug":
            self.logger.debug(message)
        elif level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
        elif level == "critical":
            self.logger.critical(message)

    def log_block(self, header, content_lines, level="info"):
        """
        Utility method to log blocks of messages with a header and separators.
        """
        separator = "=" * 30
        self.log_message(f"\n{separator}\n=== {header} ===\n{separator}", level=level)
        
        for line in content_lines:
            self.log_message(line, level=level)
        
        self.log_message(separator, level=level)

    def log_start(self, method_name):
        """Log the start of a method."""
        self.log_message(f"Starting {method_name}...", level="info")

    def log_end(self, method_name, success=True, additional_message=""):
        """
        Log the end of a method.
        """
        status = "successfully" if success else "with errors"
        end_message = f"Finished {method_name} {status}. {additional_message}"
        self.log_message(end_message, level="info")

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
        """
        self.log_error(message)
        if dbutils:
            dbutils.notebook.exit(f"[ERROR] {message}")
        else:
            raise SystemExit(f"[ERROR] {message}")

    def log_function_entry_exit(self, func):
        """
        Decorator to log the entry and exit of a function, including arguments and return value.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.log_debug(f"Entering {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            self.log_debug(f"Exiting {func.__name__} with result: {result}")
            return result
        return wrapper