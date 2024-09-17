# File: custom_utils/logging/logger.py

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
        # Skip logging info messages if debug mode is off
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