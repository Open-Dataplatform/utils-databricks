import logging
import functools
import sqlparse

from pyspark.sql import DataFrame
from pygments import highlight
from pygments.lexers import SqlLexer, PythonLexer
from pygments.formatters import TerminalFormatter

class Logger:
    """
    Custom logger class for enhanced logging functionality, including debugging,
    block logging, SQL query formatting, Python code formatting, and function entry/exit logging.
    """

    def __init__(self, debug: bool = False, log_to_file: str = None):
        """
        Initialize the Logger using Python's built-in logging.

        Args:
            debug (bool): Enable debug-level logging if True.
            log_to_file (str, optional): File path to log messages to a file.
        """
        self.debug = debug
        self.logger = logging.getLogger("custom_logger")

        # Clear existing handlers to prevent duplicate logging
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Set initial logging level
        self.set_level(debug)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('[%(levelname)s] - %(message)s'))
        self.logger.addHandler(console_handler)

        # File handler (if provided)
        if log_to_file:
            file_handler = logging.FileHandler(log_to_file)
            file_handler.setFormatter(logging.Formatter('[%(levelname)s] - %(message)s'))
            self.logger.addHandler(file_handler)

    def set_level(self, debug: bool):
        """Set the logging level based on the debug flag."""
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

    def update_debug_mode(self, debug: bool):
        """Update the debug mode and adjust the logger level."""
        self.debug = debug
        self.set_level(debug)

    def log_message(self, message: str, level: str = "info"):
        """
        Log a message using the specified log level.

        Args:
            message (str): Message to log.
            level (str): Log level ('debug', 'info', 'warning', 'error', 'critical').
        """
        log_function = getattr(self.logger, level, self.logger.info)
        log_function(message)
        if level in {"error", "critical"}:
            raise RuntimeError(message)

    def log_block(self, header, content_lines=None, sql_query=None, level="info"):
        """
        Utility method to log blocks of messages with a header, separators, and optional SQL queries.

        Args:
            header (str): The header text for the block.
            content_lines (list, optional): A list of lines to log within the block.
            sql_query (str, optional): SQL query to log with syntax highlighting.
            level (str): The logging level for the block content.
        """
        log_level = getattr(logging, level.upper(), logging.INFO)
        if not self.logger.isEnabledFor(log_level):
            return

        separator_length = 100
        start_separator = "=" * separator_length
        end_separator = "-" * separator_length
        formatted_header = f" {header} ".center(separator_length, "=")

        print("\n" + start_separator)
        print(formatted_header)
        print(start_separator)

        if content_lines:
            for line in content_lines:
                if line.strip():
                    self.log_message(line, level=level)

        if sql_query:
            self.log_sql_query(sql_query, level=level)

        print(end_separator + "\n")

    def log_sql_query(self, query: str, level: str = "info"):
        """
        Format and log an SQL query with syntax highlighting.

        Args:
            query (str): SQL query string.
            level (str): Log level for the query.
        """
        formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
        highlighted_query = highlight(formatted_query, SqlLexer(), TerminalFormatter())
        self.log_message(f"SQL Query:\n{highlighted_query}", level=level)

    def log_python_code(self, code: str, level: str = "info"):
        """
        Format and log Python code with syntax highlighting.

        Args:
            code (str): Python code string.
            level (str): Log level for the code.
        """
        highlighted_code = highlight(code, PythonLexer(), TerminalFormatter())
        self.log_message(f"Python Code:\n{highlighted_code}", level=level)

    def log_start(self, method_name: str):
        """Log the start of a method."""
        self.log_message(f"Starting {method_name}...", level="info")

    def log_end(self, method_name: str, success: bool = True, additional_message: str = ""):
        """
        Log the end of a method.

        Args:
            method_name (str): Name of the method.
            success (bool): Whether the method completed successfully.
            additional_message (str): Additional message to include.
        """
        status = "successfully" if success else "with warnings or issues"
        self.log_message(f"Finished {method_name} {status}. {additional_message}", level="info" if success else "warning")

    def log_dataframe_summary(self, df: DataFrame, label: str, level="info"):
        """
        Logs summary information of a DataFrame.

        Args:
            df (DataFrame): The DataFrame to summarize.
            label (str): Label for the DataFrame (e.g., 'Initial', 'Flattened').
            level (str): The logging level for the summary.
        """
        log_level = getattr(logging, level.upper(), logging.INFO)
        if not self.logger.isEnabledFor(log_level):
            return

        if df:
            row_count = df.count()
            column_count = len(df.columns)
            estimated_memory = df.rdd.map(lambda row: len(str(row))).sum() / (1024 * 1024)

            # Log the DataFrame info block
            self.log_block(f"{label} DataFrame Info", [
                f"{label} Estimated Memory Usage: {estimated_memory:.2f} MB",
                f"{label} Rows: {row_count}",
                f"{label} Columns: {column_count}",
                f"{label} Schema:"
            ], level=level)
            df.printSchema()

            # Log column types at the specified level
            column_types = [(field.name, field.dataType.simpleString()) for field in df.schema.fields]
            self.log_block(f"{label} Column Types", [
                f"Column: {name}, Type: {dtype}" for name, dtype in column_types
            ], level="debug")

    def log_info(self, message):
        """Convenience method for logging informational messages."""
        self.log_message(message, level="info")

    def log_debug(self, message: str):
        """Log a debug message."""
        self.log_message(message, level="debug")

    def log_warning(self, message: str):
        """Log a warning message."""
        self.log_message(message, level="warning")

    def log_error(self, message: str):
        """Log an error message and raise a RuntimeError."""
        self.log_message(message, level="error")

    def log_critical(self, message: str):
        """Log a critical message and raise a RuntimeError."""
        self.log_message(message, level="critical")

    def log_function_entry_exit(self, func):
        """
        Decorator to log the entry and exit of a function, including arguments and return values.

        Args:
            func (function): Function to wrap with entry/exit logging.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.log_debug(f"Entering {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            self.log_debug(f"Exiting {func.__name__} with result: {result}")
            return result
        return wrapper

class LoggerTester:
    def __init__(self):
        self.logger = Logger(debug=True)

    def run_all_tests(self):
        self.test_log_messages()
        self.test_log_blocks()
        self.test_log_sql_query()
        self.test_log_python_code()
        self.test_function_entry_exit()

    def test_log_messages(self):
        print("Testing individual log messages:")
        self.logger.log_message("This is an informational message.", level="info")
        self.logger.log_debug("This is a debug message for troubleshooting.")
        self.logger.log_warning("This is a warning message.")
        try:
            self.logger.log_error("This is an error message. It will raise an exception.")
        except RuntimeError as e:
            print(f"Caught RuntimeError: {e}")

        try:
            self.logger.log_critical("This is a critical message. It will also raise an exception.")
        except RuntimeError as e:
            print(f"Caught RuntimeError: {e}")

    def test_log_blocks(self):
        print("\nTesting block logging at INFO level:")
        self.logger.log_block("INFO Level Block", [
            "This is an informational message.",
            "Another piece of info."
        ], level="info")

        print("\nTesting block logging at DEBUG level:")
        self.logger.log_block("DEBUG Level Block", [
            "This is a debug message.",
            "Useful for troubleshooting."
        ], level="debug")

    def test_log_sql_query(self):
        print("\nTesting SQL query logging:")
        sql_query = """
        SELECT id, name, age
        FROM users
        WHERE age > 30
        ORDER BY name;
        """
        self.logger.log_sql_query(sql_query)

    def test_log_python_code(self):
        print("\nTesting Python code logging:")
        python_code = """
        def example_function(a, b):
            return a + b

        result = example_function(5, 10)
        print(result)
        """
        self.logger.log_python_code(python_code)

    def test_function_entry_exit(self):
        print("\nTesting function entry/exit logging:")

        @self.logger.log_function_entry_exit
        def test_function(x, y):
            return x + y

        result = test_function(5, 10)
        print(f"Result of test_function: {result}")