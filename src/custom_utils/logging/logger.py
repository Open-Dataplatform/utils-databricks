import logging
import functools
import sqlparse
import sys

from pyspark.sql import DataFrame
from pygments import highlight
from pygments.lexers import SqlLexer, PythonLexer
from pygments.formatters import TerminalFormatter

class Logger:
    """
    Custom logger with structured block logging, syntax highlighting, and improved readability.
    """
    ICONS = {
        "debug": "🐞",   # Debugging
        "info": "ℹ️",    # Information
        "warning": "⚠️",  # Warning
        "error": "❌",    # Error
        "critical": "🔥"  # Critical issue
    }

    def __init__(self, debug: bool = False, log_to_file: str = None):
        """
        Initialize the Logger with an improved format to prevent duplicate log levels.

        Args:
            debug (bool): Enable debug-level logging if True.
            log_to_file (str, optional): File path to log messages to a file.
        """
        self.logger = logging.getLogger("custom_logger")
        self.logger.propagate = False  # Prevents duplicate logs in Databricks

        # ✅ Set debug mode immediately
        self.debug = debug
        self.set_level(debug)  # ✅ Ensure the correct log level is set

        if not self.logger.hasHandlers():
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter('%(message)s')  
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)

            if log_to_file:
                file_handler = logging.FileHandler(log_to_file)
                file_handler.setFormatter(console_formatter)
                self.logger.addHandler(file_handler)

        # ✅ Prevent duplicate initialization messages
        if not hasattr(self, "_initialized"):
            self.log_info(f"🔄 Logger initialized with debug={self.debug}")
            self._initialized = True  # ✅ Flag to prevent duplicate messages
                
    def set_level(self, debug: bool):
        """Set logging level based on debug flag."""
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        self.debug = debug  # ✅ Ensure self.debug is properly updated
        
    def update_debug_mode(self, debug: bool, log_update: bool = True):
        """
        Dynamically updates the debug mode and adjusts the logger level accordingly.

        Args:
            debug (bool): If True, enables debug-level logging. Otherwise, sets it to info-level.
            log_update (bool): If True, logs the update message (set to False inside __init__()).
        """
        self.debug = debug
        self.set_level(debug)  # ✅ Adjust log level dynamically

        # ✅ Log only if explicitly requested (to prevent duplicate logs in __init__()).
        if log_update:
            self.log_info(f"🔄 Debug mode updated. New debug state: {self.debug}")

    def log_header(self, title: str):
        """Logs a structured header section."""
        separator = "=" * 100
        formatted_title = f" {title} ".center(100, "=")
        print(f"\n{separator}\n{formatted_title}\n{separator}")

    def log_footer(self):
        """Logs a structured footer section."""
        print("-" * 100)

    def log_message(self, message: str, level: str = "info"):
        """
        Log a message with an icon prefix.

        Args:
            message (str): The message to log.
            level (str): Log level ('debug', 'info', 'warning', 'error', 'critical').
        """
        if level == "debug" and not self.debug:
            return  # ✅ Ignore debug logs if debug mode is disabled

        icon = self.ICONS.get(level, "ℹ️")  # Default to info icon
        formatted_message = f"{icon} [{level.upper()}] - {message}"

        log_function = getattr(self.logger, level, self.logger.info)
        log_function(formatted_message)

        if level in {"error", "critical"}:
            raise RuntimeError(message)

    def log_block(self, header, content_lines=None, sql_query=None, python_query=None, level="info"):
        """
        Logs structured blocks for readability while preventing log reordering in Databricks.

        Args:
            header (str): Title of the block.
            content_lines (list, optional): Lines of text inside the block.
            sql_query (str, optional): SQL query to be formatted and highlighted.
            python_query (str, optional): Python query to be formatted and highlighted.
            level (str): Log level ('debug', 'info', 'warning', 'error', 'critical').
        """
        if level == "debug" and not self.debug:
            return  # ✅ Ignore debug logs if debug mode is disabled

        separator_length = 100
        start_separator = "=" * separator_length
        formatted_header = f" {header} ".center(separator_length, "=")
        end_separator = "-" * separator_length

        output_lines = [
            "\n" + start_separator,
            formatted_header,
            start_separator,
        ]

        # ✅ Ensure content lines include both icon and log level
        if content_lines:
            output_lines.extend([
                f"{self.ICONS.get(level, 'ℹ️')} [{level.upper()}] - {line}"
                for line in content_lines
            ])

        # ✅ Log SQL query with formatting
        if sql_query:
            formatted_query = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
            highlighted_query = highlight(formatted_query, SqlLexer(), TerminalFormatter())

            output_lines.append("\n" + "-" * 100)
            output_lines.append("📜 SQL Code")
            output_lines.append("-" * 100)
            output_lines.append(f"\n{highlighted_query.strip()}\n")
            #output_lines.append(end_separator)

        # ✅ Log Python query with formatting
        if python_query:
            highlighted_code = highlight(python_query, PythonLexer(), TerminalFormatter())

            output_lines.append("\n" + "-" * 100)
            output_lines.append("🐍 Python Code")
            output_lines.append("-" * 100)
            output_lines.append(f"\n{highlighted_code.strip()}\n")
            #output_lines.append(end_separator)

        print("\n".join(output_lines))
        print(end_separator + "\n")
        sys.stdout.flush()

    def log_sql_query(self, query: str, level: str = "info"):
        """Format and log an SQL query with syntax highlighting."""
        formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
        highlighted_query = highlight(formatted_query, SqlLexer(), TerminalFormatter())

        print("\n" + "-" * 100)
        print("📜 SQL Code")
        print("-" * 100)
        print(f"\n{highlighted_query.strip()}\n")
        self.log_footer()

    def log_python_code(self, code: str, level: str = "info"):
        """Format and log Python code with syntax highlighting."""
        highlighted_code = highlight(code, PythonLexer(), TerminalFormatter())

        print("\n" + "-" * 100)
        print("🐍 Python Code")
        print("-" * 100)
        print(f"\n{highlighted_code.strip()}\n")
        self.log_footer()

    def log_start(self, method_name: str):
        """Log the start of a method."""
        self.log_message(f"Starting {method_name}...", level="info")

    def log_end(self, method_name: str, success: bool = True, additional_message: str = ""):
        """Log the end of a method with an appropriate icon."""
        
        # ✅ Determine the correct icon based on success or failure
        icon = "✅" if success else self.ICONS.get("warning", "⚠️")  # Defaults to warning if failure

        status = "successfully" if success else "with warnings or issues"
        formatted_message = f"{icon} Finished {method_name} {status}. {additional_message}"
        
        # ✅ Log as "info" if success, otherwise "warning" or "error"
        self.log_message(formatted_message, level="info" if success else "warning")

    def log_dataframe_summary(self, df: DataFrame, label: str, level="info"):
        """
        Logs summary information of a DataFrame in a structured block.

        Args:
            df (DataFrame): The DataFrame to summarize.
            label (str): Label for the DataFrame block.
            level (str): Log level ('debug', 'info', etc.).
        """
        if df:
            row_count = df.count()
            column_count = len(df.columns)
            estimated_memory = df.rdd.map(lambda row: len(str(row))).sum() / (1024 * 1024)

            # **Capture Schema as String** (Instead of printing separately)
            schema_str = df._jdf.schema().treeString()

            # ✅ **Remove Explicit Level Formatting** (Let log_block handle it)
            output_lines = [
                f"Estimated Memory Usage: {estimated_memory:.2f} MB",
                f"Rows: {row_count}",
                f"Columns: {column_count}",
                "Schema:\n" + schema_str  # ✅ Schema is included inside the block
            ]

            # ✅ Print everything inside the structured log block
            self.log_block(f"{label} DataFrame Info", content_lines=output_lines, level=level)

    def log_info(self, message):
        """Convenience method for logging informational messages."""
        self.log_message(message, level="info")

    def log_debug(self, message: str):
        """Log a debug message only if debug mode is enabled."""
        if self.debug:
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
        """Decorator to log function entry/exit."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.log_debug(f"Entering {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            self.log_debug(f"Exiting {func.__name__} with result: {result}")
            return result
        return wrapper

class LoggerTester:
    """
    A structured test suite for the Logger class.
    Ensures all functions work correctly and logs are formatted properly.
    """

    def __init__(self, logger: Logger):
        """
        Initializes the LoggerTester.

        Args:
            logger (Logger): Existing logger instance.
        """
        # ✅ Use the provided logger directly
        self.logger = logger

        # ✅ Ensure consistency with the passed logger’s debug state
        self.debug = self.logger.debug

        # ✅ Log the debug state for verification
        self.logger.log_debug(f"LoggerTester initialized with debug={self.debug}")

    def run_all_tests(self):
        """Runs all tests inside a structured logging block."""

        self.logger.log_start("Logger Test Suite")

        try:
            self.test_log_messages()
            self.test_log_sql_query()
            self.test_log_python_code()
            self.test_log_blocks()
            self.test_function_entry_exit()
            
            self.logger.log_info("✅ All tests completed successfully.")

        except Exception as e:
            self.logger.log_error(f"❌ An unexpected error occurred: {e}")

        self.logger.log_footer()
        self.logger.log_end("Logger Test Suite")

    def test_log_messages(self):
        """Tests individual log messages at various levels."""
        # ✅ Log a structured header for the test
        self.logger.log_block("Testing Individual Log Messages", level="info")

        # ✅ Log messages directly without icons
        self.logger.log_info("This is an informational message.")
        if self.logger.debug:
            self.logger.log_debug("This is a debug message for troubleshooting.")
        self.logger.log_warning("This is a warning message.")

        # ✅ Handle ERROR message with exception
        try:
            self.logger.log_error("This is an error message. It will raise an exception.")
        except RuntimeError as e:
            self.logger.log_info(f"✅ Caught RuntimeError: {e}")

        # ✅ Handle CRITICAL message with exception
        try:
            self.logger.log_critical("This is a critical message. It will also raise an exception.")
        except RuntimeError as e:
            self.logger.log_info(f"✅ Caught RuntimeError: {e}")

    def test_log_blocks(self):
        """Tests block-style logging at different log levels."""
        
        # ✅ Test INFO & DEBUG in one combined block
        messages = ["This is an informational message."]
        if self.logger.debug:
            messages.append("This is a debug message.")
        
        self.logger.log_block("Testing INFO & DEBUG Log Blocks", content_lines=messages, level="info")

        # ✅ Test WARNING block
        self.logger.log_block("Testing WARNING Log Block", content_lines=["This is a warning message."], level="warning")

        # ✅ Test ERROR block
        self.logger.log_block("Testing ERROR Log Block", content_lines=["This is an error message."], level="error")

        # ✅ Test CRITICAL block
        self.logger.log_block("Testing CRITICAL Log Block", content_lines=["This is a critical message."], level="critical")

    def test_log_sql_query(self):
        """Tests logging an SQL query with syntax highlighting."""
        sql_query = """
        SELECT id, name, age
        FROM users
        WHERE age > 30
        ORDER BY name;
        """
        self.logger.log_block("Testing SQL Query Logging", sql_query=sql_query)

    def test_log_python_code(self):
        """Tests logging a Python script with syntax highlighting."""
        python_code = """
        def example_function(a, b):
            return a + b

        result = example_function(5, 10)
        print(result)
        """
        self.logger.log_block("Testing Python Code Logging", python_query=python_code)

    def test_function_entry_exit(self):
        """Tests function entry/exit logging using the decorator."""

        @self.logger.log_function_entry_exit
        def test_function(x, y):
            return x + y

        result = test_function(5, 10)
        self.logger.log_info(f"✅ Function returned: {result}")