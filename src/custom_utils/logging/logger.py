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
        self.debug = debug
        self.logger = logging.getLogger("custom_logger")
        self.logger.propagate = False  # Prevents duplicate logs in Databricks

        if not self.logger.hasHandlers():
            self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

            # Console Handler - ✅ Removes duplicate level formatting
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter('%(message)s')  # ✅ Prevents repeated level names
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)

            # File Handler (if provided)
            if log_to_file:
                file_handler = logging.FileHandler(log_to_file)
                file_handler.setFormatter(console_formatter)
                self.logger.addHandler(file_handler)

    def set_level(self, debug: bool):
        """Set logging level based on debug flag."""
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

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
        icon = self.ICONS.get(level, "ℹ️")  # Default to info icon
        formatted_message = f"{icon} [{level.upper()}] - {message}"  # ✅ Icon first, level, then message
        log_function = getattr(self.logger, level, self.logger.info)

        log_function(formatted_message)  # ✅ No duplicated level anymore

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
        separator_length = 100
        start_separator = "=" * separator_length
        formatted_header = f" {header} ".center(separator_length, "=")
        end_separator = "-" * separator_length

        output_lines = [
            "\n" + start_separator,  # ✅ Block Start
            formatted_header,
            start_separator,
        ]

        # ✅ Log normal text content inside the block
        if content_lines:
            output_lines.extend([f"{self.ICONS.get(level, 'ℹ️')} [{level.upper()}] - {line}" for line in content_lines])

        # ✅ Append block footer only if no separate SQL/Python logs are created
        if not sql_query and not python_query:
            output_lines.append(end_separator)

        # ✅ Print block content first to ensure structured logging
        print("\n".join(output_lines))
        sys.stdout.flush()

        # ✅ Log SQL queries inside the block
        if sql_query:
            self.log_sql_query(sql_query, level=level)

        # ✅ Log Python queries inside the block
        if python_query:
            self.log_python_code(python_query, level=level)

        # ✅ Ensure SQL/Python logs end with a footer
        if sql_query or python_query:
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
        """Decorator to log function entry/exit."""
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
        """Runs all tests inside a structured logging block."""
        
        # ✅ Start Logging (Outside the Block)
        self.logger.log_start("Logger Test Suite")

        try:
            self.test_log_messages()
            self.test_log_blocks()
            self.test_log_sql_query()
            self.test_log_python_query()
            self.test_log_sql_in_block()
            self.test_log_python_in_block()
            self.test_dataframe_summary()
            self.test_function_entry_exit()

            # ✅ Success message remains inside the block
            self.logger.log_info("✅ All tests completed successfully.")

        except Exception as e:
            self.logger.log_error(f"❌ An unexpected error occurred: {e}")

        # ✅ Ensure footer is printed at the end of the block
        self.logger.log_footer()

        # ✅ End Logging (Outside the Block)
        self.logger.log_end("Logger Test Suite")

    def test_log_messages(self):
        """Tests log messages at different levels."""
        
        self.logger.log_block("Testing Log Messages", [
            "This is an informational message.",
            "This is a debug message for troubleshooting.",
            "This is a warning message."
        ], level="info")

        try:
            self.logger.log_error("This is an error message. It will raise an exception.")
        except RuntimeError:
            self.logger.log_info("✅ Error handling works correctly.")  # ✅ Inside structured log

        try:
            self.logger.log_critical("This is a critical message. It will also raise an exception.")
        except RuntimeError:
            self.logger.log_info("✅ Critical error handling works correctly.")  # ✅ Inside structured log

    def test_log_blocks(self):
        """Tests structured logging blocks."""
        self.logger.log_block("INFO Level Block", ["Informational message."], level="info")
        self.logger.log_block("DEBUG Level Block", ["Debugging details."], level="debug")

    def test_log_sql_query(self):
        """Tests SQL query logging."""
        sql_query = "SELECT * FROM users WHERE age > 30"
        self.logger.log_sql_query(sql_query)

    def test_log_python_query(self):
        """Tests Python code logging."""
        python_code = "def hello():\n    print('Hello, world!')"
        self.logger.log_python_code(python_code)

    def test_log_sql_in_block(self):
        """Tests logging an SQL query inside a structured block."""
        sql_query = """
        SELECT PipelineId,
               COUNT(*) AS duplicate_count
        FROM view_pipelines_df
        GROUP BY PipelineId
        HAVING COUNT(*) > 1
        """
        self.logger.log_block("SQL Validation Query", sql_query=sql_query)

    def test_log_python_in_block(self):
        """Tests logging a Python script inside a structured block."""
        python_code = """
        def greet(name):
            print(f"Hello, {name}!")

        greet('World')
        """
        self.logger.log_block("Python Code Execution", python_query=python_code)

    def test_dataframe_summary(self):
        """Tests DataFrame summary logging."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("LoggerTest").getOrCreate()
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["ID", "Name"])
        self.logger.log_dataframe_summary(df, label="Test DataFrame")

    def test_function_entry_exit(self):
        """Tests function entry/exit logging."""
        @self.logger.log_function_entry_exit
        def sample_function(x, y):
            return x + y

        sample_function(5, 10)