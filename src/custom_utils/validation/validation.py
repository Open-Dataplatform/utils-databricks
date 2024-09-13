from pyspark.sql.utils import AnalysisException
from custom_utils.config.config import Config


class PathValidator:
    def __init__(self, config: Config):
        """
        Initializes the PathValidator class.

        Args:
            config (Config): An instance of the Config class containing configuration parameters.
        """
        self.config = config
        self.dbutils = config.dbutils
        self.helper = config.helper

    def verify_paths_and_files(self):
        """
        Verifies that the schema folder, schema file, and source folder exist and contain the expected files.
        Returns the schema file path, data file path, and file type (e.g., JSON or XML) for further processing.

        Returns:
            tuple: A tuple containing the schema file path, data file path, and file type.
        """
        try:
            mount_point = self._get_mount_point()

            # Verify schema folder and file
            schema_file_path, file_type = self._verify_schema_folder(mount_point)

            # Verify source folder and files
            data_file_path = self._verify_source_folder(mount_point)

            self._log_verified_info(schema_file_path, data_file_path, file_type)

            return schema_file_path, data_file_path, file_type

        except Exception as e:
            error_message = f"Failed to validate paths or files: {str(e)}"
            self._log_message(error_message, level="error")
            self._exit_notebook(error_message)

    def _get_mount_point(self):
        """
        Retrieves the mount point based on the source environment.

        Returns:
            str: The mount point path.
        """
        try:
            target_mount = [
                m.mountPoint
                for m in self.dbutils.fs.mounts()
                if self.config.source_environment in m.source
            ]

            if not target_mount:
                error_message = f"No mount point found for environment: {self.config.source_environment}"
                self._log_message(error_message, level="error")
                raise Exception(error_message)

            return target_mount[0]
        except Exception as e:
            error_message = f"Error while retrieving mount points: {str(e)}"
            self._log_message(error_message, level="error")
            self._exit_notebook(error_message)

    def _verify_schema_folder(self, mount_point: str):
        """
        Verifies the schema folder and the expected schema file.

        Args:
            mount_point (str): The mount point path.

        Returns:
            tuple: The schema file path and file type.
        """
        schema_directory_path = f"{mount_point}/{self.config.schema_folder_name}/{self.config.source_datasetidentifier}"
        self._log_message(f"Schema directory path: {schema_directory_path}", level="info")

        try:
            schema_files = self.dbutils.fs.ls(schema_directory_path)
            expected_schema_filename = f"{self.config.source_datasetidentifier}_schema"
            schema_format_mapping = {".json": "json", ".xsd": "xml"}

            found_schema_file = None
            file_type = None

            for file in schema_files:
                for ext, ftype in schema_format_mapping.items():
                    if file.name == f"{expected_schema_filename}{ext}":
                        found_schema_file = file.name
                        file_type = ftype
                        break

            if not found_schema_file:
                available_files = [file.name for file in schema_files]
                error_message = (f"Expected schema file '{expected_schema_filename}.json' or "
                                 f"'{expected_schema_filename}.xsd' not found in {schema_directory_path}.")
                self._log_message(error_message, level="error")
                self._log_message(f"Available files: {available_files}", level="error")
                raise Exception(error_message)

            schema_file_path = f"/dbfs{schema_directory_path}/{found_schema_file}"
            return schema_file_path, file_type

        except AnalysisException as e:
            error_message = f"Failed to access schema folder: {str(e)}"
            self._log_message(error_message, level="error")
            raise Exception(error_message)

    def _verify_source_folder(self, mount_point: str):
        """
        Verifies the source folder and checks for the expected files.

        Args:
            mount_point (str): The mount point path.

        Returns:
            str: The data file path.
        """
        source_directory_path = f"{mount_point}/{self.config.source_datasetidentifier}"
        self._log_message(f"Source directory path: {source_directory_path}", level="info")

        try:
            source_files = self.dbutils.fs.ls(source_directory_path)
            number_of_files = len(source_files)

            if self.config.source_filename == "*":
                return f"{source_directory_path}/*"
            else:
                self._log_message(f"Expected minimum files: 1", level="info")
                self._log_message(f"Actual number of files found: {number_of_files}", level="info")

                matched_files = [
                    file
                    for file in source_files
                    if self.config.source_filename in file.name
                ]
                if not matched_files:
                    available_files = [file.name for file in source_files]
                    error_message = f"No files matching '{self.config.source_filename}' found in {source_directory_path}."
                    self._log_message(error_message, level="error")
                    self._log_message(f"Available files: {available_files}", level="error")
                    raise Exception(error_message)

                return f"{source_directory_path}/{matched_files[0].name}"

        except AnalysisException as e:
            error_message = f"Failed to access source folder: {str(e)}"
            self._log_message(error_message, level="error")
            raise Exception(error_message)

    def _log_verified_info(self, schema_file_path, data_file_path, file_type):
        """
        Log the validated paths and files information in a clear format.
        """
        self._log_message("\n=== Validated Information ===", level="info", single_info_prefix=True)
        print("------------------------------")
        print(f"Schema File Path: {schema_file_path}")
        print(f"Source File Path: {data_file_path}")
        print(f"File Type: {file_type}")
        print("------------------------------")

    def _log_message(self, message, level="info", single_info_prefix=False):
        """
        Logs a message using the helper, or falls back to print if helper is not available.
        If single_info_prefix is True, it prints '[INFO]' only once before the log block.

        Args:
            message (str): The message to log or print.
            level (str): The log level (e.g., info, warning, error). Defaults to "info".
            single_info_prefix (bool): Whether to print `[INFO]` prefix just once.
        """
        if level == "info" and not self.config.debug:
            return
        if single_info_prefix and level == "info":
            print("[INFO]")
            print(message)
        elif self.helper:
            self.helper.write_message(message, level)
        else:
            print(f"[{level.upper()}] {message}")

    def _exit_notebook(self, message):
        """Exit the notebook with an error message."""
        if self.dbutils:
            self.dbutils.notebook.exit(f"[ERROR] {message}")
        else:
            raise SystemExit(f"[ERROR] {message}")