from pyspark.sql.utils import AnalysisException
from custom_utils.dp_storage.config import Config


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

            self.helper.write_message(
                "All paths and files verified successfully. Proceeding with notebook execution."
            )
            return schema_file_path, data_file_path, file_type

        except Exception as e:
            self.helper.write_message(f"Failed to validate paths or files: {str(e)}")
            self.dbutils.notebook.exit(str(e))
            raise Exception(f"Failed to validate paths or files: {str(e)}")

    def _get_mount_point(self):
        """
        Retrieves the mount point based on the source environment.

        Returns:
            str: The mount point path.
        """
        target_mount = [
            m.mountPoint
            for m in self.dbutils.fs.mounts()
            if self.config.source_environment in m.source
        ]

        if not target_mount:
            self.helper.write_message(
                f"No mount point found for environment: {self.config.source_environment}"
            )
            raise Exception(
                f"No mount point found for environment: {self.config.source_environment}"
            )

        return target_mount[0]

    def _verify_schema_folder(self, mount_point: str):
        """
        Verifies the schema folder and the expected schema file.

        Args:
            mount_point (str): The mount point path.

        Returns:
            tuple: The schema file path and file type.
        """
        schema_directory_path = f"{mount_point}/{self.config.schema_folder_name}/{self.config.source_datasetidentifier}"
        self.helper.write_message(f"Schema directory path: {schema_directory_path}")

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
                self.helper.write_message(
                    f"Expected schema file not found in {schema_directory_path}."
                )
                available_files = [file.name for file in schema_files]
                self.helper.write_message(f"Available files: {available_files}")
                raise Exception(
                    f"Expected schema file '{expected_schema_filename}.json' or '{expected_schema_filename}.xsd' not found in {schema_directory_path}."
                )

            schema_file_path = f"/dbfs{schema_directory_path}/{found_schema_file}"
            return schema_file_path, file_type

        except AnalysisException as e:
            self.helper.write_message(f"Failed to access schema folder: {str(e)}")
            raise Exception(f"Failed to access schema folder: {str(e)}")

    def _verify_source_folder(self, mount_point: str):
        """
        Verifies the source folder and checks for the expected files.

        Args:
            mount_point (str): The mount point path.

        Returns:
            str: The data file path.
        """
        source_directory_path = f"{mount_point}/{self.config.source_datasetidentifier}"
        self.helper.write_message(f"Source directory path: {source_directory_path}")

        try:
            source_files = self.dbutils.fs.ls(source_directory_path)
            number_of_files = len(source_files)

            self.helper.write_message(f"Expected minimum files: 1")
            self.helper.write_message(
                f"Actual number of files found: {number_of_files}"
            )

            if self.config.source_filename == "*":
                return f"{source_directory_path}/*"

            matched_files = [
                file
                for file in source_files
                if self.config.source_filename in file.name
            ]
            if not matched_files:
                available_files = [file.name for file in source_files]
                self.helper.write_message(
                    f"No files matching '{self.config.source_filename}' found in {source_directory_path}."
                )
                self.helper.write_message(f"Available files: {available_files}")
                raise Exception(
                    f"No files matching '{self.config.source_filename}' found in {source_directory_path}."
                )

            return f"{source_directory_path}/{matched_files[0].name}"

        except AnalysisException as e:
            self.helper.write_message(f"Failed to access source folder: {str(e)}")
            raise Exception(f"Failed to access source folder: {str(e)}")
