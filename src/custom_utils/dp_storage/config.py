from pyspark.sql.utils import AnalysisException

def verify_paths_and_files(dbutils, config, helper):
    """
    Verifies that the schema folder, schema file, and source folder exist and contain the expected files.
    Returns the schema file path and data file path for further processing.

    Args:
        dbutils (object): Databricks utility object to interact with DBFS.
        config (object): Configuration object containing paths and settings.
        helper (object): Helper object for logging messages.

    Returns:
        tuple: A tuple containing the schema file path and the data file path.
    """

    # Step 1: Identify the correct mount point for the specified environment.
    target_mount = [m.mountPoint for m in dbutils.fs.mounts() if config.source_environment in m.source]

    if not target_mount:
        error_message = f"No mount point found for environment: {config.source_environment}"
        helper.write_message(error_message)
        raise Exception(error_message)
    
    # Extract the first matched mount point (assuming only one is relevant)
    mount_point = target_mount[0]

    # Step 2: Validate the Schema Folder and File
    schema_directory_path = f"{mount_point}/{config.schema_folder_name}/{config.source_datasetidentifier}"
    print(f"Schema directory path: {schema_directory_path}")

    try:
        schema_files = dbutils.fs.ls(schema_directory_path)
        expected_schema_filename = f"{config.source_datasetidentifier}_schema"
        expected_schema_formats = [".json", ".xsd"]

        # Search for the expected schema file
        found_schema_file = next((file.name for file in schema_files if any(file.name == f"{expected_schema_filename}{ext}" for ext in expected_schema_formats)), None)
        schema_file_extension = next((ext for ext in expected_schema_formats if found_schema_file and found_schema_file.endswith(ext)), None)

        # Print expected and found schema names
        print(f"Expected schema file: {expected_schema_filename}.json or {expected_schema_filename}.xsd")
        print(f"Found schema file: {found_schema_file if found_schema_file else 'None'}")

        # Ensure the found schema matches the expected name and format
        if not found_schema_file:
            error_message = f"Expected schema file not found in {schema_directory_path}."
            helper.write_message(error_message)
            raise Exception(error_message)

        # Construct the full schema file path
        schema_file_path = f"/dbfs{schema_directory_path}/{found_schema_file}"
    
    except Exception as e:
        error_message = f"Failed to access schema folder: {str(e)}"
        helper.write_message(error_message)
        raise Exception(error_message)

    # Step 3: Validate the Source Folder and Data Files
    source_directory_path = f"{mount_point}/{config.source_datasetidentifier}"
    print(f"Source directory path: {source_directory_path}")

    try:
        source_files = dbutils.fs.ls(source_directory_path)
        if not source_files:
            error_message = f"No files found in {source_directory_path}. Expected at least 1 file."
            helper.write_message(error_message)
            raise Exception(error_message)

        # Handle wildcard or specific file pattern for data files
        if config.source_filename == "*":
            # Default to all files, adjust for XML if the schema is .xsd
            data_file_path = f"{source_directory_path}/*.xml" if schema_file_extension == ".xsd" else f"{source_directory_path}/*"
        else:
            # Match specific files based on the provided pattern
            matched_files = [file for file in source_files if config.source_filename in file.name]
            if not matched_files:
                error_message = f"No files matching '{config.source_filename}' found in {source_directory_path}."
                helper.write_message(error_message)
                raise Exception(error_message)

            # Adjust the file extension if needed (e.g., .json -> .xml if using an .xsd schema)
            data_file_name = matched_files[0].name.replace(".json", ".xml") if schema_file_extension == ".xsd" else matched_files[0].name
            data_file_path = f"{source_directory_path}/{data_file_name}"

    except Exception as e:
        error_message = f"Failed to access source folder: {str(e)}"
        helper.write_message(error_message)
        raise Exception(error_message)

    # Log success if all checks pass
    helper.write_message("All paths and files verified successfully. Proceeding with notebook execution.")

    # Return the validated schema file path and data file path
    return schema_file_path, data_file_path

def initialize_config(dbutils, helper, source_environment, destination_environment, source_container, source_datasetidentifier, 
                      source_filename='*', key_columns='', feedback_column='', schema_folder_name='schemachecks', depth_level=None):
    """
    Initialize the Config class with input parameters and return the config object.
    
    This function provides a centralized way to manage configuration settings needed for your Databricks notebook.
    It allows both required and optional parameters to be easily defined, with sensible defaults where applicable.

    Args:
        dbutils (object): The Databricks utility object used for interacting with DBFS, widgets, secrets, and other operations.
        helper (object): A helper object responsible for logging messages and fetching parameters.
        source_environment (str): The source storage account/environment where the input data is stored.
        destination_environment (str): The destination storage account/environment for storing processed data.
        source_container (str): The container within the storage account where source files are stored.
        source_datasetidentifier (str): The dataset identifier, usually a folder or dataset name that’s being processed.
        source_filename (str, optional): The pattern or name of the source files. Defaults to '*' to read all files.
        key_columns (str, optional): Comma-separated key columns used to identify unique records in the dataset. Defaults to ''.
        feedback_column (str, optional): The column used for feedback or timestamp tracking within the data. Defaults to ''.
        schema_folder_name (str, optional): The folder where schema validation files are stored. Defaults to 'schemachecks'.
        depth_level (int, optional): The depth level for processing and flattening JSON structures. Can be left as None.

    Returns:
        Config: An instance of the Config class containing all the initialized parameters.
    """
    # Create and return an instance of the Config class with all parameters
    return Config(
        dbutils=dbutils,
        helper=helper,
        source_environment=source_environment,
        destination_environment=destination_environment,
        source_container=source_container,
        source_datasetidentifier=source_datasetidentifier,
        source_filename=source_filename,
        key_columns=key_columns,
        feedback_column=feedback_column,
        schema_folder_name=schema_folder_name,
        depth_level=depth_level
    )
