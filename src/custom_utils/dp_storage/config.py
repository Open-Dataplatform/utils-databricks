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
