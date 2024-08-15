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

    # Step 1: Filter and find the mount point that contains the config.source_environment
    target_mount = [m.mountPoint for m in dbutils.fs.mounts() if config.source_environment in m.source]

    if not target_mount:
        helper.write_message(f"No mount point found for environment: {config.source_environment}")
        raise Exception(f"No mount point found for environment: {config.source_environment}")
    
    # Extract the target mount point
    mount_point = target_mount[0]

    # Step 2: Verify the Schema Folder and Schema File
    schema_directory_path = f"{mount_point}/{config.schema_folder_name}/{config.source_datasetidentifier}"
    print(f"Schema directory path: {schema_directory_path}")

    try:
        schema_files = dbutils.fs.ls(schema_directory_path)
        expected_schema_filename = f"{config.source_datasetidentifier}_schema"
        expected_schema_formats = [".json", ".xsd"]

        # Check for the expected schema file in .json or .xsd formats
        found_schema_file = None
        schema_file_extension = None
        for file in schema_files:
            for ext in expected_schema_formats:
                if file.name == f"{expected_schema_filename}{ext}":
                    found_schema_file = file.name
                    schema_file_extension = ext
                    break

        # Print expected and found schema names
        print(f"Expected schema file: {expected_schema_filename}.json or {expected_schema_filename}.xsd")
        print(f"Found schema file: {found_schema_file if found_schema_file else 'None'}")

        # Assertion: Ensure the found schema matches the expected name and format
        if not found_schema_file:
            helper.write_message(f"Expected schema file not found in {schema_directory_path}.")
            raise Exception(f"Expected schema file not found in {schema_directory_path}.")

        # Construct the full schema file path
        schema_file_path = f"/dbfs{schema_directory_path}/{found_schema_file}"
    
    except Exception as e:
        helper.write_message(f"Failed to access schema folder: {str(e)}")
        raise Exception(f"Failed to access schema folder: {str(e)}")

    # Step 3: Verify the Source Folder and Check for Files
    source_directory_path = f"{mount_point}/{config.source_datasetidentifier}"
    print(f"Source directory path: {source_directory_path}")

    try:
        source_files = dbutils.fs.ls(source_directory_path)
        number_of_files = len(source_files)
        
        # Print expected and actual number of files
        expected_min_files = 1
        print(f"Expected minimum files: {expected_min_files}")
        print(f"Actual number of files found: {number_of_files}")

        if config.source_filename == "*":
            # If the schema file is .xsd, the data files are expected to be .xml
            if schema_file_extension == ".xsd":
                data_file_path = f"{source_directory_path}/*.xml"
            else:
                data_file_path = f"{source_directory_path}/*"
        else:
            # Ensure that a specific file pattern is matched (if it's not "*")
            matched_files = [file for file in source_files if config.source_filename in file.name]
            number_of_files = len(matched_files)
            print(f"Number of matching files found: {number_of_files}")
            if not matched_files:
                helper.write_message(f"No files matching '{config.source_filename}' found in {source_directory_path}.")
                raise Exception(f"No files matching '{config.source_filename}' found in {source_directory_path}.")

            # Adjust the extension if the schema is .xsd (expect .xml for data)
            data_file_name = matched_files[0].name
            if schema_file_extension == ".xsd":
                data_file_name = data_file_name.replace(".json", ".xml")

            # Construct the full data file path with the exact file name
            data_file_path = f"{source_directory_path}/{data_file_name}"

    except Exception as e:
        helper.write_message(f"Failed to access source folder: {str(e)}")
        raise Exception(f"Failed to access source folder: {str(e)}")

    # Log success if all checks pass
    helper.write_message("All paths and files verified successfully. Proceeding with notebook execution.")

    # Return the schema file path and data file path
    return schema_file_path, data_file_path
