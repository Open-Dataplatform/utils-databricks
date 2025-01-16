from typing import Dict, Optional
from custom_utils.logging.logger import Logger

def initialize_common_widgets(dbutils, logger: Optional[Logger] = None):
    """
    Initializes common widgets that are the same across all datasets.

    Args:
        dbutils: The Databricks utilities object.
        logger (Optional[Logger]): Optional logger instance for logging.
    """
    try:
        dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
        dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")
        dbutils.widgets.text("SourceContainer", "landing", "Source Container")
        if logger:
            logger.log_message("Common widgets initialized successfully.")
    except Exception as e:
        if logger:
            logger.log_error(f"Error initializing common widgets: {str(e)}")
        raise RuntimeError(f"Failed to initialize common widgets: {str(e)}")


def clear_widgets_except_common(dbutils, logger: Optional[Logger] = None):
    """
    Clears all widgets except the common widgets like SourceDatasetidentifier,
    SourceStorageAccount, DestinationStorageAccount, and SourceContainer.

    Args:
        dbutils: The Databricks utilities object.
        logger (Optional[Logger]): Optional logger instance for logging.
    """
    common_keys = ["SourceDatasetidentifier", "SourceStorageAccount", "DestinationStorageAccount", "SourceContainer"]
    all_keys = dbutils.widgets.get().split("\n")  # Assuming this retrieves all widget keys
    try:
        for key in all_keys:
            if key not in common_keys:
                try:
                    dbutils.widgets.remove(key)
                    if logger:
                        logger.log_debug(f"Removed widget: {key}")
                except Exception as remove_error:
                    if "InputWidgetNotDefined" in str(remove_error):
                        if logger:
                            logger.log_debug(f"Widget '{key}' not found; skipping removal.")
                    else:
                        if logger:
                            logger.log_warning(f"Unexpected error while removing widget '{key}': {str(remove_error)}")
        if logger:
            logger.log_message("Cleared all dataset-specific widgets.")
    except Exception as e:
        if logger:
            logger.log_error(f"Error clearing widgets: {str(e)}")
        raise RuntimeError(f"Failed to clear widgets: {str(e)}")


def initialize_widgets(dbutils, selected_dataset: str, external_params: Optional[Dict[str, str]] = None, logger: Optional[Logger] = None):
    """
    Dynamically initializes and updates widget values based on the selected dataset.

    Args:
        dbutils: The Databricks utilities object.
        selected_dataset (str): The selected dataset identifier.
        external_params (Dict[str, str], optional): External parameters to override widget values.
        logger (Optional[Logger]): Optional logger instance for logging.
    """
    try:
        clear_widgets_except_common(dbutils, logger)

        dataset_config = {
            "example_dataset": {
                "FileType": "json",
                "SourceFileName": "example_dataset*",
                "KeyColumns": "column1,column2",
                "FeedbackColumn": "EventTimestamp",
                "DepthLevel": "1",
                "SchemaFolderName": "schemachecks"
            },
            # Add more dataset configurations as needed
        }

        if selected_dataset not in dataset_config:
            raise ValueError(f"Unknown dataset identifier: {selected_dataset}")

        dataset_widgets = dataset_config[selected_dataset]

        for key, value in dataset_widgets.items():
            dbutils.widgets.text(key, value, key)

        if external_params:
            for key, value in external_params.items():
                if key in dataset_widgets:
                    dbutils.widgets.text(key, value, key)

        if logger:
            logger.log_message(f"Widgets initialized for dataset: {selected_dataset}")

    except Exception as e:
        if logger:
            logger.log_error(f"Error initializing widgets for dataset {selected_dataset}: {str(e)}")
        raise RuntimeError(f"Failed to initialize widgets for dataset: {selected_dataset}")
