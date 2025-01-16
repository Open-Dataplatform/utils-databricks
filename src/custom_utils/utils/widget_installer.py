from typing import Dict, Optional
from custom_utils.logging.logger import Logger

def initialize_common_widgets(dbutils, logger=None):
    """
    Initializes common widgets that are the same across all datasets, ensuring required widgets are created.

    Args:
        dbutils: The Databricks utilities object.
        logger: Optional logger instance for logging.
    """
    try:
        # Define the required common widgets
        dbutils.widgets.text("SourceDatasetidentifier", "", "Source Dataset Identifier")
        dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
        dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")
        dbutils.widgets.text("SourceContainer", "landing", "Source Container")

        if logger:
            logger.log_message("Common widgets initialized successfully.")
    except Exception as e:
        if logger:
            logger.log_error(f"Error initializing common widgets: {str(e)}")
        raise RuntimeError(f"Failed to initialize common widgets: {str(e)}")

def clear_all_widgets(dbutils, logger=None):
    """
    Removes all existing widgets to ensure a clean slate before initialization.

    Args:
        dbutils: The Databricks utilities object.
        logger: Optional logger instance for logging.
    """
    try:
        dbutils.widgets.removeAll()
        if logger:
            logger.log_message("All existing widgets removed successfully.")
    except Exception as e:
        if logger:
            logger.log_error(f"Error during widget cleanup: {str(e)}")
        raise RuntimeError(f"Failed to clear widgets: {str(e)}")

def clear_widgets_except_common(dbutils, logger=None):
    """
    Clears all widgets except common ones like SourceDatasetidentifier,
    SourceStorageAccount, DestinationStorageAccount, and SourceContainer.

    Args:
        dbutils: The Databricks utilities object.
        logger: Optional logger instance for logging.
    """
    common_keys = ["SourceDatasetidentifier", "SourceStorageAccount", "DestinationStorageAccount", "SourceContainer"]

    # List of all potential widget keys to clear (predefined)
    all_possible_keys = [
        "FileType",
        "SourceFileName",
        "KeyColumns",
        "FeedbackColumn",
        "DepthLevel",
        "SchemaFolderName",
        "XmlRootName",
        "SheetName",
    ]

    try:
        for key in all_possible_keys:
            if key not in common_keys:
                try:
                    dbutils.widgets.remove(key)
                    if logger:
                        logger.log_message(f"Removed widget: {key}")
                except Exception as e:
                    # Only log errors that are not about missing widgets
                    if "InputWidgetNotDefined" not in str(e):
                        if logger:
                            logger.log_warning(f"Unexpected error while removing widget '{key}': {str(e)}")
        if logger:
            logger.log_message("Cleared all dataset-specific widgets except common ones.")
    except Exception as e:
        if logger:
            logger.log_error(f"Error clearing widgets: {str(e)}")
        raise RuntimeError(f"Failed to clear widgets: {str(e)}")

def initialize_widgets(dbutils, selected_dataset, external_params=None, logger=None):
    """
    Dynamically initializes and updates widget values based on the selected dataset.

    Args:
        dbutils: The Databricks utilities object.
        selected_dataset (str): The selected dataset identifier.
        external_params (dict, optional): External parameters to override widget values.
        logger: Optional logger instance for logging.
    """
    try:
        # Step 1: Clear all widgets to start fresh
        clear_all_widgets(dbutils, logger)

        # Step 2: Reinitialize common widgets, including SourceDatasetidentifier
        initialize_common_widgets(dbutils, logger)

        # Step 3: Define dataset-specific widget configurations
        dataset_config = {
            "triton__flow_plans": {
                "FileType": "json",
                "SourceFileName": "triton__flow_plans*",
                "KeyColumns": "Guid",
                "FeedbackColumn": "EventTimestamp",
                "DepthLevel": "1",
                "SchemaFolderName": "schemachecks"
            },
            "cpx_so__nomination": {
                "FileType": "json",
                "SourceFileName": "cpx_so__nomination*",
                "KeyColumns": "flows_accountInternal_code, flows_accountExternal_code, flows_location_code, flows_direction, flows_periods_validityPeriod_begin, flows_periods_validityPeriod_end",
                "FeedbackColumn": "dateCreated",
                "DepthLevel": "",
                "SchemaFolderName": "schemachecks"
            },
            "ddp_em__dayahead_flows_nemo": {
                "FileType": "xml",
                "SourceFileName": "ddp_em__dayahead_flows_nemo*",
                "KeyColumns": "TimeSeries_mRID, TimeSeries_Period_timeInterval_start, TimeSeries_Period_Point_position",
                "FeedbackColumn": "createdDateTime",
                "DepthLevel": "",
                "SchemaFolderName": "schemachecks",
                "XmlRootName": "Schedule_MarketDocument"
            },
            "ddp_cm__mfrr_settlement": {
                "FileType": "xml",
                "SourceFileName": "ddp_cm__mfrr_settlement*",
                "KeyColumns": "mRID, TimeSeries_mRID, TimeSeries_Period_timeInterval_start, TimeSeries_Period_Point_position, TimeSeries_Period_resolution",
                "FeedbackColumn": "createdDateTime",
                "DepthLevel": "",
                "SchemaFolderName": "schemachecks",
                "XmlRootName": "ReserveAllocationResult_MarketDocument"
            },
            "pluto_pc__units_scadamw": {
                "FileType": "xlsx",
                "SourceFileName": "*",
                "KeyColumns": "Unit_GSRN",
                "SheetName": "Sheet"
            }
        }

        # Step 4: Validate selected dataset and get its configuration
        if selected_dataset not in dataset_config:
            raise ValueError(f"Unknown dataset identifier: {selected_dataset}")
        dataset_widgets = dataset_config[selected_dataset]

        # Step 5: Create widgets dynamically based on dataset configuration
        for key, value in dataset_widgets.items():
            dbutils.widgets.text(key, value, key)

        # Step 6: Apply external parameters if provided
        if external_params:
            for key, value in external_params.items():
                if key in dataset_widgets:
                    dbutils.widgets.text(key, value, key)

        if logger:
            logger.log_message(f"Widgets initialized and updated for dataset: {selected_dataset}")

    except Exception as e:
        if logger:
            logger.log_error(f"Error initializing widgets for dataset {selected_dataset}: {str(e)}")
        raise RuntimeError(f"Failed to initialize widgets for dataset: {selected_dataset}")