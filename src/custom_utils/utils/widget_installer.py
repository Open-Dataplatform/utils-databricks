"""
Module: widget_installer
Description: Provides utility functions to initialize and manage Databricks widgets.
"""

from typing import Dict, Optional
import dbutils  # Assumes running in a Databricks environment
from custom_utils.logging.logger import Logger


# ============================================================== 
# Widget Keys for Dynamic Initialization
# ==============================================================

WIDGET_KEYS = [
    "FileType",
    "SourceStorageAccount",
    "DestinationStorageAccount",
    "SourceContainer",
    "SourceFileName",
    "KeyColumns",
    "FeedbackColumn",
    "DepthLevel",
    "SchemaFolderName",
    "XmlRootName",
    "SheetName"
]


# ============================================================== 
# Initialize Common Widgets
# ==============================================================

def initialize_common_widgets(logger: Logger):
    """
    Initializes common widgets used across all datasets.

    Args:
        logger (Logger): An instance of the custom logger for structured logging.
    """
    try:
        dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
        dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")
        dbutils.widgets.text("SourceContainer", "landing", "Source Container")
        logger.log_message("Common widgets initialized successfully.")
    except Exception as e:
        logger.log_error(f"Error initializing common widgets: {str(e)}")
        raise RuntimeError(f"Failed to initialize common widgets: {str(e)}")


# ============================================================== 
# Clear Widgets Except Common Ones
# ==============================================================

def clear_widgets_except_common(logger: Logger):
    """
    Clears all widgets except common ones.

    Args:
        logger (Logger): An instance of the custom logger for structured logging.
    """
    common_keys = ["SourceDatasetidentifier", "SourceStorageAccount", "DestinationStorageAccount", "SourceContainer"]
    try:
        for key in WIDGET_KEYS:
            if key not in common_keys:
                try:
                    dbutils.widgets.remove(key)
                except Exception:
                    # Ignore if the widget doesn't exist
                    continue
        logger.log_message("Cleared dataset-specific widgets.")
    except Exception as e:
        logger.log_error(f"Error clearing widgets: {str(e)}")
        raise RuntimeError(f"Failed to clear widgets: {str(e)}")


# ============================================================== 
# Initialize Dataset-Specific Widgets
# ==============================================================

def initialize_widgets(selected_dataset: str, logger: Logger, external_params: Optional[Dict[str, str]] = None):
    """
    Dynamically initializes widgets based on the selected dataset.

    Args:
        selected_dataset (str): Identifier for the selected dataset.
        logger (Logger): An instance of the custom logger for structured logging.
        external_params (dict, optional): External parameters to override widget values.
    """
    try:
        # Step 1: Clear non-common widgets
        clear_widgets_except_common(logger)

        # Step 2: Dataset-specific widget configuration
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

        # Step 3: Get the configuration for the selected dataset
        if selected_dataset not in dataset_config:
            raise ValueError(f"Unknown dataset identifier: {selected_dataset}")
        dataset_widgets = dataset_config[selected_dataset]

        # Step 4: Initialize widgets dynamically
        for key, value in dataset_widgets.items():
            dbutils.widgets.text(key, value, key)

        # Step 5: Apply external parameters if provided
        if external_params:
            for key, value in external_params.items():
                if key in dataset_widgets:
                    dbutils.widgets.text(key, value, key)

        logger.log_message(f"Widgets initialized for dataset: {selected_dataset}")
    except Exception as e:
        logger.log_error(f"Error initializing widgets for dataset '{selected_dataset}': {str(e)}")
        raise RuntimeError(f"Failed to initialize widgets for dataset '{selected_dataset}': {str(e)}")
