from typing import Dict, Optional
from custom_utils.logging.logger import Logger

class WidgetInstaller:
    def __init__(self, dbutils, logger: Logger, debug: bool = False):
        """
        Initializes the WidgetInstaller with dbutils and logging options.

        Args:
            dbutils: The Databricks utilities object.
            logger (Logger): Logger instance for logging activities.
            debug (bool): Enables debug-level logging.
        """
        self.dbutils = dbutils
        self.logger = logger
        self.debug = debug
        self.logger.update_debug_mode(debug)

    def _log(self, message: str, level: str = "info"):
        """
        Helper method for logging messages based on debug mode.

        Args:
            message (str): The message to log.
            level (str): The log level ("info" or "debug").
        """
        if self.debug and level == "debug":
            self.logger.log_message(message, level="debug")
        elif level == "info":
            self.logger.log_message(message, level="info")

    def initialize_common_widgets(self):
        """
        Initializes common widgets that are the same across all datasets.
        """
        try:
            self._log("Initializing common widgets.", level="debug")

            # Initialize common widgets
            self.dbutils.widgets.dropdown("FileType", "json", ["json", "xml", "xlsx"], "Select File Type")
            self.dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest", "Source Storage Account")
            self.dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest", "Destination Storage Account")
            self.dbutils.widgets.text("SourceContainer", "landing", "Source Container")

            # Ensure SourceDatasetidentifier exists
            try:
                self.dbutils.widgets.get("SourceDatasetidentifier")
            except Exception:
                self.dbutils.widgets.dropdown(
                    "SourceDatasetidentifier",
                    "triton__flow_plans",
                    [
                        "triton__flow_plans",
                        "cpx_so__nomination",
                        "ddp_em__dayahead_flows_nemo",
                        "pluto_pc__units_scadamw",
                        "ddp_cm__mfrr_settlement"
                    ],
                    "Select Dataset Identifier"
                )
                self._log("SourceDatasetidentifier dropdown initialized successfully.", level="info")

            self._log("Common widgets initialized successfully.", level="info")
        except Exception as e:
            self.logger.log_error(f"Error initializing common widgets: {str(e)}")
            raise RuntimeError(f"Failed to initialize common widgets: {str(e)}")

    def clear_all_widgets(self):
        """
        Removes all existing widgets to ensure a clean slate before initialization.
        """
        try:
            self._log("Clearing all existing widgets.", level="debug")
            self.dbutils.widgets.removeAll()
            self._log("All existing widgets removed successfully.", level="info")
        except Exception as e:
            self.logger.log_error(f"Error during widget cleanup: {str(e)}")
            raise RuntimeError(f"Failed to clear widgets: {str(e)}")

    def clear_widgets_except_common(self):
        """
        Clears all widgets except common ones like SourceDatasetidentifier,
        SourceStorageAccount, DestinationStorageAccount, and SourceContainer.
        """
        common_keys = ["SourceDatasetidentifier", "SourceStorageAccount", "DestinationStorageAccount", "SourceContainer"]

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
            self._log("Clearing dataset-specific widgets except common ones.", level="debug")
            for key in all_possible_keys:
                if key not in common_keys:
                    try:
                        self.dbutils.widgets.remove(key)
                        self._log(f"Removed widget: {key}", level="debug")
                    except Exception as e:
                        if "InputWidgetNotDefined" not in str(e):
                            self.logger.log_warning(f"Unexpected error while removing widget '{key}': {str(e)}")
            self._log("Cleared all dataset-specific widgets except common ones.", level="info")
        except Exception as e:
            self.logger.log_error(f"Error clearing widgets: {str(e)}")
            raise RuntimeError(f"Failed to clear widgets: {str(e)}")

    def initialize_widgets(self, selected_dataset: str):
        """
        Dynamically initializes and updates widget values based on the selected dataset.

        Args:
            selected_dataset (str): The selected dataset identifier.
        """
        try:
            self._log("Starting widget initialization.", level="debug")

            # Clear all widgets
            self.clear_all_widgets()

            # Initialize common widgets
            self.initialize_common_widgets()

            # Reinitialize SourceDatasetidentifier with selected dataset
            self.dbutils.widgets.dropdown(
                "SourceDatasetidentifier",
                selected_dataset,
                [
                    "triton__flow_plans",
                    "cpx_so__nomination",
                    "ddp_em__dayahead_flows_nemo",
                    "pluto_pc__units_scadamw",
                    "ddp_cm__mfrr_settlement"
                ],
                "Select Dataset Identifier"
            )
            self._log(f"SourceDatasetidentifier set to: {selected_dataset}", level="info")

            # Dataset-specific widget configurations
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
                    "KeyColumns": "mfrr_mRID, ts_mRID, timestamp_utc",
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

            if selected_dataset not in dataset_config:
                raise ValueError(f"Unknown dataset identifier: {selected_dataset}")

            dataset_widgets = dataset_config[selected_dataset]

            # Initialize widgets and collect debug logs
            widget_debug_logs = []
            for key, value in dataset_widgets.items():
                # Replace empty DepthLevel with "None" for better clarity
                display_value = value if key != "DepthLevel" or value else "None"
                self.dbutils.widgets.text(key, value, key)
                widget_debug_logs.append(f"{key}: {display_value}")

            # Debug block for widget initialization
            self.logger.log_block(f"Initializing Widgets for Dataset: {selected_dataset}", widget_debug_logs, level="debug")

        except Exception as e:
            self.logger.log_error(f"Error initializing widgets for dataset {selected_dataset}: {str(e)}")
            raise RuntimeError(f"Failed to initialize widgets for dataset: {selected_dataset}")