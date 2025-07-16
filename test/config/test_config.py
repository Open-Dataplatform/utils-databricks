import pytest
from typing import Any

# from databricks.sdk.runtime import dbutils
from custom_utils import Config
from ..test_utils.dbutils_mocker import dbutils

class TestConfig:
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        self.dbutils = dbutils
        # set_getAll(self.dbutils)
        self.dbutils.widgets.dropdown("FileType", "json", ["json", "xml", "xlsx"])
        self.dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest")
        self.dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest")
        self.dbutils.widgets.text("SourceContainer", "landing")
        self.dbutils.widgets.text("SourceDatasetidentifier", "custom_utils_test_data")
        self.dbutils.widgets.text("SourceFileName", "custom_utils_test_data*")
        self.dbutils.widgets.text("KeyColumns", "A")
        self.dbutils.widgets.text("DepthLevel", "8")
        self.dbutils.widgets.text("SchemaFolderName", "schemachecks")
        self.debug: bool = False
        self.config = Config(dbutils=self.dbutils, debug=self.debug)

    def teardown_method(self, method: callable):
        print(f"Tearing down {method}")
        del self.dbutils
        del self.debug
        del self.config
    
    def test_initialisation(self):
        with pytest.raises(RuntimeError) as excinfo_initialize:
            Config()
        with pytest.raises(RuntimeError) as excinfo_reinitialize:
            self.config.initialize()
        assert str(excinfo_initialize.value) == str(excinfo_reinitialize.value)
    
    @pytest.mark.parametrize("namespace", [locals(), globals()])
    def test_unpack(self, namespace: dict[str, Any]):
        initial_namespace = namespace.copy()
        self.config.unpack(namespace=namespace)
        assert initial_namespace != namespace

    def test_widgets_and_parameter_initialisation(self):
        assert self.config.file_type == self.dbutils.widgets.get("FileType")
        assert self.config.source_environment == self.dbutils.widgets.get("SourceStorageAccount")
        assert self.config.destination_environment == self.dbutils.widgets.get("DestinationStorageAccount")
        assert self.config.source_container == self.dbutils.widgets.get("SourceContainer")
        assert self.config.source_datasetidentifier == self.dbutils.widgets.get("SourceDatasetidentifier")
        assert self.config.source_filename == self.dbutils.widgets.get("SourceFileName")
        assert self.config.key_columns == self.dbutils.widgets.get("KeyColumns")
        assert self.config.depth_level == self.dbutils.widgets.get("DepthLevel")
        assert self.config.schema_folder_name == self.dbutils.widgets.get("SchemaFolderName")