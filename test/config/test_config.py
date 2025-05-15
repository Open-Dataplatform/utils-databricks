import pytest
from typing import Any

from databricks.sdk.runtime import dbutils
from custom_utils import Config

class TestConfig:
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        self.dbutils: dbutils = dbutils
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
        
if __name__ == "__main__":
    dbutils: dbutils = dbutils
    debug: bool = True
    dbutils.widgets.dropdown("FileType", "json", ["json", "xlsx", "xml"], "File Type")
    print(dbutils.widgets.get("FileType"))
    config = Config(dbutils=dbutils, debug=debug)