import pytest
from typing import Any, Callable

from ..test_utils import dbutils
# from ...custom_utils.config.config import Config
from ...custom_utils import Config
class TestConfig:
    def setup_method(self, method: Callable):
        print(f"Setting up {method}")
        self.dbutils: dbutils = dbutils()
        self.debug: bool = False
        self.config = Config(dbutils=self.dbutils, debug=self.debug)

    def teardown_method(self, method: Callable):
        print(f"Tearing down {method}")
    
    def test_initialisation(self):
        with pytest.raises(RuntimeError) as excinfo_initialize:
            Config()
        with pytest.raises(RuntimeError) as excinfo_reinitialize:
            self.config.initialize()
        assert str(excinfo_initialize.value) == str(excinfo_initialize.value)
    
    @pytest.mark.parametrize("namespace", [locals(), globals()])
    def test_unpack(self, namespace: dict[str, Any]):
        initial_namespace = namespace.copy()
        self.config.unpack(namespace=namespace)
        assert initial_namespace != namespace