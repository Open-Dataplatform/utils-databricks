import pytest
from typing import Any

from custom_utils import Validator, Config
from databricks.sdk.runtime import dbutils

class TestValidator:
    def setup_method(self):
        self.validator: Validator = Validator(config=Config(dbutils=dbutils))
    
    def teardown_method(self):
        del self.validator
    
    def test_check_and_exit(self):
        assert None == self.validator.check_and_exit()
    
    @pytest.mark.parametrize("namespace", [locals(), globals()])
    def test_unpack(self, namespace: dict[str, Any]):
        initial_namespace = namespace.copy()
        self.validator.unpack(namespace=namespace)
        assert initial_namespace != namespace