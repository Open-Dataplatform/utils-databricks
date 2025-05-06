import pytest

from typing import Any

from .test_utils import dbutils
from ..custom_utils.adf import (
    get_parameter,
    #get_config_parameter,
    #get_source_config,
    #get_destination_config,
    is_executed_by_adf
)

class testadf:
    
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        self.dbutils: dbutils = dbutils()
        self.param_name: str = "TestParam"

    def teardown_method(self, method: callable):
        print(f"Tearing down {method}")
        del self.dbutils
        del self.param_name

    @pytest.mark.parametrize("value", ["test_string", 2, None])
    def test_get_parameter(self, value: Any):
        return_value: Any = get_parameter(dbutils=self.dbutils,
                                          parameter_name=self.param_name,
                                          default_value=value)
        assert self.param_name in self.dbutils.widgets
        assert return_value == value

    # def test_get_config_parameter(self):
        