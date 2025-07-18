import pytest
from typing import Any, Callable
from custom_utils.adf import get_parameter

from .test_utils.dbutils_mocker import dbutils_mocker, dbutils

class Testadf:
    def setup_method(self):
        self.dbutils: dbutils_mocker = dbutils
        self.param_name: str = "TestParam"

    def teardown_method(self):
        del self.dbutils
        del self.param_name

    @pytest.mark.parametrize("value", ["test_string", str(2), str(None)])
    def test_get_parameter(self, value: Any):
        return_value: Any = get_parameter(dbutils=self.dbutils,
                                          parameter_name=self.param_name,
                                          default_value=value)
        assert self.param_name in self.dbutils.widgets.getAll()
        assert return_value == value