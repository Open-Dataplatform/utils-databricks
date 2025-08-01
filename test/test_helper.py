import pytest
from unittest.mock import patch
from custom_utils.helper import (
    write_message,
    exit_notebook,
    get_adf_parameter,
    get_key_columns_list
)

from .test_utils.dbutils_mocker import dbutils_mocker, dbutils

class Testhelper:
    @patch("builtins.print")
    def test_write_message(self, mock_print):
        text_string: str = "Hello"
        write_message(text_string)
        mock_print.assert_called_with(text_string)
        
        import sys
        sys.stdout.write(f"{str(mock_print.call_args)} \n")
        sys.stdout.write(f"{str(mock_print.call_args_list)} \n")
        
    def test_exit_notebook(self):
        message: str = "Testing System Exit"
        with pytest.raises(RuntimeError) as cm:
            exit_notebook(message)
        
    def test_get_adf_parameter(self):
        dbutil: dbutils_mocker = dbutils
        param_name: str = "existing_param"
        return_value = get_adf_parameter(dbutils=dbutil, param_name=param_name)
        assert return_value == ''
        
        with pytest.raises(Exception) as cm:
            return_value = get_adf_parameter(dbutils=dbutils)
            
    def test_get_key_columns_list(self):
        key_columns: str = "uuid, timestamp, area"
        return_list: list[str] = get_key_columns_list(key_columns=key_columns)
        assert return_list == ["uuid", "timestamp", "area"]
        with pytest.raises(ValueError) as excinfo_valueerror:
            raise_return = get_key_columns_list('')
            assert excinfo_valueerror == "ERROR: No KeyColumns defined!"
            assert raise_return is None