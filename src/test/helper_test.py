# from unittest import TestCase
# from unittest.mock import patch, Mock

# from src.custom_utils.helper import (
#     write_message,
#     exit_notebook,
#     get_adf_parameter,
#     get_param_value,
#     get_key_columns_list
# )

# from databricks.sdk.runtime import dbutils

# class helperTest(TestCase):
#     def setUp(self):
#         return super().setUp()
    
#     @patch("builtins.print")
#     def test_write_message(self, mock_print):
#         text_string: str = "Hello"
#         write_message(text_string)
#         mock_print.assert_called_with(text_string)
        
#         import sys
#         sys.stdout.write(f"{str(mock_print.call_args)} \n")
#         sys.stdout.write(f"{str(mock_print.call_args_list)} \n")
        
#     def test_exit_notebook(self):
#         message: str = "Testing System Exit"
#         with self.assertRaises(SystemExit) as cm:
#             exit_notebook(message)
#         self.assertEqual(cm.exception.code, 1)
        
#     def test_get_adf_parameter(self):
#         dbutils: Mock = Mock()
#         param_name: str = "existing_param"
#         return_value = get_adf_parameter(dbutils=dbutils, param_name=param_name)
#         self.assertEqual(return_value, '')
        
#         with self.assertRaises(Exception) as cm:
#             dbutils.side_effect(KeyError)
#             return_value = get_adf_parameter(dbutils=dbutils)
        