import pytest
from typing import Any
from pathlib import Path
from os import getcwd
from uuid import uuid4
from databricks.sdk.runtime import dbutils
from custom_utils import DataStorageManager

class TestDataStorageManager:
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        self.storage_manager: DataStorageManager = DataStorageManager(logger=None)
        self.dbutils: dbutils = dbutils()
        
    def teardown_method(self, method: callable):
        print(f"Tearing down {method}")
        del self.storage_manager
    
    def test_ensure_path_exists(self):
        existing_filepath: Path = Path(str(__file__))
        existing_dirpath: Path = existing_filepath.parent
        unexisting_path: Path = existing_dirpath/str(uuid4())
        while unexisting_path.exists():
            unexisting_path: Path = existing_dirpath/str(uuid4())

        runtime_error_path = None # Used to provoke a runtime error.
        assert None == self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                            destination_path=str(existing_filepath))
        assert None == self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                           destination_path=str(existing_dirpath))
        self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                destination_path=str(unexisting_path))
        assert unexisting_path.exists()
        unexisting_path.rmdir()
        with pytest.raises(RuntimeError) as exc_info:
            self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                    destination_path=runtime_error_path)

    @pytest.mark.parametrize("key_columns, expected_returns", [('hello, there', ['hello', 'there']), (['hello', 'there'], ['hello', 'there'])])
    def test_normalize_key_columns(self, key_columns, expected_returns):
        return_list: list[str] = self.storage_manager.normalize_key_columns(key_columns)
        assert return_list == expected_returns
        
    def test_create_or_replace_table(self):
        pass