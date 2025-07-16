import pytest
from pathlib import Path

from uuid import uuid4
from custom_utils import DataStorageManager
from ..test_utils.dbutils_mocker import dbutils_mocker, dbutils
class TestDataStorageManager:
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        self.storage_manager: DataStorageManager = DataStorageManager(logger=None)
        self.dbutils: dbutils_mocker = dbutils

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
        # Ensure file exists.
        assert None == self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                            destination_path=str(existing_filepath))
        # Ensure diretory exists.
        assert None == self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                           destination_path=str(existing_dirpath))
        
        # Ensure exception is thrown, when path does not exist.
        with pytest.raises(RuntimeError) as exc_info:
            self.storage_manager.ensure_path_exists(dbutils=self.dbutils,
                                                    destination_path=runtime_error_path)

    @pytest.mark.parametrize("key_columns, expected_returns", [('hello, there', ['hello', 'there']), (['hello', 'there'], ['hello', 'there'])])
    def test_normalize_key_columns(self, key_columns, expected_returns):
        return_list: list[str] = self.storage_manager.normalize_key_columns(key_columns)
        assert return_list == expected_returns