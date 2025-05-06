import pytest
from typing import Any
from pathlib import Path
from os import getcwd
from uuid import uuid4
from ..test_utils import dbutils
from ...custom_utils import DataStorageManager

class TestDataStorageManager:
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        self.storage_manager: DataStorageManager = DataStorageManager(logger=None)
        self.dbutils: dbutils = dbutils()
        
    def teardown_method(self, method: callable):
        print(f"Tearing down {method}")
        del self.storage_manager
    
    def test_ensure_path_exists(self):
        existing_path: Path = Path(getcwd())
        #unexisting_path: existing_path/str(uuid4())
        self.storage_manager.ensure_path_exists(dbutils=self.dbutils,destination_path=str(existing_path))