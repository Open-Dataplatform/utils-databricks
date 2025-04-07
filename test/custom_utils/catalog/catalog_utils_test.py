from unittest import TestCase
from unittest.mock import Mock, patch
from .. import catalog
from ....src.catalog.catalog_utils import DataStorageManange

class DataStorageManagerTest(TestCase):
    def setUp(self):
        self.storage_manager: DataStorageManager = DataStorageManager()
        

# Use as template for new tests
if __name__ == '__main__':
    from pathlib import Path
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    from src.custom_utils.catalog.catalog_utils import DataStorageManager