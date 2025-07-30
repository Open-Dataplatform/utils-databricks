import pytest
from shutil import rmtree
from datetime import datetime
from pathlib import Path
from custom_utils import Config
from custom_utils.file_handler.file_handler import FileHandler

from ..test_utils.dbutils_mocker import dbutils_mocker, dbutils
from ..test_utils.data_generation import generate_files

class TestFileHandler:
    def setup_method(self):
        self.dbutils: dbutils_mocker = dbutils
        self.dbutils.widgets.dropdown("FileType", "json", ["json", "xml", "xlsx"])
        self.dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest")
        self.dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest")
        self.dbutils.widgets.text("SourceContainer", "landing")
        self.dbutils.widgets.text("SourceDatasetidentifier", "custom_utils_test_data")
        self.dbutils.widgets.text("SourceFileName", "custom_utils_test_data*")
        self.dbutils.widgets.text("KeyColumns", "A")
        self.dbutils.widgets.text("DepthLevel", "")
        self.dbutils.widgets.text("SchemaFolderName", "schemachecks")

        self.data_path : Path = Path(__file__).parent/self.dbutils.widgets.get("SourceDatasetidentifier")/self.dbutils.widgets.get("SourceStorageAccount") \
            /self.dbutils.widgets.get("SourceDatasetidentifier")

        self.dbutils.widgets.text("unittest_data_path", str(self.data_path.parent.parent))

        generate_files(self.data_path)
        self.config = Config(dbutils=self.dbutils)
        self.config.unpack(globals())
        self.file_handler: FileHandler = FileHandler(config=self.config)
        
    def teardown_method(self):
        rmtree(self.data_path.parent.parent)
        del self.config
        del self.file_handler
        del self.dbutils
        del self.data_path
        
    def test_manage_paths(self):
        returned_paths: dict[str, str] = self.file_handler.manage_paths()
        
        expected_paths: dict[str, str] = {'data_base_path': str(self.data_path),
                                          'schema_base_path': str((self.data_path.parent/"schemachecks")/self.data_path.name)}
        assert returned_paths == expected_paths
        
    def test_directory_exists(self):
        exists: bool = self.file_handler.directory_exists(str(self.data_path))
        assert exists == True
        #print(self.dbutils.fs.ls("asdfhsjkafdkljshsfa"))
        #exists_not: bool = self.file_handler.directory_exists(f"directory_does_not_exist_{datetime.now()}")
        #with pytest.raises(FileNotFoundError) as except_info:
        exists_not: bool = self.file_handler.directory_exists(f"directory_does_not_exist_{datetime.now()}")
        assert exists_not == False
            
        with pytest.raises(Exception) as except_info:
            error: bool = self.file_handler.directory_exists(None)
            assert error == False
            
    def test_normalize_path(self):
        normalized_path: str = self.file_handler.normalize_path(str(self.data_path))
        assert normalized_path == f"/{str(self.data_path).lstrip('/')}"
