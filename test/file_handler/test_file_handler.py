import pytest
from custom_utils.file_handler.file_handler import FileHandler
from ..test_utils.dbutils_mocker import dbutils_mocker, dbutils
from custom_utils import Config
from ..test_utils.data_generation import generate_files
from datetime import datetime

class TestFileHandler:
    def setup_method(self, method: callable):
        print(f"Setting up {method}")
        
        # Assuming Config is a class that needs to be instantiated for FileHandler
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

    def teardown_method(self, method: callable):
        print(f"Tearing down {method}")
        del self.file_handler

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