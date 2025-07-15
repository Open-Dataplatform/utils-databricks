import pytest

from pathlib import Path
from custom_utils import DataFrameTransformer, Config
from shutil import rmtree
from pyspark.sql import DataFrame
from typing import Generator
import json

from .utils import get_init_df, get_flat_df, count_json_rows, equal_dataframes
from ..test_utils.data_generation import generate_files
from ..test_utils.dbutils_mocker import dbutils_mocker, dbutils

#@pytest.mark.parametrize("format", ("json",))
class TestDataFrameTransformer:
    #@pytest.fixture(scope="function", autouse=True)
    def setup_method(self):
        
        self.dbutils: dbutils_mocker = dbutils
        self.dbutils.widgets.dropdown("FileType", "json", ["json", "xml", "xlsx"])
        self.dbutils.widgets.text("SourceStorageAccount", "dplandingstoragetest")
        self.dbutils.widgets.text("DestinationStorageAccount", "dpuniformstoragetest")
        self.dbutils.widgets.text("SourceContainer", "landing")
        self.dbutils.widgets.text("SourceDatasetidentifier", f"custom_utils_test_data_{format}")
        self.dbutils.widgets.text("SourceFileName", "custom_utils_test_data*")
        self.dbutils.widgets.text("KeyColumns", "data_A")
        self.dbutils.widgets.text("DepthLevel", "")
        if format == "json":
            self.dbutils.widgets.text("SchemaFolderName", "schemachecks")
        if format == "xml":
            self.dbutils.widgets.text("XmlRootName", "data")

        self.data_path : Path = Path(__file__).parent/self.dbutils.widgets.get("SourceDatasetidentifier")/self.dbutils.widgets.get("SourceStorageAccount") \
            /self.dbutils.widgets.get("SourceDatasetidentifier")

        self.dbutils.widgets.text("unittest_data_path", str(self.data_path.parent.parent))

        
        generate_files(self.data_path, format=format)
        self.config = Config(dbutils=self.dbutils)
        self.config.unpack(globals())
        self.transformer: DataFrameTransformer = DataFrameTransformer(config=self.config, debug=True)
        
        #yield

    def teardown_method(self):
        rmtree(self.data_path.parent.parent)
        del self.transformer
        del self.config
        del self.dbutils
        del self.data_path
        
  
    def test_process_and_flatten_data(self):
        depth_level: int = ''
        df_init, df_flat = self.transformer.process_and_flatten_data(depth_level=depth_level)
        df_init_static: DataFrame = get_init_df()
        df_flat_static: DataFrame = get_flat_df()
        
        assert isinstance(df_init, DataFrame) and isinstance(df_flat, DataFrame)
        df_init_test: DataFrame = df_init.drop("input_file_name")
        df_flat_test: DataFrame = df_flat.drop("input_file_name")
        assert equal_dataframes(df_init_test, df_init_static, "data")
        assert equal_dataframes(df_flat_test, df_flat_static, "data_A")
        assert self._test_input_file_name(df_init, flat_df = False)
        assert self._test_input_file_name(df_flat, flat_df = True)
    
    def _test_input_file_name(self, df: DataFrame, flat_df: bool = False) -> bool:
        assert "input_file_name" in df.columns
        paths: Generator[Path] = Path(self.data_path).glob('**/*.json')
        for path in paths:
            df_count: int = df.where(df.input_file_name.like("%"+str(path).replace('\\', '/'))).count()
            if flat_df:
                with open(path, "rb") as f:
                    data = json.load(f)
                json_count: int = count_json_rows(data)
                if json_count != df_count:
                    return False
            else:
                if not df_count > 0:
                    return False
        return True
    
    def test_rename_and_process(self):
        depth_level: str = ""
        _, df_flat = self.transformer.process_and_flatten_data(depth_level=depth_level)
        columns: list[str] = df_flat.columns
        column_mappings: dict[str, str] = {i: i.strip("data_") for i in columns}
        type_mappings: dict[str, str] = {}
        for value in column_mappings.values():
            type_mappings[value] = "string"
        reanmed_df: DataFrame = self.transformer.rename_and_process(df_flat, column_mapping=column_mappings, cast_columns=type_mappings)
        new_cols: list[str] = reanmed_df.columns
        assert list(column_mappings.values()).sort() == new_cols.sort()
        expected_datatypes: list[StringType] = [StringType()]*len(new_cols)
        mapped_datatypes: list[Any] = [field.dataType for field in reanmed_df.schema.fields]
        assert expected_datatypes == mapped_datatypes
        