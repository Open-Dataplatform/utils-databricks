from .dbutils_mocker import dbutils_mocker
import os 
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

dbutils: dbutils_mocker = dbutils_mocker()

__all__: list[str] = ["dbutils_mocker", "dbutils"]