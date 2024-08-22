# dp_storage/__init__.py

from .reader import *
from .writer import *
from .config import Config, initialize_config, initialize_notebook  # Import Config, initialize_config, and initialize_notebook

# Automatically call the initialize_notebook function when this module is imported
if "dbutils" in globals() and "helper" in globals():
    spark, config = initialize_notebook(dbutils, helper)
