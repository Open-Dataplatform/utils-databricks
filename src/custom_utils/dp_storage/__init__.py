# dp_storage/__init__.py

from .reader import *
from .writer import *
from .config import Config, initialize_config, setup_pipeline  # Import Config, initialize_config, and setup_pipeline

# Automatically call the setup function when this module is imported
if "dbutils" in globals() and "helper" in globals():
    spark, config = setup_pipeline(dbutils, helper)
