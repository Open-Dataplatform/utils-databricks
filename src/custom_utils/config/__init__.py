# File: custom_utils/config/__init__.py

from .config import Config, initialize_config, initialize_notebook  # Import Config, initialize_config, and initialize_notebook

# Automatically call the initialize_notebook function when this module is imported
if "dbutils" in globals() in globals():
    spark, config, logger = initialize_notebook(dbutils)