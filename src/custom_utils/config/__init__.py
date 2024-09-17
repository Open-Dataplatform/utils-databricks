# File: custom_utils/config/__init__.py

from .config import Config

# Automatically initialize the Config object if `dbutils` is available in the global scope
if "dbutils" in globals():
    config = Config.initialize(dbutils=dbutils, debug=True)  # Initialize Config
    config.initialize_spark()  # Initialize Spark session within Config

    # Optionally unpack config attributes into globals (use cautiously)
    config.unpack(globals())