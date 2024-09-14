from .config import Config, initialize_config, initialize_notebook

# Try to get dbutils if available in the global scope
dbutils = globals().get("dbutils", None)