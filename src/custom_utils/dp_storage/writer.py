"""Functions related to writing to the Delta lake"""


def get_destination_path(destination_config):
    """Extracts destination path from destination_config"""
    data_config = list(destination_config.values())[0]
    destination_path = f'{data_config["mount_point"]}/{data_config["dataset"]}'
    return destination_path


def get_databricks_table_info(destination_config):
    """Constructs database and table names to be used in Databricks."""
    data_config = list(destination_config.values())[0]
    
    database_name = data_config['mount_point'].split('/')[-1]
    table_name = data_config['dataset']
    
    return database_name, table_name