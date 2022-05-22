"""Code to mount storage to the Databricks file system"""

from datetime import datetime
from typing import Tuple, List
import logging

from ..adf import is_executed_by_adf


logger = logging.getLogger(__name__)


def _get_environment(dbutils) -> str:
    """If executed from ADF, it reads the environment parameter."""
    if is_executed_by_adf(dbutils):
        env = dbutils.widgets.get('environment')
        assert (env == 'prod') or (env == 'test'), f"The environment parameter in ADF should be either 'test' or 'prod'. It was {env = }."
    else:
        env = 'test'

    return env


def _generate_test_mount_point(storage_account: str) -> str:
    return f'/mnt/{storage_account}test'


def _generate_prod_mount_point(dbutils, storage_account: str) -> str:
    """Generates unique mount point path."""
    timestamp = datetime.strftime(datetime.utcnow(), '%y%m%dT%H%M%SZ')
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_name = notebook_path.split('/')[-1]

    mount_point = f'/mnt/{storage_account}prod_{timestamp}_{notebook_name}'

    return mount_point


def _get_mount_point(dbutils, storage_account: str) -> str:
    env = _get_environment(dbutils)

    if env == 'test':
        mount_point = _generate_test_mount_point(storage_account)
    elif env == 'prod':
        mount_point = _generate_prod_mount_point(dbutils, storage_account)
    else:
        raise ValueError(f'The environment ({env =  }) is invalid. It should be either "test" or "prod"!')

    return mount_point


def _is_test_mounted(dbutils, storage_account: str) -> bool:
    mount_point = _generate_test_mount_point(storage_account)
    return any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())


def _do_mount(dbutils, storage_account: str, container: str) -> str:
    """Checks environment, reads service principals, and mounts."""
    env = _get_environment(dbutils)

    tenant_id = dbutils.secrets.get(scope="shared-key-vault",key="tenantid")
    client_id = dbutils.secrets.get(scope="shared-key-vault",key=f"clientid-databricks-sp-{env}")
    client_secret =  dbutils.secrets.get(scope="shared-key-vault",key=f"pwd-databricks-sp-{env}")
    account_name = f'{storage_account}{env}'

    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    mount_point = _get_mount_point(dbutils, storage_account)

    dbutils.fs.mount(source=f"abfss://{container}@{account_name}.dfs.core.windows.net/",
                     mount_point=mount_point,
                     extra_configs=configs)

    logger.info(f"Mount point ({mount_point = }) is ready")
    return mount_point


def mount(dbutils, source_config: dict, destination_config: dict) -> Tuple[dict, dict]:
    """Mounts storage for all ADLS datasets in both source and destination. Returns configs with 'mount_point' added to the
    individual dataset config dicts."""

    containers_to_mount = _list_containers_to_mount(source_config, destination_config)
    mount_point_dict = _mount_all_containers(dbutils, containers_to_mount)

    source_config = _add_mount_points_to_config(source_config, mount_point_dict)
    destination_config = _add_mount_points_to_config(destination_config, mount_point_dict)

    return source_config, destination_config


def _add_mount_points_to_config(config, mount_point_dict: dict) -> dict:
    """Adds mount points to every dataset config in the input config"""
    for data_config in config.values():
        if data_config['type'] == 'adls':
            data_config['mount_point'] = mount_point_dict[(data_config['account'], data_config['container'])]

    return config


def _mount_all_containers(dbutils, containers_to_mount: List[tuple]) -> dict:
    """"Mounts all containers and returns dictionary with {(<account>, <container>): <mount_points>, ...}."""
    env = _get_environment(dbutils)

    mount_points = {}
    for storage_account, container in containers_to_mount:

        # If running in test environment and test storage is already mounted.
        if (env == 'test') and _is_test_mounted(dbutils, storage_account):
            mount_point = _generate_test_mount_point(storage_account)
        else:
            logger.info("Mounting storage account %s and container %s", storage_account, container)
            mount_point = _do_mount(dbutils, storage_account, container)

        mount_points[(storage_account, container)] = mount_point

    return mount_points


def _list_containers_to_mount(source_config, destination_config: dict) -> List[tuple]:
    """Returns list with ("<account>", "<container>") for all combinations of accounts/containers in the input configs."""

    containers_to_mount = set()

    dataset_configs = list(source_config.values()) + list(destination_config.values())
    for data_config in dataset_configs:
        if data_config['type'] == 'adls':
            containers_to_mount.add((data_config['account'], data_config["container"]))

    return list(containers_to_mount)


def _list_mount_points(source_config, destination_config: dict) -> List[str]:
    """Returns list with all mount points."""

    containers_to_mount = set()

    combined_configs = {**source_config, **destination_config}
    for data_config in combined_configs.values():
        if data_config['type'] == 'adls':
            containers_to_mount.add(data_config['mount_point'])

    return list(containers_to_mount)


def unmount_if_prod(dbutils, source_config: dict, destination_config: dict):
    """If running in prod, unmount storage"""
    env = _get_environment(dbutils)

    if env == 'prod':
        mount_points = _list_mount_points(source_config, destination_config)
        for mount_point in mount_points:
            dbutils.fs.unmount(mount_point)
            logger.info(f"Successfully unmounted mount_point = %s", mount_point)
