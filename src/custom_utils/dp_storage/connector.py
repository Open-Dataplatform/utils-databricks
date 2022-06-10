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


def get_mount_point_name(storage_account: str) -> str:
    return f'/mnt/{storage_account}'


def _is_mounted(dbutils, storage_account: str) -> bool:
    """Checks whether the storage account is mounted"""
    mount_point = get_mount_point_name(storage_account)
    return any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())


def _get_mount_config(dbutils) -> dict:
    """Returns config to be used in dbutils.fx.mount(..., extra_configs=<>)"""
    env = _get_environment(dbutils)

    tenant_id = dbutils.secrets.get(scope="shared-key-vault",key="tenantid")
    client_id = dbutils.secrets.get(scope="shared-key-vault",key=f"clientid-databricks-sp-{env}")
    client_secret =  dbutils.secrets.get(scope="shared-key-vault",key=f"pwd-databricks-sp-{env}")

    config = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    return config


def _check_account_or_container_name(name):
    if ('<' in name) or ('>' in name):
        raise ValueError(f'{name} is not a valid storage account or container. Make sure to update the source and destination configs.')


def _do_mount(dbutils, storage_account: str, container: str) -> str:
    """Reads service principals and mounts storage_account."""

    _check_account_or_container_name(storage_account)
    _check_account_or_container_name(container)

    mount_config = _get_mount_config(dbutils)
    mount_point = get_mount_point_name(storage_account)

    dbutils.fs.mount(source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
                     mount_point=mount_point,
                     extra_configs=mount_config)

    logger.info("Mount point (%s) is ready", mount_point)
    return mount_point


def mount(dbutils, source_config: dict, destination_config: dict):
    """Mounts storage for all ADLS datasets in both source and destination."""

    containers_to_mount = _list_containers_to_mount(source_config, destination_config)
    _mount_all_containers(dbutils, containers_to_mount)


def _mount_all_containers(dbutils, containers_to_mount: List[tuple]):
    """"Mounts all containers and returns dictionary with {(<account>, <container>): <mount_points>, ...}."""

    for storage_account, container in containers_to_mount:
        if _is_mounted(dbutils, storage_account):
            logger.info("%s@%s is already mounted", container, storage_account)
        else:
            logger.info("Mounting %s@%s", container, storage_account)
            _do_mount(dbutils, storage_account, container)


def _list_containers_to_mount(source_config, destination_config: dict) -> List[tuple]:
    """Returns list with ("<account>", "<container>") for all combinations of accounts/containers in the input
    configs."""

    containers_to_mount = set()

    dataset_configs = list(source_config.values()) + list(destination_config.values())
    for data_config in dataset_configs:
        if data_config['type'] == 'adls':
            containers_to_mount.add((data_config['account'], data_config["container"]))

    return list(containers_to_mount)
