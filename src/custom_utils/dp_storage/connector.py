"""Code to mount storage to the Databricks file system"""

import uuid

def _get_environment(dbutils):
    try:
        env = dbutils.widgets.get('environment')
    except:
        print("Environment is not set. Defaulting to test.")
        env = 'test'

    return env


def _generate_test_mount_point():
    return "/mnt/dp_test"


def _generate_prod_mount_point():
    return f"/mnt/dp_prod_{uuid.uuid4()}"


def _get_mount_point(dbutils):
    env = _get_environment(dbutils)

    if env == 'test':
        mount_point = _generate_test_mount_point()
    elif env == 'prod':
        mount_point = _generate_prod_mount_point()
    else:
        raise Exception(f'The environment {env =  } is invalid. It should be either "test" or "prod"!')

    return mount_point


def _is_test_mounted(dbutils):
    mount_point = _generate_test_mount_point()
    return any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())


def _construct_config_and_mount(dbutils):
    """Checks environment, reads service principals, and mounts."""
    env = _get_environment(dbutils)

    tenant_id = dbutils.secrets.get(scope="shared-key-vault",key="tenantid")
    client_id = dbutils.secrets.get(scope="shared-key-vault",key=f"clientid-databricks-sp-{env}")
    client_secret =  dbutils.secrets.get(scope="shared-key-vault",key=f"pwd-databricks-sp-{env}")
    account_name = dbutils.secrets.get(scope="shared-key-vault",key=f"accountname-storage-{env}")
    container = "datasets"

    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    mount_point = _get_mount_point(dbutils)

    dbutils.fs.mount(source=f"abfss://{container}@{account_name}.dfs.core.windows.net/",
                     mount_point=mount_point,
                     extra_configs=configs)

    print(f"Mount point ({mount_point = }) is ready")
    return mount_point


def mount(dbutils):
    """Mounts storage to /mnt/dp_{env} if it is not yet mounted. Returns mount point."""
    env = _get_environment(dbutils)

    # If running in test environment and test storage is already mounted.
    if (env == 'test') and _is_test_mounted(dbutils):
        pass
    else:
        print("Mounting...")
        mount_point = _construct_config_and_mount(dbutils)

    return mount_point


def unmount_if_prod(mount_point, dbutils):
    """If running in prod, unmount storage"""
    env = _get_environment(dbutils)

    if env == 'prod':
        dbutils.fs.unmount(mount_point)
