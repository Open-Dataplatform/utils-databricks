"""Code to mount storage to the Databricks file system"""

def _get_environment(dbutils):
    try:
        env = dbutils.widgets.get('environment')
    except:
        env = 'test'

    return env


def _get_mount_point(dbutils):
    env = _get_environment(dbutils)
    mount_point = f"/mnt/dp_{env}"

    return mount_point


def _construct_config_and_mount(dbutils):
    """Checks environment, reads service principals, and mounts."""
    env = _get_environment(dbutils)
    if env not in ['test', 'prod']:
        raise Exception(f'The environment {env =  } is invalid. It should be either "test" or "prod"!')

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


def mount(dbutils):
    """Mounts storage to /mnt/dp_{env} if it is not yet mounted. Returns mount point."""
    mount_point = _get_mount_point(dbutils)

    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        pass
    else:
        print(f"Mounting {mount_point}...")
        _construct_config_and_mount(dbutils)

    return mount_point
