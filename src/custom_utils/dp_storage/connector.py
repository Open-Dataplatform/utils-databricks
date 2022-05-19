"""Code to mount storage to the Databricks file system"""

from datetime import datetime


def _get_environment(dbutils):
    try:
        env = dbutils.widgets.get('environment')
    except:
        print("Environment is not set. Defaulting to test.")
        env = 'test'

    return env


def _generate_test_mount_point(storage_account):
    return f'/mnt/{storage_account}test'


def _generate_prod_mount_point(dbutils, storage_account: str):
    timestamp = datetime.strftime(datetime.utcnow(), '%y%m%dT%H%M%SZ')
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_name = notebook_path.split('/')[-1]

    mount_point = f'/mnt/{storage_account}prod_{timestamp}_{notebook_name}'

    return mount_point


def _get_mount_point(dbutils, storage_account: str):
    env = _get_environment(dbutils)

    if env == 'test':
        mount_point = _generate_test_mount_point(storage_account)
    elif env == 'prod':
        mount_point = _generate_prod_mount_point(dbutils, storage_account)
    else:
        raise Exception(f'The environment {env =  } is invalid. It should be either "test" or "prod"!')

    return mount_point


def _is_test_mounted(dbutils, storage_account):
    mount_point = _generate_test_mount_point(storage_account)
    return any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())


def _do_mount(dbutils, storage_account: str, container: str):
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

    print(f"Mount point ({mount_point = }) is ready")
    return mount_point


def mount(dbutils, source_config, destination_config):
    """Mounts storage to /mnt/dp_{env} if it is not yet mounted. Returns mount point."""

    containers_to_mount = _list_containers_to_mount(source_config, destination_config)
    mount_point_dict = _mount_all_containers(dbutils, containers_to_mount)

    source_config = _add_mount_points_to_config(source_config, mount_point_dict)
    destination_config = _add_mount_points_to_config(destination_config, mount_point_dict)

    return source_config, destination_config


def _add_mount_points_to_config(config, mount_point_dict):
    """Adds mount points to every dataset config in the input config"""
    for data_config in config.values():
        if data_config['type'] == 'adls':
            data_config['mount_point'] = mount_point_dict[(data_config['account'], data_config['container'])]

    return config


def _mount_all_containers(dbutils, containers_to_mount):
    """"Mounts all containers and returns dictionary with {(<account>, <container>): <mount_points>, ...}."""
    env = _get_environment(dbutils)

    mount_points = {}
    for storage_account, container in containers_to_mount:

        # If running in test environment and test storage is already mounted.
        if (env == 'test') and _is_test_mounted(dbutils, storage_account):
            mount_point = _generate_test_mount_point(storage_account)
        else:
            print("Mounting...")
            mount_point = _do_mount(dbutils, storage_account, container)

        mount_points[(storage_account, container)] = mount_point

    return mount_points


def _list_containers_to_mount(source_config, destination_config):
    """Returns list with ("<account>", "<container>") for all combinations of accounts/containers in the input configs."""

    containers_to_mount = set()

    dataset_configs = list(source_config.values()) + list(destination_config.values())
    for data_config in dataset_configs:
        if data_config['type'] == 'adls':
            containers_to_mount.add((data_config['account'], data_config["container"]))

    return list(containers_to_mount)


def _list_mount_points(source_config, destination_config):
    """Returns list with all mount points."""

    containers_to_mount = set()

    combined_configs = {**source_config, **destination_config}
    for data_config in combined_configs.values():
        if data_config['type'] == 'adls':
            containers_to_mount.add(data_config['mount_point'])

    return list(containers_to_mount)


def unmount_if_prod(dbutils, source_config, destination_config):
    """If running in prod, unmount storage"""
    env = _get_environment(dbutils)

    if env == 'prod':
        mount_points = _list_mount_points(source_config, destination_config)
        for mount_point in mount_points:
            dbutils.fs.unmount(mount_point)
