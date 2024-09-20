import logging
import subprocess
import sys
from contextlib import contextmanager
from io import StringIO
import pkg_resources

# Set up logging to capture only necessary information with INFO level
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Suppress logs from py4j (which generates excess messages).
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

# Context manager to suppress stdout and stderr
@contextmanager
def suppress_output():
    new_stdout, new_stderr = StringIO(), StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_stdout, new_stderr
        yield
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr

def get_installed_version(package_name):
    """
    Get the installed version of a package.
    Args:
        package_name (str): The name of the package.
    Returns:
        str: The installed version or None if the package is not installed.
    """
    try:
        package_info = pkg_resources.get_distribution(package_name)
        return package_info.version
    except pkg_resources.DistributionNotFound:
        return None

def run_pip_command(command):
    """
    Run a pip command using subprocess.
    Args:
        command (str): The pip command to run.
    """
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running pip command: {e}")
        raise SystemExit("Pip command failed.")

def install_package(git_project, package_name, desired_version, uninstall_existing=False):
    """
    Install a specific version of a package from a GitHub repository in a Databricks notebook environment.
    
    Args:
        git_project (str): The GitHub project path (e.g., 'xazms/utils-databricks').
        package_name (str): The name of the package to install (e.g., 'databricks-custom-utils').
        desired_version (str): The version of the package to install (e.g., 'v0.6.4').
        uninstall_existing (bool): Whether to uninstall the existing package before installation.

    The function will:
    1. Uninstall the existing package (if `uninstall_existing=True`).
    2. Attempt to install the specified version from GitHub.
    3. If the specified version is not found, it will fallback to the 'main' branch.
    4. Log successful installation.
    """

    # Ensure the version is normalized with a "v" prefix for GitHub version tags.
    version = f"v{desired_version.lstrip('v')}"

    # Step 1: Uninstall the existing package if requested.
    if uninstall_existing:
        installed_version = get_installed_version(package_name)
        if installed_version:
            logger.info(f"Uninstalling the existing version {installed_version} of {package_name}...")
        else:
            logger.info(f"{package_name} is not currently installed.")

        try:
            # Use subprocess to run the pip uninstall command
            run_pip_command(f"pip uninstall {package_name} -y")
            if installed_version:
                logger.info(f"Uninstallation of {package_name} version {installed_version} completed successfully.")
            else:
                logger.info(f"Uninstallation completed successfully.")
        except Exception as uninstall_error:
            logger.error(f"Error during uninstallation: {uninstall_error}")
            raise SystemExit("Uninstallation failed. Aborting process.")

    # Step 2: Attempt to install the specified version from GitHub.
    try:
        logger.info(f"Attempting to install {package_name} version {version} from {git_project}...")
        # Use subprocess to run the pip install command for the specific version
        run_pip_command(f"pip install git+https://github.com/{git_project}.git@{version}")
        logger.info(f"Installation of {package_name} version {version} was successful.")
    except Exception as e:
        logger.warning(f"Failed to install version {version}. Error: {e}")
        logger.info(f"Falling back to installing from the 'main' branch...")

        # Step 3: Fallback to the main branch if the version installation fails.
        try:
            run_pip_command(f"pip install git+https://github.com/{git_project}.git@main")
            logger.info(f"Fallback installation of {package_name} from the 'main' branch was successful.")
        except Exception as fallback_error:
            logger.error(f"Failed to install from the 'main' branch. Error: {fallback_error}")
            raise SystemExit("Installation aborted due to errors.")
    
    # Final log to indicate process completion.
    logger.info("Process completed. Please restart the environment to verify the installation.")