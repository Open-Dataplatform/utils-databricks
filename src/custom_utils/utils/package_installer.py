import logging
import subprocess
import sys
from contextlib import contextmanager
from io import StringIO
import pkg_resources

class PackageInstaller:
    def __init__(self, logger=None, debug: bool = False):
        """
        Initializes the PackageInstaller with a logger and debug mode.

        Args:
            logger: Logger instance for logging activities.
            debug (bool): Enables debug-level logging.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.debug = debug
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

    @contextmanager
    def suppress_output(self):
        """
        Context manager to suppress stdout and stderr.
        """
        new_stdout, new_stderr = StringIO(), StringIO()
        old_stdout, old_stderr = sys.stdout, sys.stderr
        try:
            sys.stdout, sys.stderr = new_stdout, new_stderr
            yield
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr

    def get_installed_version(self, package_name):
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

    def run_pip_command(self, command):
        """
        Run a pip command using subprocess.

        Args:
            command (str): The pip command to run.
        """
        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error running pip command: {e}")
            raise SystemExit("Pip command failed.")

    def install_package(self, git_project, package_name, desired_version, uninstall_existing=False):
        """
        Install a specific version of a package from a GitHub repository.

        Args:
            git_project (str): The GitHub project path.
            package_name (str): The name of the package to install.
            desired_version (str): The version of the package to install.
            uninstall_existing (bool): Whether to uninstall the existing package before installation.
        """
        version = f"v{desired_version.lstrip('v')}"

        # Step 1: Uninstall the existing package if requested.
        if uninstall_existing:
            installed_version = self.get_installed_version(package_name)
            if installed_version:
                self.logger.info(f"Uninstalling the existing version {installed_version} of {package_name}...")
                try:
                    self.run_pip_command(f"pip uninstall {package_name} -y")
                    self.logger.info(f"Uninstallation of {package_name} completed successfully.")
                except Exception as e:
                    self.logger.error(f"Error during uninstallation: {e}")
                    raise SystemExit("Uninstallation failed. Aborting process.")

        # Step 2: Attempt to install the specified version from GitHub.
        try:
            self.logger.info(f"Attempting to install {package_name} version {version} from {git_project}...")
            self.run_pip_command(f"pip install git+https://github.com/{git_project}.git@{version}")
            self.logger.info(f"Installation of {package_name} version {version} was successful.")
        except Exception as e:
            self.logger.warning(f"Failed to install version {version}. Error: {e}")
            self.logger.info(f"Falling back to installing from the 'main' branch...")

            # Step 3: Fallback to the main branch if the version installation fails.
            try:
                self.run_pip_command(f"pip install git+https://github.com/{git_project}.git@main")
                self.logger.info(f"Fallback installation of {package_name} from the 'main' branch was successful.")
            except Exception as fallback_error:
                self.logger.error(f"Failed to install from the 'main' branch. Error: {fallback_error}")
                raise SystemExit("Installation aborted due to errors.")

        # Final log to indicate process completion.
        self.logger.info("Process completed. Please restart the environment to verify the installation.")