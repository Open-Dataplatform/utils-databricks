import logging
import subprocess
import sys
from contextlib import contextmanager
from io import StringIO
import pkg_resources

class PackageInstaller:
    def __init__(self, debug: bool = False):
        """
        Initializes the PackageInstaller with a basic logger.

        Args:
            debug (bool): Enables debug-level logging if set to True.
        """
        self.debug = debug
        self.logger = self._initialize_logger()

    def _initialize_logger(self):
        """
        Sets up a basic logger for the class.

        Returns:
            Logger: A configured logger instance.
        """
        logger = logging.getLogger("PackageInstaller")
        logger.setLevel(logging.DEBUG if self.debug else logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(handler)
        return logger

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
            command (str): The pip command to execute.

        Raises:
            SystemExit: If the pip command fails.
        """
        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error running pip command: {e}")
            raise SystemExit(f"Pip command failed: {e}")

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

        try:
            self.logger.info(f"Attempting to install {package_name} version {version} from {git_project}...")
            self.run_pip_command(f"pip install git+https://github.com/{git_project}.git@{version}")
            self.logger.info(f"Installation of {package_name} version {version} was successful.")
        except Exception as e:
            self.logger.warning(f"Failed to install version {version}. Error: {e}")
            self.logger.info(f"Falling back to installing from the 'main' branch...")

            try:
                self.run_pip_command(f"pip install git+https://github.com/{git_project}.git@main")
                self.logger.info(f"Fallback installation of {package_name} from the 'main' branch was successful.")
            except Exception as fallback_error:
                self.logger.error(f"Failed to install from the 'main' branch. Error: {fallback_error}")
                raise SystemExit("Installation aborted due to errors.")

        self.logger.info("Process completed. Please restart the environment to verify the installation.")

    def install_packages(self, packages: list):
        """
        Install a list of Python packages using pip.

        Args:
            packages (list): List of package names to install (can include specific versions).

        Raises:
            SystemExit: If a pip command fails.
        """
        for package in packages:
            try:
                self.logger.debug(f"Installing package: {package}")
                self.run_pip_command(f"pip install {package}")
                self.logger.info(f"Successfully installed: {package}")
            except SystemExit:
                self.logger.warning(f"Failed to install {package}. Continuing with the next package.")