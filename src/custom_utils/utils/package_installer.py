import subprocess
import pkg_resources

def install_package(git_project, package_name, desired_version, uninstall_existing=False):
    """
    Install a specific version of a package from a GitHub repository.
    
    Args:
        git_project (str): The GitHub project path (e.g., 'xazms/utils-databricks').
        package_name (str): The name of the package to install (e.g., 'databricks-custom-utils').
        desired_version (str): The version of the package to install (e.g., 'v0.6.4').
        uninstall_existing (bool): Whether to uninstall the existing package before installation.
    """
    # Ensure the version is normalized with a "v" prefix
    version = f"v{desired_version.lstrip('v')}"
    
    # Uninstall the existing version if requested
    if uninstall_existing:
        print(f"Uninstalling the existing version of {package_name}...")
        try:
            subprocess.check_call(f"pip uninstall {package_name} -y", shell=True)
            print("Uninstallation completed successfully.")
        except subprocess.CalledProcessError as uninstall_error:
            print(f"Error during uninstallation: {uninstall_error}")
            raise SystemExit("Uninstallation failed. Aborting process.")

    try:
        # Attempt to install the specified version from GitHub
        print(f"Attempting to install {package_name} version {version} from {git_project}...")
        subprocess.check_call(f"pip install git+https://github.com/{git_project}.git@{version}", shell=True)
        installed_version = version
    except subprocess.CalledProcessError as e:
        # Handle installation errors
        print(f"\nFailed to install version {version}. Error: {e}")
        print("Falling back to installing from the 'main' branch instead...")

        try:
            # Fallback to the main branch if the version-specific installation fails
            subprocess.check_call(f"pip install git+https://github.com/{git_project}.git@main", shell=True)
            installed_version = "main"
        except subprocess.CalledProcessError as fallback_error:
            print(f"\nInstallation from the 'main' branch also failed. Error: {fallback_error}")
            raise SystemExit("Installation aborted due to repeated errors.")

    # Retrieve and print detailed version information of the installed package
    try:
        package_info = pkg_resources.get_distribution(package_name)
        actual_installed_version = f"v{package_info.version}"  # Ensure "v" prefix is present
        
        # Print success message with installed version
        print("\nInstallation successful.")
        print(f"Latest installed version: {actual_installed_version}")
        print(f"Installed package location: {package_info.location}")
        
        # Check if the installed version matches the desired version
        if installed_version != "main" and actual_installed_version != version:
            print(f"\nWarning: The desired version was {version}, but the installed version is {actual_installed_version}.")
    except pkg_resources.DistributionNotFound:
        print(f"\nInstallation completed, but the package '{package_name}' could not be found. Please check the installation.")
        raise SystemExit("Package not found after installation. Please verify the package name and installation steps.")

    finally:
        print("\nProcess completed. Ensure everything works as expected.")