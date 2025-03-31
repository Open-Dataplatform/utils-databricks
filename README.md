# utils-databricks


## Releasing a new version

For releasing new versions, see [this thorough guide](https://py-pkgs.org/07-releasing-versioning.html).

In simple terms, follow these steps:
1. Merge your bug fixes or new features into the main branch (multiple commits to main since the last version are okay).
2. Bump up the version in setup.cfg and in src/custom_utils/__init__.py.
3. Add, commit, and push the changes.
4. Add a tag with `git tag -a <new_version_number> -m "<message>"`. (An example could be `git tag -a v1.1.0 -m "Version 1.1.0"`).
5. Push the tag with `git push origin --tags`.

Make sure to go through the next section after updating the version.

## Updating the template
After you released a new version, make sure to update the version tag in the beginning of transformation_template.ipynb.
Also make sure that all the used functions are still working in the new version.