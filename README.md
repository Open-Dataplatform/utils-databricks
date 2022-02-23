# utils-databricks


## Releasing a new version

For releasing new versions, see [this thorough guide]](https://py-pkgs.org/07-releasing-versioning.html).

In simple terms, follow these steps:
1. Merge your bug fixes or new features into the main branch (multiple commits to main since the last version are okay).
2. Bump up the version in pyproject.toml
3. Add and commit pyproject.toml
4. Add a tag with `git tag -a <new_version_number> -m "<message>"`
5. Push the tag with `git push origin --tags`.