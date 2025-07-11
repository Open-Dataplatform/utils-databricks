from typing import Any
from databricks.sdk.runtime import dbutils

def getAll(self) -> dict[str, Any]:
    """Mimics getAll method from databricks.

    Returns:
        dict[str, Any]:dict of all widgets in dbutils object.
    """
    return self.widgets._widgets
        
def set_getAll(obj: dbutils) -> None:
    """Sets the getAll object in dbutils. Can be used for other methods.

    Args:
        obj (dbutils): dbutils object to receive the method.
    """
    obj.widgets.getAll = lambda *args, **kwargs: getAll(dbutils, *args, **kwargs)

