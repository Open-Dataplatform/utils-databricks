from typing import Any, Callable
from databricks.sdk.runtime import dbutils
class Widgets:
    mandatory_widgets: list = ["SourceStorageAccount",
                               "DestinationStorageAccount",
                               "SourceContainer",
                               "SourceDatasetidentifier",
                               "SourceFileName",
                               "KeyColumns"]
    def __init__(self):
        self.dropdowns: dict[dict[str, Any]] = {}
        self.texts: dict[dict[str, Any]] = {}
        self.widgets: dict[dict[str, Any]] = {} 
        for widget in Widgets.mandatory_widgets:
            self.text(widget, " ", widget)

def get(self, key:str):
    val = self.widgets.get(key, None)
    return val.get("value")

def getAll(self):
    return self.widgets._widgets
        
def set_getAll(obj: dbutils) -> None:
    assert isinstance(obj, dbutils), f"only dbutils objects are allowed, received obejct of {type(obj)}"
    dbutils.widgets.getAll = lambda : getAll(dbutils)
