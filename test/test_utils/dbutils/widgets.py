from typing import Any
from databricks.sdk.runtime import dbutils
from dataclasses import dataclass

class Widgets:
    def __init__(self):
        self._widgets: dict[str, Any] = {}
    
    def text(self, param: str, value: Any = None, label:str|None = None) -> None:
        self._widgets[param] = Text(param, str(value), label)

    def get(self, param: str) -> str:
        return self._widgets.get(param).value
    
    def dropdown(self, param:str, value: Any = None, options: list[Any] = None, label: str | None = None) -> None:
        options = [str(option) for option in options]
        self._widgets[param] = Dropdown(param, str(value), options, label)
        
@dataclass
class Text:
    name: str
    value: str
    label: str | None
    
    # def __repr__(self):
    #     return self.value
@dataclass
class Dropdown:
    name: str
    value: str
    options: list[str]
    label: str

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

