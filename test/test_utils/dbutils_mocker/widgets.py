from typing import Any
from dataclasses import dataclass
from typing import Union

class Widgets:
    """Mimics widget objects in databricks.
    """
    def __init__(self):
        """Initializer
        """
        self._widgets: dict[str, Union[Text, Dropdown]] = {}
    
    def text(self, param: str, value: Any|str = None, label:str|None = None) -> None:
        """Sets text widget

        Args:
            param (str): widget name
            value (Any | str, optional): widget value. Defaults to None.
            label (str | None, optional): widget label. Defaults to None.
        """
        self._widgets[param] = Text(param, str(value), label)

    def get(self, param: str) -> str:
        """Gets the widget parameter

        Args:
            param (str): widget name

        Returns:
            str: widget value
        """
        return self._widgets.get(param).value
    
    def dropdown(self, param:str, value: Any = None, options: list[Any] = None, label: str | None = None) -> None:
        """Sets dropdown widget

        Args:
            param (str): widget name
            value (Any, optional): widget value. Defaults to None.
            options (list[Any], optional): list of possible values. Defaults to None.
            label (str | None, optional): widget name. Defaults to None.
        """
        options = [str(option) for option in options]
        self._widgets[param] = Dropdown(param, str(value), options, label)
        
    def remove(self, param: str):
        del self._widgets[param]
        
    def getAll(self) -> dict[str, str]:
        """Returns all widget names and values

        Returns:
            dict[str, str]: dictionary with widget names and values.
        """
        return {name: widget.value for name, widget in self._widgets.items()}
        
@dataclass
class Text:
    """Dataclass to contain text widget
    """
    name: str
    value: str
    label: str | None
    
@dataclass
class Dropdown:
    """Dataclass to contain dropdown widget
    """
    name: str
    value: str
    options: list[str]
    label: str
    