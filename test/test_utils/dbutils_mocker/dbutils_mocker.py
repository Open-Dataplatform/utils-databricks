from .filesystem import fs
from .widgets import Widgets

class dbutils_mocker:
    """Mimics dbutils in databricks to some extend
    """
    def __init__(self):
        """Initializer
        """
        self.fs: fs = fs()
        self.widgets: Widgets = Widgets()