
from dataclasses import dataclass
from .widgets import Widgets
from typing import Any

@dataclass
class dbutils:
    credentials = 0
    data = 0
    fs = 0
    jobs = 0
    library = 0
    meta = 0
    notebook = 0
    preview = 0
    secret = 0
    widgets = Widgets()