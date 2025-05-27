
from dataclasses import dataclass
from typing import Any

from .widgets import Widgets
from .filesystem import fs


@dataclass
class dbutils:
    credentials = 0
    data = 0
    fs = fs()
    jobs = 0
    library = 0
    meta = 0
    notebook = 0
    preview = 0
    secret = 0
    widgets = Widgets()