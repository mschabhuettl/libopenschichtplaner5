from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class WorkLocation:
    """5WOPL - Arbeitsorte"""
    id: int
    name: str
    shortname: str
    position: int
    colortext: int = 0
    colorbar: int = 0
    colorbk: int = 0
    bold: int = 0
    hide: int = 0
    reserved: Optional[str] = ""
