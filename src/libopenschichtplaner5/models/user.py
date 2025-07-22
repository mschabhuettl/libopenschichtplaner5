from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class User:
    """5USER - Benutzer"""
    id: int
    position: int
    name: str
    descrip: str
    admin: int = 0
    digest: Optional[str] = ""
    rights: int = 0
    category: str = ""
    addempl: int = 0
    wduties: int = 0
    wabsences: int = 0
    wovertimes: int = 0
    wnotes: int = 0
    wdeviation: int = 0
    wcycleass: int = 0
    wswaponly: int = 0
    wpast: int = 0
    waccemwnd: int = 0
    waccgrwnd: int = 0
    showabs: int = 0
    shownotes: int = 0
    showstats: int = 0
    raccemwnd: int = 0
    raccgrwnd: int = 0
    backup: int = 0
    hidebarin: int = 0
    hidebarno: int = 0
    accvownd: int = 0
    accadmwnd: int = 0
    minitable: int = 0
    report: str = ""
    hide: int = 0
    reserved: Optional[str] = ""
