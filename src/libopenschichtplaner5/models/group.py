from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Group:
    """5GROUP - Gruppen/Abteilungen"""
    id: int
    name: str
    shortname: str
    arbitr: Optional[str] = ""
    superid: Optional[int] = None
    position: int = 0
    dailydem: Optional[str] = ""
    cfglabel: int = 0
    cbklabel: int = 0
    cbksched: int = 0
    bold: int = 0
    hide: int = 0
    reserved: Optional[str] = ""
