from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class LeaveType:
    """5LEAVT - Urlaubstypen/Abwesenheitsarten"""
    id: int
    name: str
    shortname: str
    position: int
    colortext: int = 0
    colorbar: int = 16711680
    colorbk: int = 16777215
    bold: int = 0
    chargetype: int = 1
    chargehrs: float = 0.0
    deductact: int = 0
    deductovt: int = 0
    entitled: int = 0
    stdentit: float = 0.0
    carryfwd: int = 0
    countall: int = 1
    ignored: int = 0
    validdays: str = "1 1 1 1 1 1 1"
    category: int = 0
    hide: int = 0
    reserved: Optional[str] = ""
