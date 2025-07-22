from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class ShiftDetail:
    """5SPSHI - Schichtplan-Details"""
    id: int
    employee_id: int
    date: date
    name: str
    shortname: str
    shift_id: int
    workplace_id: int
    type: int
    colortext: int = 0
    colorbar: int = 0
    colorbk: int = 0
    bold: int = 0
    startend: Optional[str] = ""
    duration: Optional[float] = 0.0
    noextra: int = 0
    reserved: Optional[str] = ""
