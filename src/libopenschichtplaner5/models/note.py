from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Note:
    """5NOTE - Notizen"""
    id: int
    employee_id: int
    date: date
    text1: str
    text2: str
    reserved: Optional[str] = ""
