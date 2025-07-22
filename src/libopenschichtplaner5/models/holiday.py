from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Holiday:
    """5HOLID - Feiertage"""
    id: int
    date: date
    name: str
    interval: int
    reserved: Optional[str] = ""
