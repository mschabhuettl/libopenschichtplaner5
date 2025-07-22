from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Cycle:
    """5CYCLE - Zyklen"""
    id: int
    name: str
    position: int
    size: int
    unit: int
    hide: int = 0
    reserved: Optional[str] = ""
