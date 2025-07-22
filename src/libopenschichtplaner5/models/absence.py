from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Absence:
    """5ABSEN - Abwesenheiten"""
    id: int
    employee_id: int
    date: date
    leave_type_id: int
    type: int
    interval: Optional[int] = None
    start: Optional[int] = None
    end: Optional[int] = None
    reserved: Optional[str] = ""
