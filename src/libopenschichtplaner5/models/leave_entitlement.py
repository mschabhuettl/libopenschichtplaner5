from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class LeaveEntitlement:
    """5LEAEN - Urlaubsansprüche"""
    id: int
    employee_id: int
    year: int
    leave_type_id: int
    entitlement: float
    rest: float = 0.0
    indays: int = 0
    reserved: Optional[str] = ""
