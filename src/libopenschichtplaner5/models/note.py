# note.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class Note:
    """5NOTE - Notizen"""
    id: int
    employee_id: int
    date: date
    text1: str
    text2: str
    reserved: Optional[str] = ""
