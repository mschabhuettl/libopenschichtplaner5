# holiday.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class Holiday:
    """5HOLID - Feiertage"""
    id: int
    date: date
    name: str
    interval: int
    reserved: Optional[str] = ""
