from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class Overtime:
    """5OVER - Überstunden"""
    id: int
    employee_id: int
    date: date
    hours: float
    reserved: Optional[str] = ""
