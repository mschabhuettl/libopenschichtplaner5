# employee_shift.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class EmployeeShift:
    """5MASHI - Mitarbeiterschichten (NICHT Master Shifts!)"""
    id: int
    employee_id: int
    date: date
    shift_id: int
    workplace_id: int
    type: int
    reserved: Optional[str] = ""
