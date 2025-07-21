# models/cycle_assignment.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

class CycleAssignment:
    """5CYASS - Zykluszuweisungen"""
    id: int
    employee_id: int
    cycle_id: int
    start: date
    end: date
    entrance: int
    reserved: Optional[str] = ""
