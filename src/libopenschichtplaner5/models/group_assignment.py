from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class GroupAssignment:
    """5GRASG - Gruppenzuweisungen"""
    id: int
    employee_id: int
    group_id: int
    reserved: Optional[str] = ""
