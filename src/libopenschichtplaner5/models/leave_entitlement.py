# leave_entitlement.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

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
