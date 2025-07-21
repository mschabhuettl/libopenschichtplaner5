from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class XChar:
    """5XCHAR - Zuschlagsregeln"""
    id: int
    name: str
    position: int
    start: int
    end: int
    validity: int
    validdays: str
    holrule: int
    date: Optional[date] = None
    hide: int = 0
    reserved: Optional[str] = ""
