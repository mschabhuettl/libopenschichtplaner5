from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class Cycle:
    """5CYCLE - Zyklen"""
    id: int
    name: str
    position: int
    size: int
    unit: int
    hide: int = 0
    reserved: Optional[str] = ""
