# group.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class Group:
    """5GROUP - Gruppen/Abteilungen"""
    id: int
    name: str
    shortname: str
    arbitr: Optional[str] = ""
    superid: Optional[int] = None
    position: int = 0
    dailydem: Optional[str] = ""
    cfglabel: int = 0
    cbklabel: int = 0
    cbksched: int = 0
    bold: int = 0
    hide: int = 0
    reserved: Optional[str] = ""
