# book.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

@dataclass
class Book:
    """5BOOK - Buchungen (Überstunden etc.)"""
    id: int
    employee_id: int
    date: date
    type: int
    value: float
    note: str = ""
    reserved: Optional[str] = ""
