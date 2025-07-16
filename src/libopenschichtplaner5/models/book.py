# book.py
from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string
from decimal import Decimal

@dataclass
class Book:
    id: int
    employee_id: int
    date: str
    type: int
    value: Decimal
    note: str = ''
    reserved: str = ''

    @classmethod
    def from_record(cls, record: dict) -> "Book":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID")),
            date=normalize_string(record.get("DATE")),
            type=int(record.get("TYPE", 0)),
            value=Decimal(record.get("VALUE", 0)),
            note=normalize_string(record.get("NOTE", "")),
            reserved=normalize_string(record.get("RESERVED", ""))
        )

def load_books(dbf_path: str | Path) -> List[Book]:
    table = DBFTable(dbf_path)
    return [Book.from_record(record) for record in table.records()]
