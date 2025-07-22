# book.py
from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
from datetime import date
from decimal import Decimal
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Book:
    id: int
    employee_id: int
    date: Optional[date]
    type: int
    value: Decimal
    note: str = ''
    reserved: str = ''

    @classmethod
    def from_record(cls, record: dict) -> "Book":
        # Parse date properly
        date_value = record.get("DATE")
        if isinstance(date_value, date):
            parsed_date = date_value
        else:
            parsed_date = None  # Or use a date parsing function

        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            date=parsed_date,
            type=int(record.get("TYPE", 0)),
            value=Decimal(str(record.get("VALUE", 0))),
            note=normalize_string(record.get("NOTE", "")),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_books(dbf_path: str | Path) -> List[Book]:
    table = DBFTable(dbf_path)
    return [Book.from_record(record) for record in table.records()]