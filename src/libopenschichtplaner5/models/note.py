from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from datetime import date
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Note:
    """5NOTE - Notizen"""
    id: int
    employee_id: int
    date: date
    text1: str
    text2: str
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Note":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            date=record.get("DATE"),
            text1=normalize_string(record.get("TEXT1", "")),
            text2=normalize_string(record.get("TEXT2", "")),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_notes(dbf_path: str | Path) -> List[Note]:
    """Load notes from DBF file."""
    table = DBFTable(dbf_path)
    return [Note.from_record(record) for record in table.records()]