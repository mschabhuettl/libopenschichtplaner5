from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from datetime import date
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Holiday:
    """5HOLID - Feiertage"""
    id: int
    date: date
    name: str
    interval: int
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Holiday":
        return cls(
            id=int(record.get("ID", 0)),
            date=record.get("DATE"),
            name=normalize_string(record.get("NAME", "")),
            interval=int(record.get("INTERVAL", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_holidays(dbf_path: str | Path) -> List[Holiday]:
    """Load holidays from DBF file."""
    table = DBFTable(dbf_path)
    return [Holiday.from_record(record) for record in table.records()]