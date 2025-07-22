from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from datetime import date
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Absence:
    """5ABSEN - Abwesenheiten"""
    id: int
    employee_id: int
    date: date
    leave_type_id: int
    type: int
    interval: Optional[int] = None
    start: Optional[int] = None
    end: Optional[int] = None
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Absence":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            date=record.get("DATE"),
            leave_type_id=int(record.get("LEAVETYPID", 0)),
            type=int(record.get("TYPE", 0)),
            interval=record.get("INTERVAL"),
            start=record.get("START"),
            end=record.get("END"),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_absences(dbf_path: str | Path) -> List[Absence]:
    """Load absences from DBF file."""
    table = DBFTable(dbf_path)
    return [Absence.from_record(record) for record in table.records()]