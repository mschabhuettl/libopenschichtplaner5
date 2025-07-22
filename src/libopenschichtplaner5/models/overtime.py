# libopenschichtplaner5/src/libopenschichtplaner5/models/overtime.py
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Overtime:
    """5OVER - Überstunden"""
    id: int
    employee_id: int
    date: date
    hours: float
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Overtime":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            date=record.get("DATE"),
            hours=float(record.get("HOURS", 0.0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_overtime(dbf_path: str | Path) -> List[Overtime]:
    """Lädt Überstunden aus 5OVER."""
    table = DBFTable(dbf_path)
    overtimes = []

    for record in table.records():
        ot = Overtime.from_record(record)
        if ot.date:  # Nur gültige Datensätze
            overtimes.append(ot)

    return overtimes