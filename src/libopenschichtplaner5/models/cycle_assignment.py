# models/cycle_assignment.py
from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from datetime import date


@dataclass
class CycleAssignment:
    id: int
    employee_id: int
    cycle_id: int
    start: date
    end: date
    entrance: str
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "CycleAssignment":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            cycle_id=int(record.get("CYCLEID", 0)),
            start=record.get("START", ""),
            end=record.get("END", ""),
            entrance=record.get("ENTRANCE", ""),
            reserved=record.get("RESERVED", ""),
        )


def load_cycle_assignments(dbf_path: str | Path) -> List[CycleAssignment]:
    table = DBFTable(dbf_path)
    return [CycleAssignment.from_record(record) for record in table.records()]
