from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable

@dataclass
class Absence:
    id: int
    employee_id: int
    date: str
    leave_type_id: int
    type: str

    @classmethod
    def from_record(cls, record: dict) -> "Absence":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID")),
            date=record.get("DATE", ""),
            leave_type_id=int(record.get("LEAVETYPID")),
            type=record.get("TYPE", ""),
        )


def load_absences(dbf_path: str | Path) -> List[Absence]:
    table = DBFTable(dbf_path)
    return [Absence.from_record(record) for record in table.records()]
