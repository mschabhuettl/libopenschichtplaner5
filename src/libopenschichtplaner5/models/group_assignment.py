from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from ..db.reader import DBFTable

@dataclass
class GroupAssignment:
    id: int
    employee_id: int
    group_id: int
    reserved: Optional[str]

    @classmethod
    def from_record(cls, record: dict) -> "GroupAssignment":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            reserved=record.get("RESERVED", ""),
        )

def load_group_assignments(dbf_path: str | Path):
    table = DBFTable(dbf_path)
    return [GroupAssignment.from_record(record) for record in table.records()]
