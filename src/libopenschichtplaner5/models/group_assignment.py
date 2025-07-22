from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class GroupAssignment:
    """5GRASG - Gruppenzuweisungen"""
    id: int
    employee_id: int
    group_id: int
    position: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "GroupAssignment":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            position=int(record.get("POSITION", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_group_assignments(dbf_path: str | Path) -> List[GroupAssignment]:
    """Load group assignments from DBF file."""
    table = DBFTable(dbf_path)
    return [GroupAssignment.from_record(record) for record in table.records()]