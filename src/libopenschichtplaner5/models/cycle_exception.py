from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class CycleException:
    id: int
    employee_id: int
    cycle_ass_id: int
    date: str  # Keeping date as a string for simplicity
    type: Optional[str]
    reserved: Optional[str]

    @classmethod
    def from_record(cls, record: dict) -> "CycleException":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            cycle_ass_id=int(record.get("CYCLEASSID", 0)),
            date=normalize_string(record.get("DATE", "")),
            type=normalize_string(record.get("TYPE", "")),
            reserved=normalize_string(record.get("RESERVED", "")),
        )


def load_cycle_exceptions(dbf_path: str | Path) -> List[CycleException]:
    table = DBFTable(dbf_path)
    return [CycleException.from_record(record) for record in table.records()]
