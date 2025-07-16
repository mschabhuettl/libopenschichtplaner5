from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class LeaveEntitlement:
    id: int
    employee_id: int
    year: int
    leave_type_id: int
    entitlement: float

    @classmethod
    def from_record(cls, record: dict) -> "LeaveEntitlement":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID")),
            year=int(record.get("YEAR")),
            leave_type_id=int(record.get("LEAVETYPID")),
            entitlement=float(record.get("ENTITLEMNT", 0)),
        )


def load_leave_entitlements(dbf_path: str | Path) -> List[LeaveEntitlement]:
    table = DBFTable(dbf_path)
    return [LeaveEntitlement.from_record(record) for record in table.records()]
