# leave_entitlement.py
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable


@dataclass
class LeaveEntitlement:
    id: int
    employee_id: int
    year: int
    leave_type_id: int
    entitlement: float
    rest: Optional[float] = 0.0
    in_days: Optional[float] = 0.0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "LeaveEntitlement":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID")),
            year=int(record.get("YEAR")),
            leave_type_id=int(record.get("LEAVETYPID")),
            entitlement=float(record.get("ENTITLEMNT", 0)),
            rest=float(record.get("REST", 0)),
            in_days=float(record.get("INDAYS", 0)),
            reserved=record.get("RESERVED", ""),
        )


def load_leave_entitlements(dbf_path: str | Path) -> List[LeaveEntitlement]:
    table = DBFTable(dbf_path)
    return [LeaveEntitlement.from_record(record) for record in table.records()]
