from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class LeaveEntitlement:
    """5LEAEN - Urlaubsansprüche"""
    id: int
    employee_id: int
    year: int
    leave_type_id: int
    entitlement: float
    rest: float = 0.0
    indays: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "LeaveEntitlement":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            year=int(record.get("YEAR", 0)),
            leave_type_id=int(record.get("LEAVETYPID", 0)),
            entitlement=float(record.get("ENTITLEMENT", 0.0)),
            rest=float(record.get("REST", 0.0)),
            indays=int(record.get("INDAYS", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_leave_entitlements(dbf_path: str | Path) -> List[LeaveEntitlement]:
    """Load leave entitlements from DBF file."""
    table = DBFTable(dbf_path)
    return [LeaveEntitlement.from_record(record) for record in table.records()]