from dataclasses import dataclass
from pathlib import Path
from typing import List
from ..db.reader import DBFTable

@dataclass
class CycleEntitlement:
    id: int
    cycle_id: int
    shift_id: int
    workplace_id: int
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "CycleEntitlement":
        return cls(
            id=int(record.get("CYCLEEID", 0)),
            cycle_id=int(record.get("INDEX", 0)),
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            reserved=record.get("RESERVED", ""),
        )


def load_cycle_entitlements(dbf_path: str | Path) -> List[CycleEntitlement]:
    table = DBFTable(dbf_path)
    return [CycleEntitlement.from_record(record) for record in table.records()]
