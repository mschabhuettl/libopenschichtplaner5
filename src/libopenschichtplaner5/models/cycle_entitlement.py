from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class CycleEntitlement:
    """Maps cycles to specific shifts in the scheduling system."""
    id: int
    cycle_id: int  # References 5CYCLE
    shift_id: int  # References 5SHIFT
    flags: str

    @classmethod
    def from_record(cls, record: dict) -> "CycleEntitlement":
        return cls(
            id=int(record.get("ID", 0)),
            cycle_id=int(record.get("CYCLEEID", 0)),
            shift_id=int(record.get("SHIFTID", 0)),
            flags=record.get("FLAGS", ""),
        )


def load_cycle_entitlements(dbf_path: str | Path) -> List[CycleEntitlement]:
    table = DBFTable(dbf_path)
    return [CycleEntitlement.from_record(record) for record in table.records()]
