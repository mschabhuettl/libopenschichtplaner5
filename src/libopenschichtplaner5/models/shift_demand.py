from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class ShiftDemand:
    """Shift demand/requirement definition - defines min/max staff per shift."""
    id: int
    group_id: int
    weekday: int  # 0=Monday, 6=Sunday
    shift_id: int
    workplace_id: int
    min_staff: int
    max_staff: int
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "ShiftDemand":
        return cls(
            id=int(record.get("ID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            weekday=int(record.get("WEEKDAY", 0)),
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            min_staff=int(record.get("MIN", 0)),
            max_staff=int(record.get("MAX", 0)),
            reserved=normalize_string(record.get("RESERVED", "")),
        )


def load_shift_demands(dbf_path: str | Path) -> List[ShiftDemand]:
    table = DBFTable(dbf_path)
    return [ShiftDemand.from_record(record) for record in table.records()]


def load_day_demands(dbf_path: str | Path) -> List[ShiftDemand]:
    """Loads day demand records from 5DADEM DBF file using same ShiftDemand model."""
    table = DBFTable(dbf_path)
    return [ShiftDemand.from_record(record) for record in table.records()]
