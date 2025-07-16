from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class ShiftDemand:
    id: int
    group_id: int
    start: str
    daily_demand: str
    description: str
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "ShiftDemand":
        return cls(
            id=int(record.get("ID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            start=record.get("START", ""),
            daily_demand=record.get("DAILYDEM", ""),
            description=normalize_string(record.get("DESCRIPT", "")),
            reserved=record.get("RESERVED", ""),
        )


def load_shift_demands(dbf_path: str | Path) -> List[ShiftDemand]:
    table = DBFTable(dbf_path)
    return [ShiftDemand.from_record(record) for record in table.records()]
