from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from ..db.reader import DBFTable

@dataclass
class Cycle:
    id: int
    name: str
    position: Optional[int]
    size: Optional[int]
    unit: Optional[int]
    hide: Optional[int]
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "Cycle":
        return cls(
            id=int(record.get("ID", 0)),
            name=record.get("NAME", "").replace("\x00", ""),
            position=int(record.get("POSITION", 0)) if record.get("POSITION") else None,
            size=int(record.get("SIZE", 0)) if record.get("SIZE") else None,
            unit=int(record.get("UNIT", 0)) if record.get("UNIT") else None,
            hide=int(record.get("HIDE", 0)) if record.get("HIDE") else None,
            reserved=record.get("RESERVED", "").replace("\x00", ""),
        )


def load_cycles(dbf_path: str | Path):
    table = DBFTable(dbf_path)
    return [Cycle.from_record(record) for record in table.records()]
