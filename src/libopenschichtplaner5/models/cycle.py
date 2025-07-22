from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Cycle:
    """5CYCLE - Zyklen"""
    id: int
    name: str
    position: int
    size: int
    unit: int
    hide: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Cycle":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME", "")),
            position=int(record.get("POSITION", 0)),
            size=int(record.get("SIZE", 0)),
            unit=int(record.get("UNIT", 0)),
            hide=int(record.get("HIDE", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_cycles(dbf_path: str | Path) -> List[Cycle]:
    """Load cycles from DBF file."""
    table = DBFTable(dbf_path)
    return [Cycle.from_record(record) for record in table.records()]