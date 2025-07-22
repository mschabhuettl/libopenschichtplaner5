from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class WorkLocation:
    """5WOPL - Arbeitsorte"""
    id: int
    name: str
    shortname: str
    position: int
    colortext: int = 0
    colorbar: int = 0
    colorbk: int = 0
    bold: int = 0
    hide: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "WorkLocation":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME", "")),
            shortname=normalize_string(record.get("SHORTNAME", "")),
            position=int(record.get("POSITION", 0)),
            colortext=int(record.get("COLORTEXT", 0)),
            colorbar=int(record.get("COLORBAR", 0)),
            colorbk=int(record.get("COLORBK", 0)),
            bold=int(record.get("BOLD", 0)),
            hide=int(record.get("HIDE", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_work_locations(dbf_path: str | Path) -> List[WorkLocation]:
    """Load work locations from DBF file."""
    table = DBFTable(dbf_path)
    return [WorkLocation.from_record(record) for record in table.records()]