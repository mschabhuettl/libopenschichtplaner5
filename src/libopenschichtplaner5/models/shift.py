# shift.py
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Shift:
    id: int
    name: str
    shortname: str
    position: Optional[int]
    colortext: Optional[int]
    colorbar: Optional[int] = 0
    colorbk: Optional[int] = 0
    bold: Optional[int] = 0
    startend: Optional[str] = ""
    duration: Optional[float] = 0.0
    noextra: Optional[int] = 0
    category: Optional[str] = ""
    hide: Optional[int] = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Shift":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME")),
            shortname=normalize_string(record.get("SHORTNAME")),
            position=int(record.get("POSITION")) if record.get("POSITION") else None,
            colortext=int(record.get("COLORTEXT")) if record.get("COLORTEXT") else None,
            colorbar=int(record.get("COLORBAR", 0)),
            colorbk=int(record.get("COLORBK", 0)),
            bold=int(record.get("BOLD", 0)),
            startend=normalize_string(record.get("STARTEND", "")),
            duration=float(record.get("DURATION0", 0.0)),  # Assuming DURATION0 as an example
            noextra=int(record.get("NOEXTRA", 0)),
            category=record.get("CATEGORY", ""),
            hide=int(record.get("HIDE", 0)),
            reserved=record.get("RESERVED", ""),
        )


def load_shifts(dbf_path: str | Path) -> List[Shift]:
    table = DBFTable(dbf_path)
    return [Shift.from_record(record) for record in table.records()]
