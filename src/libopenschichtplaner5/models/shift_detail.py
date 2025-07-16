from dataclasses import dataclass
from typing import Optional
from datetime import date
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string

@dataclass
class ShiftDetail:
    id: int
    employee_id: int
    date: date
    name: str
    shortname: str
    shift_id: int
    workplace_id: int
    type: int
    colortext: Optional[int]
    colorbar: Optional[int]
    colorbk: Optional[int]
    bold: Optional[int]
    start_end: Optional[str]
    duration: Optional[int]
    noextra: Optional[int]
    reserved: Optional[int]

    @classmethod
    def from_record(cls, record: dict) -> "ShiftDetail":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            date=record.get("DATE", ""),
            name=normalize_string(record.get("NAME", "")),
            shortname=normalize_string(record.get("SHORTNAME", "")),
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            type=int(record.get("TYPE", 0)),
            colortext=int(record.get("COLORTEXT", 0)) if record.get("COLORTEXT") else None,
            colorbar=int(record.get("COLORBAR", 0)) if record.get("COLORBAR") else None,
            colorbk=int(record.get("COLORBK", 0)) if record.get("COLORBK") else None,
            bold=int(record.get("BOLD", 0)) if record.get("BOLD") else None,
            start_end=record.get("STARTEND", ""),
            duration=int(record.get("DURATION", 0)) if record.get("DURATION") else None,
            noextra=int(record.get("NOEXTRA", 0)) if record.get("NOEXTRA") else None,
            reserved=int(record.get("RESERVED", 0)) if record.get("RESERVED") else None
        )


def load_shift_details(dbf_path: str | Path) -> list[ShiftDetail]:
    """
    Loads shift detail records from a DBF file.
    """
    table = DBFTable(dbf_path)
    return [ShiftDetail.from_record(record) for record in table.records()]
