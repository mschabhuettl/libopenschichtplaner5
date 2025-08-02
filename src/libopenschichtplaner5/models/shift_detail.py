from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from datetime import date
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class ShiftDetail:
    """5SPSHI - Schichtplan-Details"""
    id: int
    employee_id: int
    date: date
    name: str
    shortname: str
    shift_id: int
    workplace_id: int
    type: int
    colortext: int = 0
    colorbar: int = 0
    colorbk: int = 0
    bold: int = 0
    startend: Optional[str] = ""
    duration: Optional[float] = 0.0
    noextra: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "ShiftDetail":
        # Handle date field properly
        date_val = record.get("DATE")
        if isinstance(date_val, str):
            from datetime import datetime
            try:
                # Try parsing ISO format
                if '-' in date_val:
                    date_val = datetime.strptime(date_val[:10], '%Y-%m-%d').date()
                else:
                    date_val = None
            except (ValueError, TypeError):
                date_val = None
        elif not isinstance(date_val, date):
            date_val = None
            
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            date=date_val,
            name=normalize_string(record.get("NAME", "")),
            shortname=normalize_string(record.get("SHORTNAME", "")),
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            type=int(record.get("TYPE", 0)),
            colortext=int(record.get("COLORTEXT", 0)),
            colorbar=int(record.get("COLORBAR", 0)),
            colorbk=int(record.get("COLORBK", 0)),
            bold=int(record.get("BOLD", 0)),
            startend=normalize_string(record.get("STARTEND", "")),
            duration=float(record.get("DURATION", 0.0)),
            noextra=int(record.get("NOEXTRA", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_shift_details(dbf_path: str | Path) -> List[ShiftDetail]:
    """Load shift details from DBF file."""
    table = DBFTable(dbf_path)
    return [ShiftDetail.from_record(record) for record in table.records()]