from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class ShiftRestriction:
    id: int
    employee_id: int
    weekday: str
    shift_id: int
    restrict: str
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "ShiftRestriction":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            weekday=record.get("WEEKDAY", ""),
            shift_id=int(record.get("SHIFTID", 0)),
            restrict=record.get("RESTRICT", ""),
            reserved=record.get("RESERVED", "")
        )


def load_shift_restrictions(dbf_path: str | Path) -> List[ShiftRestriction]:
    """
    Loads shift restriction records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of ShiftRestriction instances
    """
    table = DBFTable(dbf_path)
    return [ShiftRestriction.from_record(record) for record in table.records()]
