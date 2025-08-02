from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class ShiftRestriction:
    """Defines which employees can/cannot work specific shifts on specific weekdays."""
    id: int
    employee_id: int  # References 5EMPL
    weekday: int      # Day of week (0=Sunday, 1=Monday, etc.)
    shift_id: int     # References 5SHIFT
    restrict: int     # Restriction type (1=preferred, 2=forbidden)
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "ShiftRestriction":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            weekday=int(record.get("WEEKDAY", 0)),
            shift_id=int(record.get("SHIFTID", 0)),
            restrict=int(record.get("RESTRICT", 0)),
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
