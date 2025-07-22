# holiday_assignment.py
from dataclasses import dataclass
from typing import List
from datetime import date
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class HolidayAssignment:
    id: int
    employee_id: int
    holiday_id: int
    start_date: date
    end_date: date
    status: str

    @classmethod
    def from_record(cls, record: dict) -> "HolidayAssignment":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEE_ID", 0)),
            holiday_id=int(record.get("HOLIDAY_ID", 0)),
            start_date=record.get("START_DATE", ""),
            end_date=record.get("END_DATE", ""),
            status=normalize_string(record.get("STATUS", "")),
        )


def load_holiday_assignments(dbf_path: str | Path) -> List[HolidayAssignment]:
    """
    Loads the 5HOBAN DBF table and returns a list of `HolidayAssignment` objects.

    :param dbf_path: Path to the 5HOBAN DBF file
    :return: List of `HolidayAssignment` instances
    """
    table = DBFTable(dbf_path)
    return [HolidayAssignment.from_record(record) for record in table.records()]