# employee_shift.py
from dataclasses import dataclass
from pathlib import Path
from typing import List
from datetime import date
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class EmployeeShift:
    id: int
    employee_id: int
    shift_id: int
    workplace_id: int
    date: date
    type: int
    reserved: str = ''

    @classmethod
    def from_record(cls, record: dict) -> "EmployeeShift":
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
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            date=date_val,
            type=int(record.get("TYPE", 0)),
            reserved=normalize_string(record.get("RESERVED", "")),
        )


def load_employee_shifts(dbf_path: str | Path) -> List[EmployeeShift]:
    """
    Loads employee shift records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of EmployeeShift instances
    """
    table = DBFTable(dbf_path)
    return [EmployeeShift.from_record(record) for record in table.records()]