# employee_shift.py
from dataclasses import dataclass
from pathlib import Path
from typing import List
from dbfread import DBF  # Import DBF for reading DBF files

@dataclass
class EmployeeShift:
    id: int
    employee_id: int
    shift_id: int
    workplace_id: int
    date: str
    type: int
    reserved: str = ''

    @classmethod
    def from_record(cls, record: dict) -> "EmployeeShift":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            date=record.get("DATE", ""),
            type=int(record.get("TYPE", 0)),
            reserved=record.get("RESERVED", ""),
        )

def load_employee_shifts(dbf_path: str | Path) -> List[EmployeeShift]:
    """
    Loads employee shift records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of EmployeeShift instances
    """
    table = DBF(dbf_path, load=True)
    return [EmployeeShift.from_record(record) for record in table]
