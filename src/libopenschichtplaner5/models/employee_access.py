from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class EmployeeAccess:
    """Individual employee permissions and access rights management."""
    id: int
    employee_id: int  # References 5EMPL
    access_code: str  # Permission identifier (REPORTSEE, EDITSHIFT, etc.)
    value: str        # Access level/configuration

    @classmethod
    def from_record(cls, record: dict) -> "EmployeeAccess":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            access_code=record.get("ACCESSCODE", ""),
            value=record.get("VALUE", ""),
        )


def load_employee_access(dbf_path: str | Path) -> list[EmployeeAccess]:
    """
    Loads the 5EMACC DBF table and returns a list of `EmployeeAccess` objects.

    :param dbf_path: Path to the 5EMACC DBF file
    :return: List of `EmployeeAccess` instances
    """
    table = DBFTable(dbf_path)
    return [EmployeeAccess.from_record(record) for record in table.records()]
