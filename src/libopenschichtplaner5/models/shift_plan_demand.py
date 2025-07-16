from dataclasses import dataclass
from pathlib import Path
from typing import List
from ..db.reader import DBFTable


@dataclass
class ShiftPlanDemand:
    id: int
    employee_id: int
    shift_id: int
    demand: str  # Describes the demand for the shift (e.g., "Urgent", "Normal")
    date: str  # Date of the shift
    notes: str  # Additional notes related to the demand

    @classmethod
    def from_record(cls, record: dict) -> "ShiftPlanDemand":
        return cls(
            id=int(record.get("ID", 0)),
            employee_id=int(record.get("EMPLOYEEID", 0)),
            shift_id=int(record.get("SHIFTID", 0)),
            demand=record.get("DEMAND", ""),
            date=record.get("DATE", ""),
            notes=record.get("NOTES", "")
        )


def load_shift_plan_demands(dbf_path: str | Path) -> List[ShiftPlanDemand]:
    """
    Loads shift plan demand records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of ShiftPlanDemand instances
    """
    table = DBFTable(dbf_path)
    return [ShiftPlanDemand.from_record(record) for record in table.records()]
