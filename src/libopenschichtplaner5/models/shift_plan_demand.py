from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from datetime import date
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class ShiftPlanDemand:
    """Special shift plan demands - specific date-based shift requirements."""
    id: int
    group_id: int
    date: date
    shift_id: int
    workplace_id: int
    min_staff: int
    max_staff: int
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "ShiftPlanDemand":
        # Handle date field
        date_val = record.get("DATE")
        if isinstance(date_val, str):
            from datetime import datetime
            try:
                if '-' in date_val:
                    date_val = datetime.strptime(date_val[:10], '%Y-%m-%d').date()
                else:
                    date_val = None
            except (ValueError, TypeError):
                date_val = None
        elif not isinstance(date_val, date):
            pass  # Already a date object
            
        return cls(
            id=int(record.get("ID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            date=date_val,
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            min_staff=int(record.get("MIN", 0)),
            max_staff=int(record.get("MAX", 0)),
            reserved=normalize_string(record.get("RESERVED", "")),
        )


def load_shift_plan_demands(dbf_path: str | Path) -> List[ShiftPlanDemand]:
    """
    Loads shift plan demand records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of ShiftPlanDemand instances
    """
    table = DBFTable(dbf_path)
    return [ShiftPlanDemand.from_record(record) for record in table.records()]
