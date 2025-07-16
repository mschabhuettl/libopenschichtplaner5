from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable  # Import von DBFTable für das Einlesen der DBF-Dateien

@dataclass
class ShiftSchedule:
    id: int
    group_id: int
    weekday: str
    shift_id: int
    workplace_id: int
    min_value: int
    max_value: int
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "ShiftSchedule":
        return cls(
            id=int(record.get("ID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            weekday=record.get("WEEKDAY", ""),
            shift_id=int(record.get("SHIFTID", 0)),
            workplace_id=int(record.get("WORKPLACID", 0)),
            min_value=int(record.get("MIN", 0)),
            max_value=int(record.get("MAX", 0)),
            reserved=record.get("RESERVED", ""),
        )


def load_shift_schedules(dbf_path: str | Path) -> List[ShiftSchedule]:
    """
    Loads shift schedule records from a DBF file.
    :param dbf_path: Path to the DBF file
    :return: List of ShiftSchedule instances
    """
    table = DBFTable(dbf_path)
    return [ShiftSchedule.from_record(record) for record in table.records()]
