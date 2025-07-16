from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string

@dataclass
class ShiftRule:
    id: int
    name: str
    position: int
    start: str
    end: str
    validity: str
    valid_days: str
    holiday_rule: str
    date: str
    hide: bool
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "ShiftRule":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME")),
            position=int(record.get("POSITION", 0)),
            start=record.get("START", ""),
            end=record.get("END", ""),
            validity=record.get("VALIDITY", ""),
            valid_days=record.get("VALIDDAYS", ""),
            holiday_rule=record.get("HOLRULE", ""),
            date=record.get("DATE", ""),
            hide=record.get("HIDE", 0) == 1,
            reserved=record.get("RESERVED", "")
        )

def load_shift_rules(dbf_path: Path) -> list[ShiftRule]:
    """
    Loads the shift rule records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of ShiftRule instances
    """
    dbf_table = DBFTable(dbf_path)
    return [ShiftRule.from_record(record) for record in dbf_table.records()]
