from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class LeaveType:
    id: int
    name: str
    shortname: str
    position: int
    colortext: int

    @classmethod
    def from_record(cls, record: dict) -> "LeaveType":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME")),
            shortname=normalize_string(record.get("SHORTNAME")),
            position=int(record.get("POSITION", 0)),
            colortext=int(record.get("COLORTEXT", 0)),
        )


def load_leavetypes(dbf_path: str | Path) -> List[LeaveType]:
    table = DBFTable(dbf_path)
    return [LeaveType.from_record(record) for record in table.records()]
