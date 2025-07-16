from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Shift:
    id: int
    name: str
    shortname: str
    position: Optional[int]
    colortext: Optional[int]

    @classmethod
    def from_record(cls, record: dict) -> "Shift":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME")),
            shortname=normalize_string(record.get("SHORTNAME")),
            position=int(record.get("POSITION")) if record.get("POSITION") else None,
            colortext=int(record.get("COLORTEXT")) if record.get("COLORTEXT") else None,
        )


def load_shifts(dbf_path: str | Path) -> List[Shift]:
    table = DBFTable(dbf_path)
    return [Shift.from_record(record) for record in table.records()]
