from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Period:
    id: int
    group_id: int
    start: str
    end: str
    color: int
    descript: Optional[str] = ""
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Period":
        return cls(
            id=int(record.get("ID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            start=normalize_string(record.get("START", "")),
            end=normalize_string(record.get("END", "")),
            color=int(record.get("COLOR", 0)),
            descript=normalize_string(record.get("DESCRIPT", "")),
            reserved=normalize_string(record.get("RESERVED", "")),
        )


def load_periods(dbf_path: str | Path) -> List[Period]:
    table = DBFTable(dbf_path)
    return [Period.from_record(record) for record in table.records()]
