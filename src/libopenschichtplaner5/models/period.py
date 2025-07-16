from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Period:
    id: int
    group_id: int
    start: str  # We'll keep the date as a string for simplicity
    end: str
    color: int

    @classmethod
    def from_record(cls, record: dict) -> "Period":
        return cls(
            id=int(record.get("ID", 0)),
            group_id=int(record.get("GROUPID")),
            start=normalize_string(record.get("START")),
            end=normalize_string(record.get("END")),
            color=int(record.get("COLOR", 0)),
        )


def load_periods(dbf_path: str | Path) -> List[Period]:
    table = DBFTable(dbf_path)
    return [Period.from_record(record) for record in table.records()]
