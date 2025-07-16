# holiday.py
from dataclasses import dataclass
from pathlib import Path
from typing import List
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Holiday:
    id: int
    date: str
    name: str
    interval: int
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "Holiday":
        return cls(
            id=int(record.get("ID", 0)),
            date=normalize_string(record.get("DATE")),
            name=normalize_string(record.get("NAME")),
            interval=int(record.get("INTERVAL", 0)),
            reserved=normalize_string(record.get("RESERVED", "")),
        )


def load_holidays(dbf_path: str | Path) -> List[Holiday]:
    table = DBFTable(dbf_path)
    return [Holiday.from_record(record) for record in table.records()]
