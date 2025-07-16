from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable

@dataclass
class Build:
    id: int
    build: str
    change: str
    description: str

    @classmethod
    def from_record(cls, record: dict) -> "Build":
        return cls(
            id=int(record.get("ID", 0)),
            build=record.get("BUILD", ""),
            change=record.get("CHANGE", ""),
            description=record.get("DESCRIPTION", ""),
        )


def load_builds(dbf_path: str | Path) -> List[Build]:
    table = DBFTable(dbf_path)
    return [Build.from_record(record) for record in table.records()]
