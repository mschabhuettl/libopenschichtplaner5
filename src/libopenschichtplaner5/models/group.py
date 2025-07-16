from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Group:
    id: int
    name: str
    shortname: str
    arbitr: str
    superid: Optional[int]

    @classmethod
    def from_record(cls, record: dict) -> "Group":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME")),
            shortname=normalize_string(record.get("SHORTNAME")),
            arbitr=normalize_string(record.get("ARBITR")),
            superid=int(record.get("SUPERID")) if record.get("SUPERID") else None,
        )


def load_groups(dbf_path: str | Path) -> List[Group]:
    table = DBFTable(dbf_path)
    return [Group.from_record(record) for record in table.records()]
