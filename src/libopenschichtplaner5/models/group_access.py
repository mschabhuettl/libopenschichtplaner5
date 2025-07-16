from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string

@dataclass
class GroupAccess:
    id: int
    user_id: int
    group_id: int
    access_level: Optional[int]

    @classmethod
    def from_record(cls, record: dict) -> "GroupAccess":
        return cls(
            id=int(record.get("ID", 0)),
            user_id=int(record.get("USERID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            access_level=int(record.get("ACCESSLEVEL", 0)) if record.get("ACCESSLEVEL") else None,
        )

def load_group_access(dbf_path: str | Path) -> list[GroupAccess]:
    table = DBFTable(dbf_path)
    return [GroupAccess.from_record(record) for record in table.records()]
