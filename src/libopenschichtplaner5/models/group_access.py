from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class GroupAccess:
    """Group-level permissions and access rights management."""
    id: int
    group_id: int     # References 5GROUP
    access_code: str  # Permission identifier (REPORTSEE, EDITSHIFT, etc.)
    value: str        # Access level/configuration

    @classmethod
    def from_record(cls, record: dict) -> "GroupAccess":
        return cls(
            id=int(record.get("ID", 0)),
            group_id=int(record.get("GROUPID", 0)),
            access_code=record.get("ACCESSCODE", ""),
            value=record.get("VALUE", ""),
        )

def load_group_access(dbf_path: str | Path) -> list[GroupAccess]:
    table = DBFTable(dbf_path)
    return [GroupAccess.from_record(record) for record in table.records()]
