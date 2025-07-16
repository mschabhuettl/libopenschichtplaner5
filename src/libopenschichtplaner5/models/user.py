# user.py
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional  # Added import for Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class User:
    id: int
    position: int
    name: str
    description: str
    admin: int
    digest: Optional[str] = ""
    rights: Optional[str] = ""
    category: Optional[str] = ""
    addempl: Optional[int] = 0
    wduties: Optional[int] = 0
    wnotes: Optional[int] = 0

    @classmethod
    def from_record(cls, record: dict) -> "User":
        return cls(
            id=int(record.get("ID", 0)),
            position=int(record.get("POSITION")),
            name=normalize_string(record.get("NAME")),
            description=normalize_string(record.get("DESCRIP")),
            admin=int(record.get("ADMIN", 0)),
            digest=normalize_string(record.get("DIGEST", "")),
            rights=normalize_string(record.get("RIGHTS", "")),
            category=normalize_string(record.get("CATEGORY", "")),
            addempl=int(record.get("ADDEMPL", 0)),
            wduties=int(record.get("WDUTIES", 0)),
            wnotes=int(record.get("WNOTES", 0)),
        )


def load_users(dbf_path: str | Path) -> List[User]:
    table = DBFTable(dbf_path)
    return [User.from_record(record) for record in table.records()]
