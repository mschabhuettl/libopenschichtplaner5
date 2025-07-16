from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class User:
    id: int
    position: int
    name: str
    description: str
    admin: int

    @classmethod
    def from_record(cls, record: dict) -> "User":
        return cls(
            id=int(record.get("ID", 0)),
            position=int(record.get("POSITION")),
            name=normalize_string(record.get("NAME")),
            description=normalize_string(record.get("DESCRIP")),
            admin=int(record.get("ADMIN", 0)),
        )


def load_users(dbf_path: str | Path) -> List[User]:
    table = DBFTable(dbf_path)
    return [User.from_record(record) for record in table.records()]
