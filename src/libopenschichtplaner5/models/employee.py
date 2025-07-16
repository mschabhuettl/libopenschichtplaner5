from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Employee:
    id: int
    position: Optional[int]
    number: str
    name: str
    firstname: str

    @classmethod
    def from_record(cls, record: dict) -> "Employee":
        return cls(
            id=int(record.get("ID", 0)),
            position=int(record.get("POSITION")) if record.get("POSITION") else None,
            number=normalize_string(record.get("NUMBER")),
            name=normalize_string(record.get("NAME")),
            firstname=normalize_string(record.get("FIRSTNAME")),
        )


def load_employees(dbf_path: str | Path) -> List[Employee]:
    table = DBFTable(dbf_path)
    return [Employee.from_record(rec) for rec in table.records()]
