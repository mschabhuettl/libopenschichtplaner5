from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class UserSetting:
    id: int
    login: str
    category: str
    overt_category: str
    anoaname: str

    @classmethod
    def from_record(cls, record: dict) -> "UserSetting":
        return cls(
            id=int(record.get("ID", 0)),
            login=record.get("LOGIN", ""),
            category=record.get("SPSHCAT", ""),
            overt_category=record.get("OVERTCAT", ""),
            anoaname=normalize_string(record.get("ANOANAME", "")),
        )


def load_user_settings(dbf_path: str | Path) -> List[UserSetting]:
    """
    Loads user setting records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of UserSetting instances
    """
    table = DBFTable(dbf_path)
    return [UserSetting.from_record(record) for record in table.records()]
