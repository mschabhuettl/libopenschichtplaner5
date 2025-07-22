from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Group:
    """5GROUP - Gruppen/Abteilungen"""
    id: int
    name: str
    shortname: str
    arbitr: Optional[str] = ""
    superid: Optional[int] = None
    position: int = 0
    dailydem: Optional[str] = ""
    cfglabel: int = 0
    cbklabel: int = 0
    cbksched: int = 0
    bold: int = 0
    hide: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Group":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME", "")),
            shortname=normalize_string(record.get("SHORTNAME", "")),
            arbitr=normalize_string(record.get("ARBITR", "")),
            superid=record.get("SUPERID"),
            position=int(record.get("POSITION", 0)),
            dailydem=normalize_string(record.get("DAILYDEM", "")),
            cfglabel=int(record.get("CFGLABEL", 0)),
            cbklabel=int(record.get("CBKLABEL", 0)),
            cbksched=int(record.get("CBKSCHED", 0)),
            bold=int(record.get("BOLD", 0)),
            hide=int(record.get("HIDE", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_groups(dbf_path: str | Path) -> List[Group]:
    """Load groups from DBF file."""
    table = DBFTable(dbf_path)
    return [Group.from_record(record) for record in table.records()]