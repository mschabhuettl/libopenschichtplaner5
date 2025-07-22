from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class User:
    """5USER - Benutzer"""
    id: int
    position: int
    name: str
    descrip: str
    admin: int = 0
    digest: Optional[str] = ""
    rights: int = 0
    category: str = ""
    addempl: int = 0
    wduties: int = 0
    wabsences: int = 0
    wovertimes: int = 0
    wnotes: int = 0
    wdeviation: int = 0
    wcycleass: int = 0
    wswaponly: int = 0
    wpast: int = 0
    waccemwnd: int = 0
    waccgrwnd: int = 0
    showabs: int = 0
    shownotes: int = 0
    showstats: int = 0
    raccemwnd: int = 0
    raccgrwnd: int = 0
    backup: int = 0
    hidebarin: int = 0
    hidebarno: int = 0
    accvownd: int = 0
    accadmwnd: int = 0
    minitable: int = 0
    report: str = ""
    hide: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "User":
        return cls(
            id=int(record.get("ID", 0)),
            position=int(record.get("POSITION", 0)),
            name=normalize_string(record.get("NAME", "")),
            descrip=normalize_string(record.get("DESCRIP", "")),
            admin=int(record.get("ADMIN", 0)),
            digest=normalize_string(record.get("DIGEST", "")),
            rights=int(record.get("RIGHTS", 0)),
            category=normalize_string(record.get("CATEGORY", "")),
            addempl=int(record.get("ADDEMPL", 0)),
            wduties=int(record.get("WDUTIES", 0)),
            wabsences=int(record.get("WABSENCES", 0)),
            wovertimes=int(record.get("WOVERTIMES", 0)),
            wnotes=int(record.get("WNOTES", 0)),
            wdeviation=int(record.get("WDEVIATION", 0)),
            wcycleass=int(record.get("WCYCLEASS", 0)),
            wswaponly=int(record.get("WSWAPONLY", 0)),
            wpast=int(record.get("WPAST", 0)),
            waccemwnd=int(record.get("WACCEMWND", 0)),
            waccgrwnd=int(record.get("WACCGRWND", 0)),
            showabs=int(record.get("SHOWABS", 0)),
            shownotes=int(record.get("SHOWNOTES", 0)),
            showstats=int(record.get("SHOWSTATS", 0)),
            raccemwnd=int(record.get("RACCEMWND", 0)),
            raccgrwnd=int(record.get("RACCGRWND", 0)),
            backup=int(record.get("BACKUP", 0)),
            hidebarin=int(record.get("HIDEBARIN", 0)),
            hidebarno=int(record.get("HIDEBARNO", 0)),
            accvownd=int(record.get("ACCVOWND", 0)),
            accadmwnd=int(record.get("ACCADMWND", 0)),
            minitable=int(record.get("MINITABLE", 0)),
            report=normalize_string(record.get("REPORT", "")),
            hide=int(record.get("HIDE", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_users(dbf_path: str | Path) -> List[User]:
    """Load users from DBF file."""
    table = DBFTable(dbf_path)
    return [User.from_record(record) for record in table.records()]