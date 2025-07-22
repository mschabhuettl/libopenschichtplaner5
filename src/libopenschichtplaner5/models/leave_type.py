from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class LeaveType:
    """5LEAVT - Urlaubstypen/Abwesenheitsarten"""
    id: int
    name: str
    shortname: str
    position: int
    colortext: int = 0
    colorbar: int = 16711680
    colorbk: int = 16777215
    bold: int = 0
    chargetype: int = 1
    chargehrs: float = 0.0
    deductact: int = 0
    deductovt: int = 0
    entitled: int = 0
    stdentit: float = 0.0
    carryfwd: int = 0
    countall: int = 1
    ignored: int = 0
    validdays: str = "1 1 1 1 1 1 1"
    category: int = 0
    hide: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "LeaveType":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME", "")),
            shortname=normalize_string(record.get("SHORTNAME", "")),
            position=int(record.get("POSITION", 0)),
            colortext=int(record.get("COLORTEXT", 0)),
            colorbar=int(record.get("COLORBAR", 16711680)),
            colorbk=int(record.get("COLORBK", 16777215)),
            bold=int(record.get("BOLD", 0)),
            chargetype=int(record.get("CHARGETYPE", 1)),
            chargehrs=float(record.get("CHARGEHRS", 0.0)),
            deductact=int(record.get("DEDUCTACT", 0)),
            deductovt=int(record.get("DEDUCTOVT", 0)),
            entitled=int(record.get("ENTITLED", 0)),
            stdentit=float(record.get("STDENTIT", 0.0)),
            carryfwd=int(record.get("CARRYFWD", 0)),
            countall=int(record.get("COUNTALL", 1)),
            ignored=int(record.get("IGNORED", 0)),
            validdays=normalize_string(record.get("VALIDDAYS", "1 1 1 1 1 1 1")),
            category=int(record.get("CATEGORY", 0)),
            hide=int(record.get("HIDE", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )


def load_leave_types(dbf_path: str | Path) -> List[LeaveType]:
    """Load leave types from DBF file."""
    table = DBFTable(dbf_path)
    return [LeaveType.from_record(record) for record in table.records()]