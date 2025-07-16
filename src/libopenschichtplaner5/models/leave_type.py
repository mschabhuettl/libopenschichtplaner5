# leave_type.py
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class LeaveType:
    id: int
    name: str
    shortname: str
    position: int
    colortext: int
    colorbar: Optional[int] = 0
    colorbk: Optional[int] = 0
    bold: Optional[int] = 0
    chargetyp: Optional[int] = 0
    chargehrs: Optional[float] = 0.0
    deductact: Optional[int] = 0
    deductovt: Optional[int] = 0
    entitled: Optional[int] = 0
    stdentit: Optional[float] = 0.0
    carryfwd: Optional[int] = 0
    countall: Optional[int] = 0
    ignored: Optional[int] = 0
    validdays: Optional[int] = 0
    category: Optional[str] = ""
    hide: Optional[int] = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "LeaveType":
        return cls(
            id=cls.parse_int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME")),
            shortname=normalize_string(record.get("SHORTNAME")),
            position=cls.parse_int(record.get("POSITION", 0)),
            colortext=cls.parse_int(record.get("COLORTEXT", 0)),
            colorbar=cls.parse_int(record.get("COLORBAR", 0)),
            colorbk=cls.parse_int(record.get("COLORBK", 0)),
            bold=cls.parse_int(record.get("BOLD", 0)),
            chargetyp=cls.parse_int(record.get("CHARGETYP", 0)),
            chargehrs=cls.parse_float(record.get("CHARGEHRS", 0.0)),
            deductact=cls.parse_int(record.get("DEDUCTACT", 0)),
            deductovt=cls.parse_int(record.get("DEDUCTOVT", 0)),
            entitled=cls.parse_int(record.get("ENTITLED", 0)),
            stdentit=cls.parse_float(record.get("STDENTIT", 0.0)),
            carryfwd=cls.parse_int(record.get("CARRYFWD", 0)),
            countall=cls.parse_int(record.get("COUNTALL", 0)),
            ignored=cls.parse_int(record.get("IGNORED", 0)),
            validdays=cls.parse_int(record.get("VALIDDAYS", 0)),
            category=record.get("CATEGORY", ""),
            hide=cls.parse_int(record.get("HIDE", 0)),
            reserved=record.get("RESERVED", ""),
        )

    @staticmethod
    def parse_int(value):
        """Safely parse an integer, handling spaces and invalid values."""
        try:
            return int(value.strip()) if isinstance(value, str) else int(value)
        except ValueError:
            return 0

    @staticmethod
    def parse_float(value):
        """Safely parse a float, handling spaces and invalid values."""
        try:
            return float(value.strip()) if isinstance(value, str) else float(value)
        except ValueError:
            return 0.0


def load_leavetypes(dbf_path: str | Path) -> List[LeaveType]:
    table = DBFTable(dbf_path)
    return [LeaveType.from_record(record) for record in table.records()]
