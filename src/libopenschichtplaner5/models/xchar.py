# libopenschichtplaner5/src/libopenschichtplaner5/models/xchar.py
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class XChar:
    """5XCHAR - Zuschlagsregeln (Sonderstunden-Charakteristika)"""
    id: int
    name: str
    position: int
    start: int
    end: int
    validity: int
    validdays: str
    holrule: int
    date: Optional[date] = None
    hide: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "XChar":
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME", "")),
            position=int(record.get("POSITION", 0)),
            start=int(record.get("START", 0)),
            end=int(record.get("END", 0)),
            validity=int(record.get("VALIDITY", 0)),
            validdays=normalize_string(record.get("VALIDDAYS", "")),
            holrule=int(record.get("HOLRULE", 0)),
            date=record.get("DATE"),
            hide=int(record.get("HIDE", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )

    def is_sunday_surcharge(self) -> bool:
        """Prüft ob es sich um Sonntagszuschlag handelt."""
        return "sonntag" in self.name.lower()

    def is_saturday_surcharge(self) -> bool:
        """Prüft ob es sich um Samstagszuschlag handelt."""
        return "samstag" in self.name.lower()

    def is_night_surcharge(self) -> bool:
        """Prüft ob es sich um Nachtzuschlag handelt."""
        return "nacht" in self.name.lower()

    def is_holiday_surcharge(self) -> bool:
        """Prüft ob es sich um Feiertagszuschlag handelt."""
        return "feiertag" in self.name.lower()


def load_xchar(dbf_path: str | Path) -> List[XChar]:
    """Lädt Zuschlagsregeln aus 5XCHAR."""
    table = DBFTable(dbf_path)
    xchars = []

    for record in table.records():
        xchar = XChar.from_record(record)
        xchars.append(xchar)

    return xchars


# Alias für Kompatibilität
ShiftRule = XChar
load_shift_rules = load_xchar