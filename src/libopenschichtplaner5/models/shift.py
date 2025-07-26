from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


@dataclass
class Shift:
    """5SHIFT - Schichtdefinitionen"""
    id: int
    name: str
    shortname: str
    position: int
    colortext: int = 16777215
    colorbar: int = 16777215
    colorbk: int = 16744448
    bold: int = 0
    # Zeiten pro Wochentag
    startend0: str = ""  # Montag
    startend1: str = ""  # Dienstag
    startend2: str = ""  # Mittwoch
    startend3: str = ""  # Donnerstag
    startend4: str = ""  # Freitag
    startend5: str = ""  # Samstag
    startend6: str = ""  # Sonntag
    startend7: str = ""  # Reserve/Feiertag?
    # Dauer pro Wochentag
    duration0: float = 0.0
    duration1: float = 0.0
    duration2: float = 0.0
    duration3: float = 0.0
    duration4: float = 0.0
    duration5: float = 0.0
    duration6: float = 0.0
    duration7: float = 0.0
    noextra: int = 0
    category: int = 0
    hide: int = 0
    reserved: Optional[str] = ""

    @classmethod
    def from_record(cls, record: dict) -> "Shift":
        """Erstellt eine Shift aus einem DBF-Record."""
        return cls(
            id=int(record.get("ID", 0)),
            name=normalize_string(record.get("NAME", "")),
            shortname=normalize_string(record.get("SHORTNAME", "")),
            position=int(record.get("POSITION", 0)),
            colortext=int(record.get("COLORTEXT", 16777215)),
            colorbar=int(record.get("COLORBAR", 16777215)),
            colorbk=int(record.get("COLORBK", 16744448)),
            bold=int(record.get("BOLD", 0)),
            startend0=normalize_string(record.get("STARTEND0", "")),
            startend1=normalize_string(record.get("STARTEND1", "")),
            startend2=normalize_string(record.get("STARTEND2", "")),
            startend3=normalize_string(record.get("STARTEND3", "")),
            startend4=normalize_string(record.get("STARTEND4", "")),
            startend5=normalize_string(record.get("STARTEND5", "")),
            startend6=normalize_string(record.get("STARTEND6", "")),
            startend7=normalize_string(record.get("STARTEND7", "")),
            duration0=float(record.get("DURATION0", 0.0)),
            duration1=float(record.get("DURATION1", 0.0)),
            duration2=float(record.get("DURATION2", 0.0)),
            duration3=float(record.get("DURATION3", 0.0)),
            duration4=float(record.get("DURATION4", 0.0)),
            duration5=float(record.get("DURATION5", 0.0)),
            duration6=float(record.get("DURATION6", 0.0)),
            duration7=float(record.get("DURATION7", 0.0)),
            noextra=int(record.get("NOEXTRA", 0)),
            category=int(record.get("CATEGORY", 0)),
            hide=int(record.get("HIDE", 0)),
            reserved=normalize_string(record.get("RESERVED", ""))
        )

    def get_weekday_time(self, weekday: int) -> str:
        """Gibt die Arbeitszeit für einen Wochentag zurück (0=Mo, 6=So)."""
        return getattr(self, f"startend{weekday}", "")

    def get_weekday_duration(self, weekday: int) -> float:
        """Gibt die Dauer für einen Wochentag zurück."""
        return getattr(self, f"duration{weekday}", 0.0)


def load_shifts(dbf_path: str | Path) -> List[Shift]:
    """Lädt alle Schichten aus einer DBF-Datei."""
    table = DBFTable(dbf_path)
    return [Shift.from_record(record) for record in table.records()]