# shift.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal

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
    
    def get_weekday_time(self, weekday: int) -> str:
        """Gibt die Arbeitszeit für einen Wochentag zurück (0=Mo, 6=So)."""
        return getattr(self, f"startend{weekday***REMOVED***", "")
    
    def get_weekday_duration(self, weekday: int) -> float:
        """Gibt die Dauer für einen Wochentag zurück."""
        return getattr(self, f"duration{weekday***REMOVED***", 0.0)
