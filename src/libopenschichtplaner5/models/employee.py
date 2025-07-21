# employee.py
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
from decimal import Decimal


@dataclass
class Employee:
    """5EMPL - Mitarbeiterstammdaten"""
    id: int
    position: int
    number: str
    name: str
    firstname: str
    shortname: Optional[str] = ""
    salutation: Optional[str] = ""
    street: Optional[str] = ""
    zip: Optional[str] = ""
    town: Optional[str] = ""
    phone: Optional[str] = ""
    email: Optional[str] = ""
    photo: Optional[str] = ""
    function: Optional[str] = ""
    arbitr1: Optional[str] = ""
    arbitr2: Optional[str] = ""
    arbitr3: Optional[str] = ""
    sex: int = 0  # 0=männlich, 1=weiblich
    birthday: Optional[date] = None
    empstart: Optional[date] = None
    empend: Optional[date] = None
    calcbase: int = 0
    hrsday: float = 7.7
    hrsweek: float = 38.5
    hrsmonth: float = 154.0
    hrstotal: float = 0.0
    workdays: str = "1 1 1 1 1 0 0"  # Mo-So
    deducthol: int = 0
    cfglabel: int = 0
    bklabel: int = 16777215
    bksched: int = 16777215
    bold: int = 0
    hide: int = 0
    note1: Optional[str] = ""
    note2: Optional[str] = ""
    note3: Optional[str] = ""
    note4: Optional[str] = ""
    reserved: Optional[str] = ""
