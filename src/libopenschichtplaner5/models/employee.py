# employee.py
from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable
from datetime import datetime, date


@dataclass
class Employee:
    id: int
    name: str
    firstname: str
    position: str
    number: str
    salutation: str
    street: str
    zip_code: str
    town: str
    phone: str
    email: str
    photo: str
    function: str
    birthday: str
    empstart: str
    empend: str

    @classmethod
    def from_record(cls, record: dict) -> "Employee":
        # Ensure proper encoding handling and convert to datetime where needed
        name = record.get("NAME", "").replace("\x00", "")
        firstname = record.get("FIRSTNAME", "").replace("\x00", "")
        position = str(record.get("POSITION", ""))
        number = record.get("NUMBER", "").replace("\x00", "")
        salutation = record.get("SALUTATION", "").replace("\x00", "")
        street = record.get("STREET", "").replace("\x00", "")
        zip_code = record.get("ZIP", "").replace("\x00", "")
        town = record.get("TOWN", "").replace("\x00", "")
        phone = record.get("PHONE", "").replace("\x00", "")
        email = record.get("EMAIL", "").replace("\x00", "")
        photo = record.get("PHOTO", "").replace("\x00", "")
        function = record.get("FUNCTION", "").replace("\x00", "")

        # Handle dates, assuming some are datetime.date objects already
        birthday = cls.parse_date(record.get("BIRTHDAY", ""))
        empstart = cls.parse_date(record.get("EMPSTART", ""))
        empend = cls.parse_date(record.get("EMPEND", ""))

        return cls(
            id=int(record.get("ID", 0)),
            name=name,
            firstname=firstname,
            position=position,
            number=number,
            salutation=salutation,
            street=street,
            zip_code=zip_code,
            town=town,
            phone=phone,
            email=email,
            photo=photo,
            function=function,
            birthday=birthday,
            empstart=empstart,
            empend=empend
        )

    @staticmethod
    def parse_date(date_str):
        # Check if date_str is already a datetime.date object, return as is
        if isinstance(date_str, date):
            return date_str
        if date_str:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()  # Assuming the format is YYYY-MM-DD
            except ValueError:
                return None
        return None


def load_employees(dbf_path: str | Path) -> List[Employee]:
    table = DBFTable(dbf_path)
    return [Employee.from_record(record) for record in table.records()]
