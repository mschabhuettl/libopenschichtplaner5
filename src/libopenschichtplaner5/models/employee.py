# employee.py
from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
from datetime import datetime, date
from ..db.reader import DBFTable
from ..utils.strings import normalize_string


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
    birthday: Optional[date]
    empstart: Optional[date]
    empend: Optional[date]

    @classmethod
    def from_record(cls, record: dict) -> "Employee":
        # Ensure proper encoding handling and convert to datetime where needed
        name = normalize_string(record.get("NAME", ""))
        firstname = normalize_string(record.get("FIRSTNAME", ""))
        position = str(record.get("POSITION", ""))
        number = normalize_string(record.get("NUMBER", ""))
        salutation = normalize_string(record.get("SALUTATION", ""))
        street = normalize_string(record.get("STREET", ""))
        zip_code = normalize_string(record.get("ZIP", ""))
        town = normalize_string(record.get("TOWN", ""))
        phone = normalize_string(record.get("PHONE", ""))
        email = normalize_string(record.get("EMAIL", ""))
        photo = normalize_string(record.get("PHOTO", ""))
        function = normalize_string(record.get("FUNCTION", ""))

        # Handle dates, assuming some are datetime.date objects already
        birthday = cls.parse_date(record.get("BIRTHDAY"))
        empstart = cls.parse_date(record.get("EMPSTART"))
        empend = cls.parse_date(record.get("EMPEND"))

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
    def parse_date(date_value) -> Optional[date]:
        """Parse date from various formats."""
        if not date_value:
            return None

        # Check if date_value is already a datetime.date object
        if isinstance(date_value, date):
            return date_value

        if isinstance(date_value, datetime):
            return date_value.date()

        if isinstance(date_value, str):
            try:
                return datetime.strptime(date_value, "%Y-%m-%d").date()
            except ValueError:
                try:
                    return datetime.strptime(date_value, "%Y%m%d").date()
                except ValueError:
                    return None

        return None


def load_employees(dbf_path: str | Path) -> List[Employee]:
    """Load employees from DBF file."""
    table = DBFTable(dbf_path)
    return [Employee.from_record(record) for record in table.records()]