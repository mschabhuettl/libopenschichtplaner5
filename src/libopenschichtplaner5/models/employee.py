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
    # Color fields for employee display
    cfglabel: Optional[int] = None  # Foreground/text color (RGB integer)
    cbklabel: Optional[int] = None  # Background color (RGB integer) 
    cbksched: Optional[int] = None  # Schedule background color (RGB integer)

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

        # Handle color fields
        cfglabel = record.get("CFGLABEL")
        cbklabel = record.get("CBKLABEL") 
        cbksched = record.get("CBKSCHED")

        # Convert to int if present, otherwise None
        cfglabel = int(cfglabel) if cfglabel is not None else None
        cbklabel = int(cbklabel) if cbklabel is not None else None
        cbksched = int(cbksched) if cbksched is not None else None

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
            empend=empend,
            cfglabel=cfglabel,
            cbklabel=cbklabel,
            cbksched=cbksched
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

    def get_text_color_hex(self) -> Optional[str]:
        """Convert foreground color to hex format (#RRGGBB)."""
        if self.cfglabel is None:
            return None
        return f"#{self.cfglabel:06X}"
    
    def get_background_color_hex(self) -> Optional[str]:
        """Convert background color to hex format (#RRGGBB)."""
        if self.cbklabel is None:
            return None
        return f"#{self.cbklabel:06X}"
    
    def get_schedule_color_hex(self) -> Optional[str]:
        """Convert schedule background color to hex format (#RRGGBB)."""
        if self.cbksched is None:
            return None
        return f"#{self.cbksched:06X}"


def load_employees(dbf_path: str | Path) -> List[Employee]:
    """Load employees from DBF file."""
    table = DBFTable(dbf_path)
    return [Employee.from_record(record) for record in table.records()]