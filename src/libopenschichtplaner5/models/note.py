# note.py
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List
from ..db.reader import DBFTable


@dataclass
class Note:
    id: int
    employee_id: int
    date: date
    text1: str
    text2: str

    @staticmethod
    def from_record(record: dict) -> 'Note':
        """
        Converts a DBF record (dict) into a Note object.
        """
        # Decode CP1252 to UTF-8 and remove null bytes
        def decode_cp1252(text: str) -> str:
            return text.encode('latin1').decode('cp1252').replace('\x00', '')

        # Ensure the fields exist and decode text fields
        return Note(
            id=record['ID'],  # Make sure 'ID' exists in the DBF file
            employee_id=record.get('EMPLOYEEID', 0),  # Adjust field name if necessary
            date=record['DATE'],  # Ensure 'DATE' is correct
            text1=decode_cp1252(record['TEXT1']),  # Decode text1 field
            text2=decode_cp1252(record['TEXT2'])   # Decode text2 field
        )


def load_notes(dbf_path: Path) -> List[Note]:
    """
    Loads the 5NOTE DBF table and returns a list of `Note` objects.

    :param dbf_path: Path to the 5NOTE DBF file
    :return: List of `Note` instances
    """
    dbf_table = DBFTable(dbf_path)
    notes = [Note.from_record(record) for record in dbf_table.records()]
    return notes
