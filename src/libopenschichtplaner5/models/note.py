from dataclasses import dataclass
from datetime import date
from pathlib import Path
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
        # Here, print all the field names of the record to identify the correct field names
        print(record)  # Add this line to inspect the record's structure

        return Note(
            id=record['ID'],  # Make sure 'ID' exists in the DBF file
            employee_id=record.get('EMPLOYEE_ID', 0),  # Adjust field name if necessary
            date=record['DATE'],  # Ensure 'DATE' is correct
            text1=record['TEXT1'],  # Ensure 'TEXT1' is correct
            text2=record['TEXT2'],  # Ensure 'TEXT2' is correct
        )


def load_notes(dbf_path: Path) -> list[Note]:
    """
    Loads the 5NOTE DBF table and returns a list of `Note` objects.

    :param dbf_path: Path to the 5NOTE DBF file
    :return: List of `Note` instances
    """
    dbf_table = DBFTable(dbf_path)
    notes = [Note.from_record(record) for record in dbf_table.records()]
    return notes
