from dataclasses import dataclass
from typing import List
from pathlib import Path
from ..db.reader import DBFTable


@dataclass
class WorkLocation:
    id: int
    name: str
    shortname: str
    position: int
    colortext: int
    colorbar: int
    bold: int
    hidden: int
    reserved: str

    @classmethod
    def from_record(cls, record: dict) -> "WorkLocation":
        return cls(
            id=int(record.get("ID", 0)),
            name=record.get("NAME", "").replace("\x00", ""),  # Remove null byte
            shortname=record.get("SHORTNAME", "").replace("\x00", ""),
            position=int(record.get("POSITION", 0)),
            colortext=int(record.get("COLORTEXT", 0)),
            colorbar=int(record.get("COLORBAR", 0)),
            bold=int(record.get("BOLD", 0)),
            hidden=int(record.get("HIDDEN", 0)),
            reserved=record.get("RESERVED", "").replace("\x00", "")
        )


def load_work_locations(dbf_path: str | Path) -> List[WorkLocation]:
    """
    Loads work location records from a DBF file.

    :param dbf_path: Path to the DBF file
    :return: List of WorkLocation instances
    """
    table = DBFTable(dbf_path)
    return [WorkLocation.from_record(record) for record in table.records()]
