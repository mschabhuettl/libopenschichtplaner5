from dbfread import DBF
from pathlib import Path
from typing import Iterator, Dict


class DBFTable:
    """
    A wrapper around a DBF file using dbfread with proper encoding handling.
    """

    def __init__(self, path: str | Path, encoding: str = "cp1252"):
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"DBF file not found: {self.path}")

        self._table = DBF(
            self.path,
            load=True,
            ignore_missing_memofile=True,
            encoding=encoding  # correct encoding for German Umlauts
        )

    def records(self) -> Iterator[Dict[str, object]]:
        for record in self._table:
            yield dict(record)
