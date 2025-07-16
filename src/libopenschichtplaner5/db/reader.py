from dbfread import DBF
from pathlib import Path
from typing import Iterator, Dict

# Define common encodings, now including a broader set of encodings
ENCODINGS = [
    "cp1252", "latin1", "utf-8", "cp437", "windows-1252", "iso-8859-1",
    "cp850", "mac_roman", "cp863", "cp874", "iso-8859-15"
]

class DBFTable:
    """
    A wrapper around a DBF file using dbfread with proper encoding handling.
    Tries multiple encodings until it finds one that works.
    """

    def __init__(self, path: str | Path, encodings: list = ENCODINGS):
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"DBF file not found: {self.path***REMOVED***")

        self._table = None
        self._try_encodings(encodings)

    def _try_encodings(self, encodings: list):
        """
        Try different encodings to load the DBF file until one works.
        """
        for encoding in encodings:
            try:
                self._table = DBF(
                    self.path,
                    load=True,
                    ignore_missing_memofile=True,
                    encoding=encoding
                )
                print(f"Successfully loaded with encoding: {encoding***REMOVED***")
                break
            except UnicodeDecodeError:
                print(f"Failed to load with encoding: {encoding***REMOVED***")
                continue
        if self._table is None:
            raise UnicodeDecodeError("None of the encodings worked.")

    def records(self) -> Iterator[Dict[str, object]]:
        for record in self._table:
            yield dict(record)
