# libopenschichtplaner5/src/libopenschichtplaner5/db/reader.py
from dbfread import DBF
from pathlib import Path
from typing import Iterator, Dict, Any, List
from datetime import date, datetime

# Encoding-Priorität für deutsche Systeme
ENCODINGS = [
    "cp1252",  # Windows-1252 (Standard)
    "cp850",  # DOS-Codepage
    "iso-8859-1",  # Latin-1
    "cp437",  # Original DOS
    "utf-8",  # Modern
]

# Zeichen-Mappings für fehlerhafte Umlaute
CHAR_REPLACEMENTS = {
    "ь": "ü",
    "д": "ä",
    "ц": "ö",
    "Ь": "Ü",
    "Д": "Ä",
    "Ц": "Ö",
    "Я": "ß",
    "ќ": "ü",
    "Ђ": "Ä",
***REMOVED***


class DBFTable:
    """
    Wrapper für DBF-Dateien mit korrektem Encoding-Handling.
    Vereinheitlicht die beiden Reader-Implementierungen.
    """

    def __init__(self, path: str | Path, encodings: list = None):
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"DBF file not found: {self.path***REMOVED***")

        self._table = None
        self._encoding = None
        self._field_info = {***REMOVED***
        self.encodings = encodings or ENCODINGS

        self._try_encodings()
        self._extract_field_info()

    def _try_encodings(self):
        """Versucht verschiedene Encodings."""
        last_error = None

        for encoding in self.encodings:
            try:
                self._table = DBF(
                    self.path,
                    load=True,
                    ignore_missing_memofile=True,
                    encoding=encoding,
                    char_decode_errors='replace'
                )
                self._encoding = encoding
                # ENTFERNT: print(f"Successfully loaded {self.path.name***REMOVED*** with encoding: {encoding***REMOVED***")
                return
            except Exception as e:
                last_error = e
                continue

        if self._table is None:
            raise ValueError(f"Could not load DBF file with any encoding: {last_error***REMOVED***")

    def _extract_field_info(self):
        """Extrahiert Feld-Informationen."""
        if not self._table:
            return

        for field in self._table.fields:
            self._field_info[field.name] = {
                'type': field.type,
                'length': field.length,
                'decimal_count': field.decimal_count
            ***REMOVED***

    def _clean_string(self, value: Any) -> str:
        """Bereinigt String-Werte."""
        if value is None:
            return ""

        # Konvertiere zu String
        if isinstance(value, bytes):
            try:
                value = value.decode(self._encoding or 'cp1252', errors='replace')
            except:
                value = str(value, errors='replace')
        else:
            value = str(value)

        # Entferne Null-Bytes
        value = value.replace("\x00", "")

        # Ersetze Problemzeichen
        for old, new in CHAR_REPLACEMENTS.items():
            value = value.replace(old, new)

        return value.strip()

    def _convert_value(self, field_name: str, value: Any) -> Any:
        """Konvertiert Feldwerte basierend auf Typ."""
        if value is None:
            return None

        field_info = self._field_info.get(field_name, {***REMOVED***)
        field_type = field_info.get('type', 'C')

        # Character-Felder
        if field_type == 'C':
            return self._clean_string(value)

        # Numerische Felder
        elif field_type in ['N', 'F']:
            if value == '' or value is None:
                return None
            try:
                if field_type == 'F' or field_info.get('decimal_count', 0) > 0:
                    return float(value)
                return int(value)
            except (ValueError, TypeError):
                return None

        # Datums-Felder
        elif field_type == 'D':
            if isinstance(value, date):
                return value
            if isinstance(value, datetime):
                return value.date()
            return None

        # Logische Felder
        elif field_type == 'L':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.upper() in ('T', 'Y', '1', 'TRUE')
            return bool(value)

        # Memo-Felder
        elif field_type == 'M':
            return self._clean_string(value)

        # Standard
        return value

    def records(self) -> Iterator[Dict[str, Any]]:
        """Iteriert über alle Records."""
        if not self._table:
            return

        for record in self._table:
            cleaned_record = {***REMOVED***
            for field_name, value in record.items():
                cleaned_record[field_name] = self._convert_value(field_name, value)
            yield cleaned_record

    def get_field_info(self) -> Dict[str, Dict[str, Any]]:
        """Gibt Feld-Informationen zurück."""
        return self._field_info.copy()


# Für Kompatibilität
SchichtplanerDBFReader = DBFTable