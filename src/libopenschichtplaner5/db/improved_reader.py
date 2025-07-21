# libopenschichtplaner5/src/libopenschichtplaner5/db/improved_reader.py
"""
Verbesserter DBF-Reader mit robustem Encoding-Handling für Schichtplaner5-Daten.
Basierend auf der Analyse der echten DBF-Dateien.
"""

from dbfread import DBF
from pathlib import Path
from typing import Iterator, Dict, List, Any, Optional
import struct
import chardet
from datetime import datetime, date


class SchichtplanerDBFReader:
    """
    Spezialisierter DBF-Reader für Schichtplaner5-Dateien.
    Behandelt die spezifischen Encoding-Probleme und Datenformate.
    """
    
    # Priorisierte Encodings für deutsche Systeme
    ENCODINGS = [
        "cp1252",      # Windows-1252 (Standard für deutsche Windows-Systeme)
        "cp850",       # DOS-Codepage (oft in älteren DBF-Dateien)
        "iso-8859-1",  # Latin-1
        "cp437",       # Original DOS
        "utf-8",       # Moderner Standard
    ]
    
    # Spezielle Zeichen-Mappings für fehlerhafte Umlaute
    CHAR_REPLACEMENTS = {
        # Häufige Fehlkodierungen von Umlauten
        "\x84": "ä",
        "\x94": "ö",
        "\x81": "ü",
        "\x8e": "Ä",
        "\x99": "Ö",
        "\x9a": "Ü",
        "\xe1": "ß",
        # Kyrillische Zeichen die eigentlich Umlaute sein sollten
        "ь": "ü",
        "д": "ä",
        "ц": "ö",
        "Ь": "Ü",
        "Д": "Ä",
        "Ц": "Ö",
        "Я": "ß",
        # Weitere problematische Zeichen
        "ќ": "ü",
        "Ђ": "Ä",
    ***REMOVED***
    
    def __init__(self, path: str | Path, encoding: str = None, detect_encoding: bool = True):
        """
        Initialisiert den DBF-Reader.
        
        Args:
            path: Pfad zur DBF-Datei
            encoding: Explizites Encoding (optional)
            detect_encoding: Automatische Encoding-Erkennung aktivieren
        """
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"DBF file not found: {self.path***REMOVED***")
        
        self._table = None
        self._encoding = encoding
        self._detect_encoding = detect_encoding
        self._field_info = {***REMOVED***
        
        # Versuche die Datei zu laden
        self._load_table()
    
    def _detect_file_encoding(self) -> Optional[str]:
        """
        Versucht das Encoding der Datei zu erkennen.
        """
        if not self._detect_encoding:
            return None
        
        try:
            # Lese einen Teil der Datei für die Erkennung
            with open(self.path, 'rb') as f:
                # Skip DBF header (32 bytes) + field descriptors
                f.seek(32)
                # Lese bis zum Header-Ende-Marker (0x0D)
                while f.read(1) != b'\x0d':
                    f.seek(31, 1)  # Skip rest of field descriptor
                
                # Lese erste Datenzeilen
                sample = f.read(1024)
            
            # Verwende chardet für die Erkennung
            result = chardet.detect(sample)
            if result['confidence'] > 0.7:
                return result['encoding']
        except Exception:
            pass
        
        return None
    
    def _load_table(self):
        """
        Lädt die DBF-Tabelle mit dem besten Encoding.
        """
        # Erkenne Encoding wenn gewünscht
        detected_encoding = self._detect_file_encoding()
        
        # Erstelle Encoding-Liste
        encodings_to_try = []
        if self._encoding:
            encodings_to_try.append(self._encoding)
        if detected_encoding:
            encodings_to_try.append(detected_encoding)
        encodings_to_try.extend(self.ENCODINGS)
        
        # Versuche verschiedene Encodings
        last_error = None
        for encoding in encodings_to_try:
            try:
                self._table = DBF(
                    self.path,
                    load=True,
                    ignore_missing_memofile=True,
                    encoding=encoding,
                    char_decode_errors='ignore'  # Ignoriere Decode-Fehler
                )
                self._encoding = encoding
                print(f"Successfully loaded {self.path.name***REMOVED*** with encoding: {encoding***REMOVED***")
                
                # Extrahiere Feld-Informationen
                self._extract_field_info()
                return
                
            except Exception as e:
                last_error = e
                continue
        
        # Wenn nichts funktioniert hat
        if last_error:
            raise ValueError(f"Could not load DBF file with any encoding: {last_error***REMOVED***")
    
    def _extract_field_info(self):
        """
        Extrahiert Informationen über die Felder.
        """
        if not self._table:
            return
        
        for field in self._table.fields:
            self._field_info[field.name] = {
                'type': field.type,
                'length': field.length,
                'decimal_count': field.decimal_count
            ***REMOVED***
    
    def _clean_string(self, value: Any) -> str:
        """
        Bereinigt String-Werte von Encoding-Problemen.
        """
        if value is None:
            return ""
        
        # Konvertiere zu String
        if isinstance(value, bytes):
            try:
                value = value.decode(self._encoding or 'utf-8', errors='ignore')
            except:
                value = str(value, errors='ignore')
        else:
            value = str(value)
        
        # Entferne Null-Bytes
        value = value.replace("\x00", "")
        
        # Ersetze bekannte Problemzeichen
        for old, new in self.CHAR_REPLACEMENTS.items():
            value = value.replace(old, new)
        
        # Trimme Whitespace
        return value.strip()
    
    def _parse_date(self, value: Any) -> Optional[date]:
        """
        Parst Datumswerte aus verschiedenen Formaten.
        """
        if not value:
            return None
        
        # Bereits ein date-Objekt?
        if isinstance(value, date):
            return value
        
        # String im Format YYYYMMDD?
        if isinstance(value, str):
            value = value.strip()
            if len(value) == 8 and value.isdigit():
                try:
                    year = int(value[0:4])
                    month = int(value[4:6])
                    day = int(value[6:8])
                    
                    # Validiere Datum
                    if year < 1900 or year > 2100:
                        return None
                    if month < 1 or month > 12:
                        return None
                    if day < 1 or day > 31:
                        return None
                    
                    return date(year, month, day)
                except ValueError:
                    return None
        
        # datetime-Objekt?
        if isinstance(value, datetime):
            return value.date()
        
        return None
    
    def _convert_value(self, field_name: str, value: Any) -> Any:
        """
        Konvertiert einen Feldwert basierend auf dem Feldtyp.
        """
        if value is None:
            return None
        
        field_info = self._field_info.get(field_name, {***REMOVED***)
        field_type = field_info.get('type', 'C')
        
        # Character-Felder
        if field_type == 'C':
            return self._clean_string(value)
        
        # Numerische Felder
        elif field_type == 'N':
            if value == '' or value is None:
                return None
            try:
                # Prüfe ob Dezimalstellen vorhanden
                if field_info.get('decimal_count', 0) > 0:
                    return float(value)
                return int(value)
            except (ValueError, TypeError):
                return None
        
        # Float-Felder
        elif field_type == 'F':
            if value == '' or value is None:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        # Datums-Felder
        elif field_type == 'D':
            return self._parse_date(value)
        
        # Logische Felder
        elif field_type == 'L':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.upper() in ('T', 'Y', '1')
            return bool(value)
        
        # Standard: als String behandeln
        return self._clean_string(value)
    
    def records(self) -> Iterator[Dict[str, Any]]:
        """
        Iteriert über alle Records mit bereinigten Werten.
        """
        if not self._table:
            return
        
        for record in self._table:
            # Konvertiere alle Feldwerte
            cleaned_record = {***REMOVED***
            for field_name, value in record.items():
                cleaned_record[field_name] = self._convert_value(field_name, value)
            
            yield cleaned_record
    
    def get_field_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Gibt Informationen über alle Felder zurück.
        """
        return self._field_info.copy()
    
    def get_encoding(self) -> str:
        """
        Gibt das verwendete Encoding zurück.
        """
        return self._encoding
    
    def validate_structure(self, expected_fields: List[str]) -> Dict[str, Any]:
        """
        Validiert die Tabellenstruktur gegen erwartete Felder.
        
        Args:
            expected_fields: Liste der erwarteten Feldnamen
            
        Returns:
            Dict mit Validierungsergebnissen
        """
        actual_fields = set(self._field_info.keys())
        expected_set = set(expected_fields)
        
        return {
            'valid': actual_fields == expected_set,
            'missing_fields': list(expected_set - actual_fields),
            'extra_fields': list(actual_fields - expected_set),
            'matching_fields': list(actual_fields & expected_set)
        ***REMOVED***


# Spezialisierte Reader für bestimmte Tabellen
class EmployeeDBFReader(SchichtplanerDBFReader):
    """Spezialisierter Reader für die 5EMPL-Tabelle."""
    
    EXPECTED_FIELDS = [
        'ID', 'POSITION', 'NUMBER', 'NAME', 'FIRSTNAME', 'SHORTNAME',
        'SALUTATION', 'STREET', 'ZIP', 'TOWN', 'PHONE', 'EMAIL',
        'PHOTO', 'FUNCTION', 'ARBITR1', 'ARBITR2', 'ARBITR3',
        'SEX', 'BIRTHDAY', 'EMPSTART', 'EMPEND', 'CALCBASE',
        'HRSDAY', 'HRSWEEK', 'HRSMONTH', 'HRSTOTAL', 'WORKDAYS',
        'DEDUCTHOL', 'CFGLABEL', 'BKLABEL', 'BKSCHED', 'BOLD',
        'HIDE', 'NOTE1', 'NOTE2', 'NOTE3', 'NOTE4', 'RESERVED'
    ]
    
    def validate(self) -> Dict[str, Any]:
        """Validiert die Struktur der Employee-Tabelle."""
        return self.validate_structure(self.EXPECTED_FIELDS)
    
    def get_active_employees(self, reference_date: date = None) -> List[Dict[str, Any]]:
        """
        Gibt nur aktive Mitarbeiter zurück.
        
        Args:
            reference_date: Referenzdatum (default: heute)
        """
        if reference_date is None:
            reference_date = date.today()
        
        active_employees = []
        
        for record in self.records():
            # Prüfe Eintrittsdatum
            empstart = record.get('EMPSTART')
            if empstart and empstart > reference_date:
                continue
            
            # Prüfe Austrittsdatum
            empend = record.get('EMPEND')
            if empend and empend < reference_date:
                continue
            
            # Prüfe Hide-Flag
            if record.get('HIDE', 0) == 1:
                continue
            
            active_employees.append(record)
        
        return active_employees


# Beispiel-Verwendung
def example_usage():
    """Zeigt die Verwendung des verbesserten Readers."""
    
    # Normaler Reader
    reader = SchichtplanerDBFReader("path/to/5EMPL.DBF")
    
    # Zeige Encoding
    print(f"Using encoding: {reader.get_encoding()***REMOVED***")
    
    # Zeige Feld-Informationen
    field_info = reader.get_field_info()
    for field, info in field_info.items():
        print(f"{field***REMOVED***: {info['type']***REMOVED*** ({info['length']***REMOVED***)")
    
    # Lese Records
    for record in reader.records():
        print(f"Employee: {record['NAME']***REMOVED***, {record['FIRSTNAME']***REMOVED***")
    
    # Spezialisierter Reader
    emp_reader = EmployeeDBFReader("path/to/5EMPL.DBF")
    
    # Validiere Struktur
    validation = emp_reader.validate()
    if not validation['valid']:
        print(f"Missing fields: {validation['missing_fields']***REMOVED***")
        print(f"Extra fields: {validation['extra_fields']***REMOVED***")
    
    # Hole nur aktive Mitarbeiter
    active = emp_reader.get_active_employees()
    print(f"Active employees: {len(active)***REMOVED***")
