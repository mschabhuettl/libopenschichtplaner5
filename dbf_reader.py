"""
Pure Python DBF/dBASE reader for Schichtplaner5 databases.
Handles UTF-16 LE string encoding used by the Delphi/FoxPro application.
"""
import struct
import os
from datetime import date
from typing import List, Dict, Any, Optional


def _is_utf16_le(raw: bytes) -> bool:
    """
    Heuristic: detect if raw bytes are UTF-16 LE encoded text.
    In UTF-16 LE ASCII text, bytes at odd positions (1, 3, 5, ...) are 0x00.
    Plain ASCII/binary data fields have non-zero bytes at odd positions.
    """
    if len(raw) < 4:
        # Very short field — check if first byte is 0 (empty UTF-16 LE)
        return len(raw) >= 2 and raw[1] == 0x00
    # Sample up to 8 bytes for detection
    sample_len = min(8, len(raw))
    sample = raw[:sample_len]
    odd_bytes = sample[1::2]
    null_count = sum(1 for b in odd_bytes if b == 0x00)
    # More than half of odd-position bytes are 0x00 → likely UTF-16 LE
    return null_count > len(odd_bytes) // 2


def _decode_string(raw: bytes) -> str:
    """
    Decode a string field from Schichtplaner5 .DBF files.
    
    SP5 uses two different encodings for Character fields:
    - Text fields (NAME, SHORTNAME, etc.): UTF-16 LE, padded with 0x20
    - Data fields (WORKDAYS, STARTEND*, etc.): plain ASCII, padded with 0x20
    
    We detect UTF-16 LE by checking if odd-indexed bytes are 0x00.
    """
    if not raw:
        return ''

    if _is_utf16_le(raw):
        # UTF-16 LE encoded text: find null terminator (0x00 0x00 at even offset)
        end = len(raw)
        for i in range(0, len(raw) - 1, 2):
            if raw[i] == 0x00 and raw[i + 1] == 0x00:
                end = i
                break
        chunk = raw[:end]
        if not chunk:
            return ''
        try:
            return chunk.decode('utf-16-le').strip()
        except Exception:
            pass

    # Plain ASCII / binary data field (WORKDAYS, STARTEND*, etc.)
    # Strip trailing spaces/nulls and decode as latin-1 to preserve all byte values
    stripped = raw.rstrip(b'\x00\x20')
    try:
        return stripped.decode('latin-1').strip()
    except Exception:
        return raw.split(b'\x00')[0].decode('latin-1', errors='replace').strip()


def _parse_date(raw: str) -> Optional[str]:
    """Parse dBASE date string YYYYMMDD to ISO format."""
    s = raw.strip()
    if len(s) == 8 and s.isdigit():
        try:
            year, month, day = int(s[:4]), int(s[4:6]), int(s[6:8])
            if 1 <= month <= 12 and 1 <= day <= 31 and year > 0:
                return f"{year:04d}-{month:02d}-{day:02d}"
        except Exception:
            pass
    return None


def read_dbf(filepath: str, encoding_hint: str = 'utf-16-le') -> List[Dict[str, Any]]:
    """
    Read a .DBF file and return a list of records as dicts.
    String fields are decoded as UTF-16 LE (as used by Schichtplaner5).
    """
    if not os.path.exists(filepath):
        return []

    with open(filepath, 'rb') as f:
        # Read header (32 bytes)
        header = f.read(32)
        if len(header) < 32:
            return []

        num_records = struct.unpack_from('<I', header, 4)[0]
        header_size = struct.unpack_from('<H', header, 8)[0]
        record_size = struct.unpack_from('<H', header, 10)[0]

        # Read field descriptors (32 bytes each, terminated by 0x0D)
        fields = []
        f.seek(32)
        while True:
            field_data = f.read(32)
            if not field_data or len(field_data) < 32 or field_data[0] == 0x0D:
                break
            name = field_data[0:11].split(b'\x00')[0].decode('ascii', errors='replace').strip()
            ftype = chr(field_data[11])
            flen = field_data[16]
            fdec = field_data[17]
            fields.append({'name': name, 'type': ftype, 'len': flen, 'dec': fdec})

        # Read records
        f.seek(header_size)
        records = []

        for _ in range(num_records):
            raw = f.read(record_size)
            if not raw or len(raw) < record_size:
                break

            # Skip deleted records (first byte = 0x2A = '*')
            if raw[0] == 0x2A:
                continue

            record = {}
            offset = 1  # skip deletion flag

            for field in fields:
                chunk = raw[offset:offset + field['len']]
                ftype = field['type']
                fname = field['name']

                if ftype == 'C':
                    # Character field - UTF-16 LE in Schichtplaner5
                    val = _decode_string(chunk)
                elif ftype == 'D':
                    # Date field YYYYMMDD
                    val = _parse_date(chunk.decode('ascii', errors='replace'))
                elif ftype in ('N', 'F'):
                    # Numeric/Float
                    s = chunk.decode('ascii', errors='replace').strip()
                    if s == '' or s == '.':
                        val = 0
                    else:
                        try:
                            val = float(s) if '.' in s or field['dec'] > 0 else int(s)
                        except ValueError:
                            val = 0
                elif ftype == 'L':
                    # Logical
                    s = chunk.decode('ascii', errors='replace').strip()
                    val = s in ('T', 't', 'Y', 'y', '1')
                elif ftype == 'M':
                    # Memo (pointer only in .DBF, actual data in .DBT)
                    val = None
                else:
                    val = chunk.decode('ascii', errors='replace').strip()

                record[fname] = val
                offset += field['len']

            records.append(record)

    return records


def get_table_fields(filepath: str) -> List[Dict[str, Any]]:
    """Return field definitions for a .DBF file."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'rb') as f:
        f.read(32)
        fields = []
        while True:
            field_data = f.read(32)
            if not field_data or len(field_data) < 32 or field_data[0] == 0x0D:
                break
            name = field_data[0:11].split(b'\x00')[0].decode('ascii', errors='replace').strip()
            ftype = chr(field_data[11])
            flen = field_data[16]
            fdec = field_data[17]
            fields.append({'name': name, 'type': ftype, 'len': flen, 'dec': fdec})
    return fields
