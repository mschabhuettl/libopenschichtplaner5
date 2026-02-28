"""
DBF write support for Schichtplaner5 databases.

Implements:
  append_record(filepath, fields, record)   – append a new record to a .DBF file
  delete_record(filepath, fields, index)    – mark a record as deleted (0x2A flag)
  find_all_records(filepath, fields, **kw) – find all records matching filter criteria

Encoding contract (verified from real SP5 files):
  • String (C) fields: UTF-16 LE string bytes + \x00\x00 null terminator
    + \x20 space padding up to field_len.
    Empty strings: \x00\x00 + \x20 * (field_len - 2).
  • Date (D) fields: 'YYYYMMDD' ASCII, space-padded to field_len.
  • Numeric (N/F) fields: right-aligned ASCII decimal string, space-padded left.
  • Logical (L) fields: 'T' or 'F' (1 byte).
  • Memo (M) fields: all spaces (not written by this module).

Write safety:
  • Exclusive fcntl.flock() around all write operations.
  • Header bytes 1-3 (YY MM DD of last update) updated on every write.
  • EOF marker (0x1A) preserved / re-appended after every write.
  • CDX index files are NOT touched – SP5 will rebuild them on next open.
"""

import fcntl
import os
import struct
from contextlib import contextmanager
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .dbf_reader import _decode_string, _parse_date, get_table_fields


# ─── string / field encoding ──────────────────────────────────────────────────

def _encode_string(value: str, field_len: int) -> bytes:
    """
    Encode a Python string to a Schichtplaner5 C field.

    Format: [UTF-16-LE bytes] [\\x00\\x00 null-terminator] [\\x20 padding …]
    For an empty string the result is [\\x00\\x00] [\\x20 …].
    """
    if field_len <= 0:
        return b''

    if not value:
        # empty string: just null-terminator + spaces
        if field_len >= 2:
            return b'\x00\x00' + b'\x20' * (field_len - 2)
        # field too short for null-terminator – just fill with nulls
        return b'\x00' * field_len

    encoded = value.encode('utf-16-le')

    # Leave 2 bytes for the null terminator (unless field is too small)
    max_content = max(0, field_len - 2)
    # Truncate at even-byte boundary
    if len(encoded) > max_content:
        encoded = encoded[: max_content & ~1]

    null_term = b'\x00\x00' if field_len - len(encoded) >= 2 else b''
    padding   = b'\x20' * (field_len - len(encoded) - len(null_term))
    result    = encoded + null_term + padding

    # Safety: always return exactly field_len bytes
    if len(result) < field_len:
        result += b'\x20' * (field_len - len(result))
    return result[:field_len]


def _encode_field(value: Any, field: Dict) -> bytes:
    """Encode a single value according to its DBF field descriptor."""
    ftype = field['type']
    flen  = field['len']
    fdec  = field['dec']

    if value is None:
        return b' ' * flen

    if ftype == 'C':
        # If bytes are passed directly (e.g. raw binary fields like DIGEST),
        # write them as-is padded to field length.
        if isinstance(value, bytes):
            return (value + b'\x00' * flen)[:flen]
        return _encode_string(str(value) if value else '', flen)

    elif ftype == 'D':
        # Expects 'YYYY-MM-DD' or 'YYYYMMDD'; pads with spaces if empty
        s = str(value).strip() if value else ''
        if len(s) == 10 and s[4] == '-':
            s = s.replace('-', '')          # YYYY-MM-DD → YYYYMMDD
        if len(s) == 8 and s.isdigit():
            return s.encode('ascii').ljust(flen)[:flen]
        return b' ' * flen

    elif ftype in ('N', 'F'):
        try:
            if fdec > 0:
                fmt = f"{{:>{flen}.{fdec}f}}"
                s   = fmt.format(float(value))
            else:
                fmt = f"{{:>{flen}d}}"
                s   = fmt.format(int(float(value)))
        except (ValueError, TypeError):
            s = ' ' * flen
        return s.encode('ascii')[:flen]

    elif ftype == 'L':
        return b'T' if value else b'F'

    elif ftype == 'M':
        return b' ' * flen

    else:
        return str(value).ljust(flen).encode('ascii', errors='replace')[:flen]


# ─── header helpers ───────────────────────────────────────────────────────────

def _read_header_info(filepath: str) -> Tuple[int, int, int]:
    """Return (num_records, header_size, record_size) from the DBF header."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"DBF-Datei nicht gefunden: {filepath}")
    with open(filepath, 'rb') as f:
        hdr = f.read(32)
    if len(hdr) < 32:
        raise ValueError(f"Truncated DBF header: {filepath}")
    num_records = struct.unpack_from('<I', hdr, 4)[0]
    header_size = struct.unpack_from('<H', hdr, 8)[0]
    record_size = struct.unpack_from('<H', hdr, 10)[0]
    return num_records, header_size, record_size


def _stamp_header(f) -> None:
    """Write today's date (YY MM DD) into bytes 1-3 of an already-open file."""
    today = date.today()
    f.seek(1)
    f.write(bytes([today.year % 100, today.month, today.day]))


def _update_record_count(f, new_count: int) -> None:
    """Write the new record count at bytes 4-7 of an already-open file."""
    f.seek(4)
    f.write(struct.pack('<I', new_count))


# ─── file locking ─────────────────────────────────────────────────────────────

@contextmanager
def _exclusive_open(filepath: str):
    """Open filepath for read+write with an exclusive POSIX lock."""
    with open(filepath, 'r+b') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield f
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


# ─── public API ───────────────────────────────────────────────────────────────

def append_record(filepath: str, fields: List[Dict], record: Dict) -> int:
    """
    Append *record* to the end of *filepath*.

    Parameters
    ----------
    filepath : str
        Path to the .DBF file.
    fields : list[dict]
        Field descriptors as returned by :func:`get_table_fields`.
    record : dict
        Mapping of field-name → value.  Missing fields default to None.

    Returns
    -------
    int
        New total record count after appending.
    """
    # Build the raw record bytes (1 active-flag byte + field data)
    row = bytearray(b'\x20')   # delete-flag: active
    for field in fields:
        row += _encode_field(record.get(field['name']), field)

    num_records, header_size, record_size = _read_header_info(filepath)

    # Pad / trim to the exact record_size
    if len(row) < record_size:
        row += b'\x20' * (record_size - len(row))
    row = bytes(row[:record_size])

    with _exclusive_open(filepath) as f:
        # Re-read the record count inside the lock to avoid TOCTOU race:
        # two concurrent appends might both read num_records=N before either
        # acquires the lock, causing both to write new_count=N+1 instead of N+2.
        f.seek(4)
        num_records = struct.unpack('<I', f.read(4))[0]

        # Find write position: just before the EOF marker (0x1A) if present
        f.seek(0, 2)                     # seek to end
        file_end = f.tell()

        # Check whether the very last byte is the EOF marker
        if file_end > 0:
            f.seek(-1, 2)
            last = f.read(1)
            if last == b'\x1a':
                f.seek(-1, 2)            # overwrite the marker
            else:
                f.seek(0, 2)             # append after whatever is there

        f.write(row)
        f.write(b'\x1a')                 # re-append EOF marker

        new_count = num_records + 1
        _update_record_count(f, new_count)
        _stamp_header(f)

    return new_count


def delete_record(filepath: str, fields: List[Dict], record_index: int) -> None:
    """
    Mark record *record_index* as deleted by writing 0x2A at its first byte.

    Parameters
    ----------
    record_index : int
        Zero-based raw index (counting deleted records too), as returned
        by :func:`find_all_records`.
    """
    num_records, header_size, record_size = _read_header_info(filepath)

    if record_index < 0 or record_index >= num_records:
        raise IndexError(
            f"record_index {record_index} out of range (file has {num_records} records)"
        )

    byte_offset = header_size + record_index * record_size

    with _exclusive_open(filepath) as f:
        f.seek(byte_offset)
        current = f.read(1)
        if current == b'\x2a':
            return  # already deleted – nothing to do
        f.seek(byte_offset)
        f.write(b'\x2a')
        _stamp_header(f)


def update_record(
    filepath: str,
    fields: List[Dict],
    record_index: int,
    data: Dict,
) -> None:
    """
    Overwrite specific fields of record *record_index* in-place.

    Parameters
    ----------
    filepath : str
        Path to the .DBF file.
    fields : list[dict]
        Field descriptors as returned by :func:`get_table_fields`.
    record_index : int
        Zero-based raw index (counting deleted records too), as returned
        by :func:`find_all_records`.
    data : dict
        Mapping of field-name → new-value.  Only listed fields are changed;
        all other fields are left untouched.
    """
    num_records, header_size, record_size = _read_header_info(filepath)

    if record_index < 0 or record_index >= num_records:
        raise IndexError(
            f"record_index {record_index} out of range (file has {num_records} records)"
        )

    byte_offset = header_size + record_index * record_size

    # Read AND write under the same exclusive lock to prevent TOCTOU race.
    with _exclusive_open(filepath) as f:
        f.seek(byte_offset)
        raw = bytearray(f.read(record_size))

        if not raw:
            raise ValueError(f"Record {record_index} could not be read (empty read)")

        if raw[0] == 0x2A:
            raise ValueError(f"Record {record_index} is already deleted")

        # Overwrite only the requested fields
        offset = 1  # skip delete-flag byte
        for field in fields:
            if field['name'] in data:
                encoded = _encode_field(data[field['name']], field)
                raw[offset : offset + field['len']] = encoded
            offset += field['len']

        f.seek(byte_offset)
        f.write(bytes(raw))
        _stamp_header(f)


def find_all_records(
    filepath: str,
    fields: Optional[List[Dict]] = None,
    **filters,
) -> List[Tuple[int, Dict]]:
    """
    Return every non-deleted record in *filepath* that matches all *filters*.

    Parameters
    ----------
    filepath : str
        Path to the .DBF file.
    fields : list[dict] | None
        Field descriptors.  Loaded automatically if not supplied.
    **filters :
        Keyword arguments specifying field → expected-value pairs.
        All must match (AND semantics).

    Returns
    -------
    list[tuple[int, dict]]
        Each tuple is (raw_record_index, record_dict).
        *raw_record_index* is the 0-based index in the file (counting deleted
        records too) and can be passed directly to :func:`delete_record`.
    """
    if not os.path.exists(filepath):
        return []

    if fields is None:
        fields = get_table_fields(filepath)

    try:
        num_records, header_size, record_size = _read_header_info(filepath)
    except (FileNotFoundError, OSError, ValueError):
        # File removed or corrupted between the exists-check and open
        return []

    results: List[Tuple[int, Dict]] = []

    try:
        open_file = open(filepath, 'rb')
    except OSError:
        return []

    with open_file as f:
        # Shared (read) lock
        fcntl.flock(f.fileno(), fcntl.LOCK_SH)
        try:
            f.seek(header_size)
            for raw_idx in range(num_records):
                raw = f.read(record_size)
                if not raw or len(raw) < record_size:
                    break
                if raw[0] == 0x2A:
                    continue  # deleted

                record = _parse_record(raw, fields)

                if _matches(record, filters):
                    results.append((raw_idx, record))
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    return results


# ─── internal parsing (re-uses dbf_reader helpers) ────────────────────────────

def _parse_record(raw: bytes, fields: List[Dict]) -> Dict[str, Any]:
    """Parse a raw record byte-string into a dict using the given field descriptors."""
    record: Dict[str, Any] = {}
    offset = 1  # skip delete-flag

    for field in fields:
        chunk = raw[offset : offset + field['len']]
        ftype = field['type']
        fname = field['name']

        if ftype == 'C':
            val = _decode_string(chunk)
        elif ftype == 'D':
            val = _parse_date(chunk.decode('ascii', errors='replace'))
        elif ftype in ('N', 'F'):
            s = chunk.decode('ascii', errors='replace').strip()
            if not s or s == '.':
                val = 0
            else:
                try:
                    val = float(s) if ('.' in s or field['dec'] > 0) else int(s)
                except ValueError:
                    val = 0
        elif ftype == 'L':
            s = chunk.decode('ascii', errors='replace').strip()
            val = s in ('T', 't', 'Y', 'y', '1')
        elif ftype == 'M':
            val = None
        else:
            val = chunk.decode('ascii', errors='replace').strip()

        record[fname] = val
        offset += field['len']

    return record


def _matches(record: Dict, filters: Dict) -> bool:
    """Return True if *record* satisfies all key=value pairs in *filters*."""
    for key, expected in filters.items():
        if record.get(key) != expected:
            return False
    return True
