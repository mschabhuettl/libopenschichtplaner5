"""
DBF write support for Schichtplaner5 databases.

Implements:
  append_record(filepath, fields, record)   – append a new record to a .DBF file
  update_record(filepath, fields, idx, d)   – overwrite fields of a record in-place
  delete_record(filepath, fields, index)    – mark a record as deleted (0x2A flag)
  find_all_records(filepath, fields, **kw) – find all records matching filter criteria

Encoding contract (verified byte-by-byte against the original sample DB):
  • Text (C) fields (Spec D-19): UTF-16 LE string bytes + \x00\x00 null
    terminator + \x20 space padding up to field_len.
    Empty strings: \x00\x00 + \x20 * (field_len - 2).
  • ASCII-class (C) fields (Spec D-20/D-31: WORKDAYS, VALIDDAYS, DAILYDEM,
    STARTEND*, CATEGORY/REPORT in 5USER): plain cp1252 bytes, \x20-padded.
    Empty value: all spaces.
  • Binary (C) fields (Spec D-21: DIGEST, CREATIME, UUID): raw bytes,
    \x00-padded.
  • Date (D) fields: 'YYYYMMDD' ASCII, space-padded to field_len.
  • Numeric (N) fields: right-aligned ASCII decimal string, space-padded left.
  • Float (F) fields (Spec D-15): right-aligned ASCII decimal with exactly 4
    fraction digits (oracle: '7.7000'), regardless of the descriptor's dec
    byte (the SP5 schema declares every F field as 'F 19 dec=0').
  • Logical (L) fields: 'T' or 'F' (1 byte; unused by SP5, Spec D-18).
  • Memo (M) fields: all spaces (not written by this module).
  • Duplicate field names (5DADEM's two START fields, Spec D-55) are addressed
    position-based: the second occurrence is keyed 'START2' in record dicts,
    matching the read path (dbf_reader._dedupe_names).

Change journal (-L companion tables, Spec §2.7):
  • Every append/update/delete also appends one entry to '<table>-L.DBF':
    NUMBER = last journal NUMBER + 1, CHANGEID1..3 = the record's composite
    key per Spec D-41 (unused components 0), CHANGE = 1 (record added/changed,
    upsert semantics) or 2 (record deleted). Original clients poll these
    journals to pick up external changes (Spec D-69/D-71/D-76).
  • Journaling is unconditional — the original's -L upkeep is not gated by
    5USETT.CHANGELOG (Spec D-68).
  • If the -L file is missing, the journal entry is skipped with a warning;
    the main write succeeds. (The original recreates missing companion files
    on next open, Spec D-14.)

CDX index files (Spec D-13/D-14):
  • SP5/CodeBase only recreates *missing* index files on open; an existing
    stale CDX would be reused and would no longer match the table after a
    write from this module. Therefore the .CDX files of the modified table
    (main and -L) are DELETED after every successful write, which forces the
    original to rebuild them from its compiled-in tag definitions (D-14).
    Header byte 28 (MDX/CDX flag, 0x01 in all original files) is left
    untouched. Set INVALIDATE_CDX = False to opt out (only safe if the data
    is never opened by an original SP5 client again).

Write safety:
  • Exclusive fcntl.flock() around all write operations.
  • Header bytes 1-3 (YY MM DD of last update) updated on every write.
  • EOF marker (0x1A) preserved / re-appended after every write.

Known interop limitation (Spec D-16): the original uses CodeBase byte-range
locks inside the DBF files, taken as an atomic group lock over the main and
-L files of all tables. This module uses POSIX flock() per file instead —
the two locking schemes do not see each other, and data + journal writes are
not atomic as a pair. Concurrent writing while an original SP5 client is
running is therefore NOT safe; sequential coexistence (original closed during
lib writes) is.
"""

import fcntl
import logging
import os
import struct
from contextlib import contextmanager
from datetime import date
from typing import Any

from .dbf_reader import (
    _dedupe_names,
    _parse_record,
    get_table_fields,
    read_dbf,
)

logger = logging.getLogger(__name__)

#: Delete the .CDX files of a modified table after every successful write
#: (see module docstring, "CDX index files"). Interop-safe default.
INVALIDATE_CDX = True

#: ASCII-class C fields (Spec D-20): plain cp1252, space-separated lists.
#: STARTEND* (D-31) belongs to the same class. Name-based matching is exact
#: for the fixed 30-table schema (CATEGORY/REPORT exist as C fields only in
#: 5USER; the CATEGORY fields of 5SHIFT/5LEAVT are N-typed).
_ASCII_C_FIELDS = {"WORKDAYS", "VALIDDAYS", "DAILYDEM", "CATEGORY", "REPORT"}


def _is_ascii_c_field(name: str) -> bool:
    return name in _ASCII_C_FIELDS or name.startswith("STARTEND")


# ─── string / field encoding ──────────────────────────────────────────────────


def _encode_string(value: str, field_len: int) -> bytes:
    """
    Encode a Python string to a Schichtplaner5 C field.

    Format: [UTF-16-LE bytes] [\\x00\\x00 null-terminator] [\\x20 padding …]
    For an empty string the result is [\\x00\\x00] [\\x20 …].
    """
    if field_len <= 0:
        return b""

    if not value:
        # empty string: just null-terminator + spaces
        if field_len >= 2:
            return b"\x00\x00" + b"\x20" * (field_len - 2)
        # field too short for null-terminator – just fill with nulls
        return b"\x00" * field_len

    encoded = value.encode("utf-16-le")

    # Leave 2 bytes for the null terminator (unless field is too small)
    max_content = max(0, field_len - 2)
    # Truncate at even-byte boundary – emit a warning so data loss is visible
    if len(encoded) > max_content:
        max_chars = (max_content & ~1) // 2
        logger.warning(
            "DBF field truncation: value '%s...' (%d chars) exceeds field capacity "
            "(%d chars / %d bytes). Truncating silently.",
            value[:30],
            len(value),
            max_chars,
            max_content & ~1,
        )
        encoded = encoded[: max_content & ~1]

    null_term = b"\x00\x00" if field_len - len(encoded) >= 2 else b""
    padding = b"\x20" * (field_len - len(encoded) - len(null_term))
    result = encoded + null_term + padding

    # Safety: always return exactly field_len bytes
    if len(result) < field_len:
        result += b"\x20" * (field_len - len(result))
    return result[:field_len]


def _encode_field(value: Any, field: dict) -> bytes:
    """Encode a single value according to its DBF field descriptor."""
    ftype = field["type"]
    flen = field["len"]
    fdec = field["dec"]

    if value is None:
        return b" " * flen

    if ftype == "C":
        # Binary fields (Spec D-21): raw bytes, \x00-padded.
        if isinstance(value, bytes):
            return (value + b"\x00" * flen)[:flen]
        if _is_ascii_c_field(field["name"]):
            # ASCII-class fields (Spec D-20/D-31): plain cp1252, space-padded.
            s = str(value) if value else ""
            encoded = s.encode("cp1252", errors="replace")
            if len(encoded) > flen:
                logger.warning(
                    "DBF ASCII field truncation: %s value '%s' exceeds %d bytes",
                    field["name"], s[:30], flen,
                )
                encoded = encoded[:flen]
            return encoded + b"\x20" * (flen - len(encoded))
        return _encode_string(str(value) if value else "", flen)

    elif ftype == "D":
        # Expects 'YYYY-MM-DD' or 'YYYYMMDD'; pads with spaces if empty
        s = str(value).strip() if value else ""
        if len(s) == 10 and s[4] == "-":
            s = s.replace("-", "")  # YYYY-MM-DD → YYYYMMDD
        if len(s) == 8 and s.isdigit():
            return s.encode("ascii").ljust(flen)[:flen]
        return b" " * flen

    elif ftype in ("N", "F"):
        try:
            if ftype == "F":
                # Spec D-15: F fields always carry 4 fraction digits in the
                # original files ('7.7000'), even though the descriptor says
                # dec=0. Match that byte format.
                s = f"{{:>{flen}.4f}}".format(float(value))
            elif fdec > 0:
                fmt = f"{{:>{flen}.{fdec}f}}"
                s = fmt.format(float(value))
            else:
                fmt = f"{{:>{flen}d}}"
                s = fmt.format(int(float(value)))
        except (ValueError, TypeError):
            return b" " * flen
        # A right-aligned numeric format never truncates an over-wide value, so
        # slicing it would silently drop the *high-order* digits/sign and corrupt
        # the stored magnitude (e.g. 99999 -> "9999"). Refuse instead of corrupting.
        if len(s) > flen:
            raise ValueError(
                f"Numeric value {value!r} does not fit field "
                f"{field.get('name', '?')} (len={flen}, dec={fdec})"
            )
        return s.encode("ascii")

    elif ftype == "L":
        return b"T" if value else b"F"

    elif ftype == "M":
        return b" " * flen

    else:
        return str(value).ljust(flen).encode("ascii", errors="replace")[:flen]


# ─── header helpers ───────────────────────────────────────────────────────────


def _read_header_info(filepath: str) -> tuple[int, int, int]:
    """Return (num_records, header_size, record_size) from the DBF header."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"DBF-Datei nicht gefunden: {filepath}")
    with open(filepath, "rb") as f:
        hdr = f.read(32)
    if len(hdr) < 32:
        raise ValueError(f"Truncated DBF header: {filepath}")
    num_records = struct.unpack_from("<I", hdr, 4)[0]
    header_size = struct.unpack_from("<H", hdr, 8)[0]
    record_size = struct.unpack_from("<H", hdr, 10)[0]
    return num_records, header_size, record_size


def _stamp_header(f) -> None:
    """Write today's date (YY MM DD) into bytes 1-3 of an already-open file."""
    today = date.today()
    f.seek(1)
    f.write(bytes([today.year % 100, today.month, today.day]))


def _update_record_count(f, new_count: int) -> None:
    """Write the new record count at bytes 4-7 of an already-open file."""
    f.seek(4)
    f.write(struct.pack("<I", new_count))


# ─── file locking ─────────────────────────────────────────────────────────────


@contextmanager
def _exclusive_open(filepath: str):
    """Open filepath for read+write with an exclusive POSIX lock."""
    with open(filepath, "r+b") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield f
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


# ─── change journal & index invalidation ──────────────────────────────────────

#: Composite journal keys per table (Spec D-41): CHANGEID1..3 = key components.
#: Tables not listed here have the single-component key (ID).
_JOURNAL_KEYS: dict[str, tuple[str, ...]] = {
    "5ABSEN": ("ID", "EMPLOYEEID"),
    "5BOOK": ("ID", "EMPLOYEEID"),
    "5CYASS": ("ID", "EMPLOYEEID"),
    "5GRASG": ("ID", "EMPLOYEEID"),
    "5LEAEN": ("ID", "EMPLOYEEID"),
    "5MASHI": ("ID", "EMPLOYEEID"),
    "5NOTE": ("ID", "EMPLOYEEID"),
    "5OVER": ("ID", "EMPLOYEEID"),
    "5RESTR": ("ID", "EMPLOYEEID"),
    "5SPSHI": ("ID", "EMPLOYEEID"),
    "5DADEM": ("ID", "GROUPID"),
    "5HOBAN": ("ID", "GROUPID"),
    "5PERIO": ("ID", "GROUPID"),
    "5SHDEM": ("ID", "GROUPID"),
    "5SPDEM": ("ID", "GROUPID"),
    "5EMACC": ("ID", "USERID"),
    "5GRACC": ("ID", "USERID"),
    "5CYENT": ("ID", "CYCLEEID"),
    "5CYEXC": ("ID", "EMPLOYEEID", "CYCLEASSID"),
}


def _table_stem(filepath: str) -> str:
    return os.path.splitext(os.path.basename(filepath))[0]


def _is_journal_file(filepath: str) -> bool:
    return _table_stem(filepath).upper().endswith("-L")


def _journal_path(filepath: str) -> str | None:
    """Return the path of the table's -L companion, or None if absent."""
    base, ext = os.path.splitext(filepath)
    for suffix in ("-L", "-l"):
        candidate = base + suffix + ext
        if os.path.exists(candidate):
            return candidate
    return None


def _invalidate_cdx(filepath: str) -> None:
    """Delete the table's stale .CDX so the original rebuilds it (Spec D-14)."""
    if not INVALIDATE_CDX:
        return
    base, _ = os.path.splitext(filepath)
    for ext in (".CDX", ".cdx"):
        candidate = base + ext
        if os.path.exists(candidate):
            try:
                os.remove(candidate)
            except OSError as exc:
                logger.warning("Could not remove stale index %s: %s", candidate, exc)


def _append_journal(filepath: str, record: dict, change: int) -> None:
    """Append a change-journal entry (Spec D-69/D-72) for *record* of *filepath*.

    change: 1 = record added/changed (upsert), 2 = record deleted.
    Missing -L file: skip with a warning (main write stays valid).
    """
    jpath = _journal_path(filepath)
    if jpath is None:
        logger.warning(
            "DBF change journal missing for %s — entry skipped "
            "(running original clients will not see this change)",
            filepath,
        )
        return

    entries = read_dbf(jpath)
    next_number = max((int(e.get("NUMBER", 0) or 0) for e in entries), default=0) + 1

    keys = _JOURNAL_KEYS.get(_table_stem(filepath).upper(), ("ID",))
    ids = [int(record.get(k) or 0) for k in keys]
    ids += [0] * (3 - len(ids))

    journal_record = {
        "NUMBER": next_number,
        "CHANGEID1": ids[0],
        "CHANGEID2": ids[1],
        "CHANGEID3": ids[2],
        "CHANGE": change,
    }
    # append_record on a -L file does not journal again (guard via _is_journal_file)
    append_record(jpath, get_table_fields(jpath), journal_record)


def _after_write(filepath: str, record: dict, change: int) -> None:
    """Post-write upkeep: journal entry (D-69) + index invalidation (D-14)."""
    if not _is_journal_file(filepath):
        _append_journal(filepath, record, change)
    _invalidate_cdx(filepath)


# ─── public API ───────────────────────────────────────────────────────────────


def append_record(filepath: str, fields: list[dict], record: dict) -> int:
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
    # Build the raw record bytes (1 active-flag byte + field data).
    # Field values are looked up by deduplicated name (5DADEM: START/START2),
    # matching the read path.
    row = bytearray(b"\x20")  # delete-flag: active
    names = _dedupe_names([str(f["name"]) for f in fields])
    for field, fname in zip(fields, names, strict=True):
        row += _encode_field(record.get(fname), field)

    num_records, header_size, record_size = _read_header_info(filepath)

    # Record-Size-Mismatch: eine zu lange Zeile stillschweigend abzuschneiden
    # würde Feldgrenzen verschieben und den Satz korrumpieren (falsche
    # fields-Liste oder beschädigter Header) — ablehnen statt korrumpieren.
    if len(row) > record_size:
        raise ValueError(
            f"Record size mismatch for {filepath}: encoded row is {len(row)} bytes, "
            f"header says record_size={record_size} — fields list does not match the file"
        )
    # Shorter rows are padded (defensive for headers with trailing slack).
    if len(row) < record_size:
        row += b"\x20" * (record_size - len(row))
    row_bytes: bytes = bytes(row)

    with _exclusive_open(filepath) as f:
        # Re-read the record count inside the lock to avoid TOCTOU race:
        # two concurrent appends might both read num_records=N before either
        # acquires the lock, causing both to write new_count=N+1 instead of N+2.
        f.seek(4)
        num_records = struct.unpack("<I", f.read(4))[0]

        # Find write position: just before the EOF marker (0x1A) if present
        f.seek(0, 2)  # seek to end
        file_end = f.tell()

        # Check whether the very last byte is the EOF marker
        if file_end > 0:
            f.seek(-1, 2)
            last = f.read(1)
            if last == b"\x1a":
                f.seek(-1, 2)  # overwrite the marker
            else:
                f.seek(0, 2)  # append after whatever is there

        write_pos = f.tell()  # remember rollback point
        try:
            f.write(row_bytes)
            f.write(b"\x1a")  # re-append EOF marker

            new_count = num_records + 1
            _update_record_count(f, new_count)
            _stamp_header(f)
        except Exception:
            # Attempt rollback: truncate back to pre-write state so the file
            # is not left partially written (e.g. on disk-full errors).
            try:
                f.truncate(write_pos)
                f.flush()
            except Exception as trunc_err:
                logger.error(
                    "DBF rollback failed after write error — file may be corrupted: "
                    "%s (truncate error: %s)",
                    filepath,
                    trunc_err,
                )
            raise

    _after_write(filepath, record, change=1)
    return new_count


def delete_record(filepath: str, fields: list[dict], record_index: int) -> None:
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
        raw = f.read(record_size)
        if not raw:
            raise ValueError(f"Record {record_index} could not be read (empty read)")
        if raw[0] == 0x2A:
            return  # already deleted – nothing to do
        f.seek(byte_offset)
        f.write(b"\x2a")
        _stamp_header(f)

    # Journal entry needs the deleted record's key components (Spec D-69).
    _after_write(filepath, _parse_record(raw, fields), change=2)


def update_record(
    filepath: str,
    fields: list[dict],
    record_index: int,
    data: dict,
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

        # Overwrite only the requested fields (keys are deduplicated names,
        # 5DADEM: START/START2 — matching the read path)
        names = _dedupe_names([str(f_["name"]) for f_ in fields])
        offset = 1  # skip delete-flag byte
        for field, fname in zip(fields, names, strict=True):
            if fname in data:
                encoded = _encode_field(data[fname], field)
                raw[offset : offset + field["len"]] = encoded
            offset += field["len"]

        f.seek(byte_offset)
        f.write(bytes(raw))
        _stamp_header(f)

    _after_write(filepath, _parse_record(bytes(raw), fields), change=1)


def _zap_journal(filepath: str) -> None:
    """Zap the -L change journal of *filepath* (Spec D-74).

    "Komprimieren" empties the journal over the whole number range and resets
    the counter to 0; original clients detect this and do a full reload
    (CHANGE=0 semantics). Missing -L file: nothing to do.
    """
    jpath = _journal_path(filepath)
    if jpath is None:
        return
    _num, header_size, _record_size = _read_header_info(jpath)
    with _exclusive_open(jpath) as f:
        f.truncate(header_size)
        f.seek(header_size)
        f.write(b"\x1a")  # EOF marker directly after the header
        _update_record_count(f, 0)
        _stamp_header(f)
    _invalidate_cdx(jpath)


def pack_table(filepath: str) -> int:
    """PACK *filepath*: physically remove deleted records (Spec 1.14/D-11).

    Rewrites the record area without the records flagged ``0x2A``, updates
    the header record count and date stamp and re-appends the EOF marker —
    all under an exclusive lock. Afterwards the table's -L change journal is
    zapped (counter reset to 0, Spec D-74) and the stale CDX files of the
    main table and the journal are deleted (D-14), because record positions
    have changed. If the table contains no deleted records, the file is left
    untouched. Returns the number of physically removed records.
    """
    num_records, header_size, record_size = _read_header_info(filepath)
    if record_size <= 0:
        return 0

    removed = 0
    with _exclusive_open(filepath) as f:
        # Re-read the count inside the lock (TOCTOU, cf. append_record)
        f.seek(4)
        num_records = struct.unpack("<I", f.read(4))[0]
        f.seek(header_size)
        records_area = f.read(num_records * record_size)
        kept = []
        for i in range(num_records):
            rec = records_area[i * record_size : (i + 1) * record_size]
            if len(rec) < record_size:
                break  # truncated trailing record — drop it
            if rec[0:1] == b"\x2a":
                removed += 1
            else:
                kept.append(rec)
        if removed == 0 and len(kept) == num_records:
            return 0  # nothing to pack — keep file, journal and CDX intact

        f.seek(header_size)
        for rec in kept:
            f.write(rec)
        f.write(b"\x1a")  # EOF marker
        f.truncate()
        _update_record_count(f, len(kept))
        _stamp_header(f)

    if not _is_journal_file(filepath):
        _zap_journal(filepath)
    _invalidate_cdx(filepath)
    return removed


def find_all_records(
    filepath: str,
    fields: list[dict] | None = None,
    **filters,
) -> list[tuple[int, dict]]:
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

    results: list[tuple[int, dict]] = []

    try:
        open_file = open(filepath, "rb")
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


def _matches(record: dict, filters: dict) -> bool:
    """Return True if *record* satisfies all key=value pairs in *filters*."""
    for key, expected in filters.items():
        if record.get(key) != expected:
            return False
    return True
