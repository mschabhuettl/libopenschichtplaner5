"""
Pure Python DBF/dBASE reader for Schichtplaner5 databases.
Handles UTF-16 LE string encoding used by the Delphi/FoxPro application.
"""

import struct
from datetime import date
from typing import Any

#: Binary C fields (Spec D-21): the content is raw bytes, not text. These are
#: returned (and written) as unstripped ``bytes`` of the full field width.
#: Name-based matching is exact for the fixed 30-table SP5 schema — verified
#: against the reference DB headers: DIGEST exists only in 5USER, CREATIME and
#: UUID only in 5BUILD. (RESERVED fields are deliberately not included here:
#: existing callers consume them as strings.)
BINARY_C_FIELDS = {"DIGEST", "CREATIME", "UUID"}


def _dedupe_names(names: list[str]) -> list[str]:
    """Disambiguate duplicate field names position-based.

    The physical 5DADEM header carries *two* fields named ``START`` (original
    schema bug, Spec D-55; the original binds fields ordinally, D-12, so the
    duplicate is harmless there). Because records are exposed as dicts, the
    second (and any further) occurrence of a name gets a numeric suffix:
    5DADEM's period end date is exposed as ``START2``. The write path
    (``dbf_writer``) applies the same convention, so ``START``/``START2``
    round-trip. No SP5 table contains a literal field name that collides with
    a generated suffix name.
    """
    seen: dict[str, int] = {}
    result = []
    for name in names:
        seen[name] = seen.get(name, 0) + 1
        result.append(name if seen[name] == 1 else f"{name}{seen[name]}")
    return result


def _is_utf16_le(raw: bytes) -> bool:
    """
    Heuristic: detect if raw bytes are UTF-16 LE encoded text.

    In UTF-16 LE Latin-1 text, bytes at odd positions (1, 3, 5, ...) are 0x00;
    for non-Latin scripts up to Arabic (Greek 0x03xx, Cyrillic 0x04xx, Hebrew
    0x05xx, Arabic 0x06xx) they are 0x01..0x07. Plain ASCII data fields
    (WORKDAYS, STARTEND*, ...) contain only printable bytes >= 0x20 at odd
    positions, so any odd byte < 0x08 marks UTF-16 LE text. Known limitation:
    text consisting ONLY of characters >= U+0800 (e.g. pure CJK) is still
    misdetected as ASCII.
    """
    if len(raw) < 4:
        # Very short field — check if the second byte is a UTF-16 high byte
        return len(raw) >= 2 and raw[1] < 0x08
    # Sample up to 8 bytes for detection
    sample_len = min(8, len(raw))
    sample = raw[:sample_len]
    odd_bytes = sample[1::2]
    high_count = sum(1 for b in odd_bytes if b < 0x08)
    # More than half of odd-position bytes are UTF-16 high bytes → UTF-16 LE
    return high_count > len(odd_bytes) // 2


def _decode_string(raw: bytes) -> str:
    """
    Decode a string field from Schichtplaner5 .DBF files.

    SP5 uses two different encodings for Character fields:
    - Text fields (NAME, SHORTNAME, etc.): UTF-16 LE, padded with 0x20
    - Data fields (WORKDAYS, STARTEND*, etc.): plain ASCII, padded with 0x20

    We detect UTF-16 LE by checking if odd-indexed bytes are 0x00.
    """
    if not raw:
        return ""

    if _is_utf16_le(raw):
        # UTF-16 LE encoded text: find null terminator (0x00 0x00 at even offset)
        end = len(raw)
        for i in range(0, len(raw) - 1, 2):
            if raw[i] == 0x00 and raw[i + 1] == 0x00:
                end = i
                break
        chunk = raw[:end]
        if not chunk:
            return ""
        try:
            return chunk.decode("utf-16-le").strip()
        except Exception:
            pass

    # Plain ASCII / binary data field (WORKDAYS, STARTEND*, etc.)
    # Strip trailing spaces/nulls and decode as latin-1 to preserve all byte values
    stripped = raw.rstrip(b"\x00\x20")
    try:
        return stripped.decode("latin-1").strip()
    except Exception:
        return raw.split(b"\x00")[0].decode("latin-1", errors="replace").strip()


def _parse_date(raw: str) -> str | None:
    """Parse dBASE date string YYYYMMDD to ISO format.

    Returns None for anything that is not a real calendar date. Full calendar
    validation (via :class:`datetime.date`) rejects impossible dates such as
    ``20230231`` (Feb 31), which the previous ``day <= 31`` check let through and
    which would later crash ``date.fromisoformat`` downstream.
    """
    s = raw.strip()
    if len(s) == 8 and s.isdigit():
        try:
            year, month, day = int(s[:4]), int(s[4:6]), int(s[6:8])
            if year > 0:
                # Constructing date() validates month/day for the given month/year.
                return date(year, month, day).isoformat()
        except ValueError:
            pass
    return None


def _parse_record(
    raw: bytes, fields: list[dict], names: list[str] | None = None
) -> dict[str, Any]:
    """Parse one raw record byte-string into a dict.

    *names* are the (deduplicated) dict keys for the fields; computed from the
    field descriptors when not supplied. Binary C fields (``BINARY_C_FIELDS``)
    are returned as raw unstripped ``bytes``.
    """
    if names is None:
        names = _dedupe_names([str(f["name"]) for f in fields])

    record: dict[str, Any] = {}
    offset = 1  # skip deletion flag

    for field, fname in zip(fields, names, strict=True):
        flen = int(field["len"])
        ftype = str(field["type"])
        fdec = int(field["dec"])
        chunk = raw[offset : offset + flen]

        val: Any = None
        if ftype == "C":
            if str(field["name"]) in BINARY_C_FIELDS:
                # Binary field (D-21): raw bytes, unstripped (a stripped or
                # latin-1-decoded MD5 digest is irreversibly mangled).
                val = chunk
            else:
                # Character field - UTF-16 LE in Schichtplaner5
                val = _decode_string(chunk)
        elif ftype == "D":
            # Date field YYYYMMDD
            val = _parse_date(chunk.decode("ascii", errors="replace"))
        elif ftype in ("N", "F"):
            # Numeric/Float
            s = chunk.decode("ascii", errors="replace").strip()
            if s == "" or s == ".":
                val = 0
            else:
                try:
                    val = float(s) if "." in s or fdec > 0 else int(s)
                except ValueError:
                    val = 0
        elif ftype == "L":
            # Logical
            s = chunk.decode("ascii", errors="replace").strip()
            val = s in ("T", "t", "Y", "y", "1")
        elif ftype == "M":
            # Memo (pointer only in .DBF, actual data in .DBT)
            val = None
        else:
            val = chunk.decode("ascii", errors="replace").strip()

        record[fname] = val
        offset += flen

    return record


def read_dbf(filepath: str, encoding_hint: str = "utf-16-le") -> list[dict[str, Any]]:
    """
    Read a .DBF file and return a list of records as dicts.
    String fields are decoded as UTF-16 LE (as used by Schichtplaner5);
    binary C fields (``BINARY_C_FIELDS``) come back as raw ``bytes``.
    Duplicate field names are disambiguated position-based (``_dedupe_names``):
    5DADEM's second ``START`` field is exposed as ``START2``.

    Returns an empty list if the file does not exist, is unreadable, or is
    corrupted — callers should treat an empty result as "no data" and not
    crash.
    """
    try:
        open_file = open(filepath, "rb")
    except OSError:
        # File missing, no permissions, or deleted between exists-check and open
        return []

    with open_file as f:
        # Read header (32 bytes)
        header = f.read(32)
        if len(header) < 32:
            return []

        num_records = struct.unpack_from("<I", header, 4)[0]
        header_size = struct.unpack_from("<H", header, 8)[0]
        record_size = struct.unpack_from("<H", header, 10)[0]

        # Read field descriptors (32 bytes each, terminated by 0x0D)
        fields: list[dict[str, Any]] = []
        f.seek(32)
        while True:
            field_data = f.read(32)
            if not field_data or len(field_data) < 32 or field_data[0] == 0x0D:
                break
            name = (
                field_data[0:11]
                .split(b"\x00")[0]
                .decode("ascii", errors="replace")
                .strip()
            )
            ftype = chr(field_data[11])
            flen = field_data[16]
            fdec = field_data[17]
            fields.append({"name": name, "type": ftype, "len": flen, "dec": fdec})

        # Read records
        f.seek(header_size)
        records = []
        names = _dedupe_names([str(f_["name"]) for f_ in fields])

        for _ in range(num_records):
            raw = f.read(record_size)
            if not raw or len(raw) < record_size:
                break

            # Skip deleted records (first byte = 0x2A = '*')
            if raw[0] == 0x2A:
                continue

            records.append(_parse_record(raw, fields, names))

    return records


def get_table_fields(filepath: str) -> list[dict[str, Any]]:
    """Return field definitions for a .DBF file."""
    try:
        open_file = open(filepath, "rb")
    except OSError:
        return []
    with open_file as f:
        hdr = f.read(32)
        if len(hdr) < 32:
            return []  # empty or truncated file
        fields = []
        while True:
            field_data = f.read(32)
            if not field_data or len(field_data) < 32 or field_data[0] == 0x0D:
                break
            name = (
                field_data[0:11]
                .split(b"\x00")[0]
                .decode("ascii", errors="replace")
                .strip()
            )
            ftype = chr(field_data[11])
            flen = field_data[16]
            fdec = field_data[17]
            fields.append({"name": name, "type": ftype, "len": flen, "dec": fdec})
    return fields
