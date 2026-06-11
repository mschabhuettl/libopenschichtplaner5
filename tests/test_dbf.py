"""Self-contained tests for the pure DBF read/write core of sp5lib.

These build their own tiny in-memory .DBF files, so they need no external
fixtures and exercise the encode/decode round-trip plus the data-integrity
guards (numeric overflow, calendar-date validation).
"""

import os
import struct
import tempfile
from datetime import date

import pytest

from sp5lib.dbf_reader import _decode_string, _is_utf16_le, _parse_date, read_dbf
from sp5lib.dbf_writer import (
    _encode_field,
    _encode_string,
    append_record,
    find_all_records,
    get_table_fields,
)

# ─── helpers: build a minimal valid DBF ───────────────────────────────────────


def _field_descriptor(name, ftype, length, dec=0):
    name_bytes = name.upper().encode("ascii")[:11].ljust(11, b"\x00")
    return name_bytes + ftype.encode("ascii") + b"\x00" * 4 + bytes([length, dec]) + b"\x00" * 14


def _make_dbf(fields_spec):
    n_fields = len(fields_spec)
    record_size = 1 + sum(f[2] for f in fields_spec)
    header_size = 32 + 32 * n_fields + 1
    hdr = bytearray(32)
    hdr[0] = 0x03
    today = date.today()
    hdr[1], hdr[2], hdr[3] = today.year % 100, today.month, today.day
    struct.pack_into("<I", hdr, 4, 0)
    struct.pack_into("<H", hdr, 8, header_size)
    struct.pack_into("<H", hdr, 10, record_size)
    field_bytes = b"".join(_field_descriptor(*f) for f in fields_spec)
    return bytes(hdr) + field_bytes + b"\x0d" + b"\x1a"


def _write_temp_dbf(fields_spec):
    fd, path = tempfile.mkstemp(suffix=".DBF")
    os.close(fd)
    with open(path, "wb") as f:
        f.write(_make_dbf(fields_spec))
    return path


SPEC = [("ID", "N", 4, 0), ("NAME", "C", 40, 0)]


# ─── _parse_date ──────────────────────────────────────────────────────────────


def test_parse_date_valid():
    assert _parse_date("20240615") == "2024-06-15"
    assert _parse_date("20240229") == "2024-02-29"  # leap day


@pytest.mark.parametrize("bad", ["", "abcdefgh", "20241301", "20230231", "20230229", "20230431"])
def test_parse_date_invalid(bad):
    assert _parse_date(bad) is None


# ─── string decode ────────────────────────────────────────────────────────────


def test_decode_utf16():
    raw = "Test".encode("utf-16-le") + b"\x00\x00"
    assert _is_utf16_le(raw) is True
    assert _decode_string(raw) == "Test"


def test_decode_ascii():
    raw = b"WORKDAYS   "
    assert _is_utf16_le(raw) is False
    assert "WORKDAYS" in _decode_string(raw)


# ─── numeric encode / overflow guard ──────────────────────────────────────────


def test_encode_numeric_fits():
    assert _encode_field(42, {"type": "N", "len": 4, "dec": 0}) == b"  42"


def test_encode_numeric_overflow_raises():
    with pytest.raises(ValueError, match="does not fit"):
        _encode_field(99999, {"type": "N", "len": 4, "dec": 0, "name": "X"})


def test_encode_string_truncates_safely():
    out = _encode_string("A", 1)
    assert len(out) == 1


# ─── round-trip append → read ─────────────────────────────────────────────────


def test_append_read_roundtrip():
    path = _write_temp_dbf(SPEC)
    try:
        fields = get_table_fields(path)
        for i, name in enumerate(["Müller", "Köhler", "Weiß"]):
            append_record(path, fields, {"ID": i + 1, "NAME": name})
        rows = read_dbf(path)
        names = {r["NAME"] for r in rows}
        assert {"Müller", "Köhler", "Weiß"} <= names
        # find_all_records returns (index, record) tuples
        assert len(find_all_records(path, fields)) == 3
    finally:
        os.unlink(path)


def test_read_missing_file_returns_empty():
    assert read_dbf("/nonexistent/path/FAKE.DBF") == []


# ─── record-size mismatch guard ───────────────────────────────────────────────


def test_append_record_size_mismatch_raises():
    """Repro: eine fields-Liste, die nicht zur Datei passt, erzeugte eine zu
    lange Zeile, die stillschweigend abgeschnitten wurde (verschobene
    Feldgrenzen = korrupter Satz). Jetzt: ValueError, Datei unverändert."""
    path = _write_temp_dbf(SPEC)
    try:
        fields = get_table_fields(path)
        wrong_fields = fields + [{"name": "EXTRA", "type": "C", "len": 10, "dec": 0}]
        before = open(path, "rb").read()
        with pytest.raises(ValueError, match="Record size mismatch"):
            append_record(path, wrong_fields, {"ID": 1, "NAME": "X", "EXTRA": "y"})
        assert open(path, "rb").read() == before  # nichts geschrieben
    finally:
        os.unlink(path)


def test_append_record_into_empty_table():
    """Randfall leere Tabelle: erster Append landet direkt hinter dem Header."""
    path = _write_temp_dbf(SPEC)
    try:
        fields = get_table_fields(path)
        assert append_record(path, fields, {"ID": 1, "NAME": "Erster"}) == 1
        rows = read_dbf(path)
        assert len(rows) == 1 and rows[0]["NAME"] == "Erster"
    finally:
        os.unlink(path)
