"""Byte-level parity tests for the DBF write path against the original format.

Covers the encoding contract fixes from the parity review (meta docs
parity-lib-write.md / parity-lib-decode.md): ASCII-class fields (W1),
F-field decimal format (W2), change journal (W3), CDX invalidation (W4),
binary fields (B2) and the 5DADEM duplicate-START convention (B1).
All fixtures are synthetic — no original data involved.
"""

import struct
from datetime import date

from sp5lib.dbf_reader import read_dbf
from sp5lib.dbf_writer import append_record, delete_record, get_table_fields, update_record

# ─── helpers: build a minimal valid DBF (same layout as tests/test_dbf.py) ────


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


JOURNAL_SPEC = [
    ("NUMBER", "N", 11, 0),
    ("CHANGEID1", "N", 11, 0),
    ("CHANGEID2", "N", 11, 0),
    ("CHANGEID3", "N", 11, 0),
    ("CHANGE", "N", 1, 0),
]


def _make_table(tmp_path, name, fields_spec, with_journal=True):
    path = tmp_path / f"{name}.DBF"
    path.write_bytes(_make_dbf(fields_spec))
    if with_journal:
        (tmp_path / f"{name}-L.DBF").write_bytes(_make_dbf(JOURNAL_SPEC))
    return str(path)


def _raw_record(path, fields_spec, index=0):
    """Return the raw bytes of record *index* (without the delete flag)."""
    data = open(path, "rb").read()
    header_size = struct.unpack_from("<H", data, 8)[0]
    record_size = struct.unpack_from("<H", data, 10)[0]
    start = header_size + index * record_size
    return data[start + 1 : start + record_size]


# ─── W1: ASCII-class fields stay ASCII ────────────────────────────────────────


def test_ascii_mask_field_written_as_ascii(tmp_path):
    spec = [("ID", "N", 11, 0), ("WORKDAYS", "C", 16, 0)]
    path = _make_table(tmp_path, "5EMPL", spec, with_journal=False)
    append_record(path, get_table_fields(path), {"ID": 1, "WORKDAYS": "1 1 1 1 1 0 0 0"})

    raw = _raw_record(path, spec)
    mask_bytes = raw[11 : 11 + 16]
    assert mask_bytes == b"1 1 1 1 1 0 0 0 "  # plain ASCII + space padding
    assert b"\x00" not in mask_bytes  # no UTF-16, no null terminator

    assert read_dbf(path)[0]["WORKDAYS"] == "1 1 1 1 1 0 0 0"


def test_startend_field_written_as_ascii(tmp_path):
    spec = [("ID", "N", 11, 0), ("STARTEND0", "C", 36, 0)]
    path = _make_table(tmp_path, "5SHIFT", spec, with_journal=False)
    append_record(path, get_table_fields(path), {"ID": 1, "STARTEND0": "06:00-14:00"})

    raw = _raw_record(path, spec)
    assert raw[11 : 11 + 11] == b"06:00-14:00"
    assert read_dbf(path)[0]["STARTEND0"] == "06:00-14:00"


# ─── W2: F fields carry 4 fraction digits ─────────────────────────────────────


def test_float_field_written_with_4_decimals(tmp_path):
    spec = [("ID", "N", 11, 0), ("HRSDAY", "F", 19, 0)]
    path = _make_table(tmp_path, "5EMPL", spec, with_journal=False)
    append_record(path, get_table_fields(path), {"ID": 1, "HRSDAY": 7.7})

    raw = _raw_record(path, spec)
    f_bytes = raw[11 : 11 + 19]
    assert f_bytes == b"7.7000".rjust(19)  # oracle format: right-aligned, 4 decimals
    assert read_dbf(path)[0]["HRSDAY"] == 7.7


# ─── B2: binary fields round-trip unstripped ──────────────────────────────────


def test_binary_digest_roundtrip(tmp_path):
    spec = [("ID", "N", 11, 0), ("DIGEST", "C", 16, 0)]
    path = _make_table(tmp_path, "5USER", spec, with_journal=False)
    # Digest deliberately starts/ends with whitespace-class bytes (0x0c, 0x20)
    digest = bytes(range(0x0C, 0x0C + 16))
    append_record(path, get_table_fields(path), {"ID": 251, "DIGEST": digest})

    rec = read_dbf(path)[0]
    assert rec["DIGEST"] == digest  # unstripped bytes, full 16 bytes


# ─── B1: duplicate START fields (5DADEM) ──────────────────────────────────────


def test_duplicate_start_fields_roundtrip(tmp_path):
    spec = [
        ("ID", "N", 11, 0),
        ("GROUPID", "N", 11, 0),
        ("START", "D", 8, 0),
        ("START", "D", 8, 0),
    ]
    path = _make_table(tmp_path, "5DADEM", spec, with_journal=False)
    append_record(
        path,
        get_table_fields(path),
        {"ID": 1, "GROUPID": 2, "START": "2024-01-01", "START2": "2024-12-31"},
    )

    rec = read_dbf(path)[0]
    assert rec["START"] == "2024-01-01"
    assert rec["START2"] == "2024-12-31"

    update_record(path, get_table_fields(path), 0, {"START2": "2025-06-30"})
    rec = read_dbf(path)[0]
    assert rec["START"] == "2024-01-01"  # untouched
    assert rec["START2"] == "2025-06-30"


# ─── W3: change journal ───────────────────────────────────────────────────────

MASHI_SPEC = [
    ("ID", "N", 11, 0),
    ("EMPLOYEEID", "N", 11, 0),
    ("DATE", "D", 8, 0),
    ("SHIFTID", "N", 11, 0),
]


def test_append_update_delete_write_journal(tmp_path):
    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC)
    jpath = str(tmp_path / "5MASHI-L.DBF")
    fields = get_table_fields(path)

    append_record(path, fields, {"ID": 5, "EMPLOYEEID": 40, "DATE": "2024-01-02", "SHIFTID": 1})
    entries = read_dbf(jpath)
    assert len(entries) == 1
    assert entries[0]["NUMBER"] == 1
    assert entries[0]["CHANGEID1"] == 5  # ID
    assert entries[0]["CHANGEID2"] == 40  # EMPLOYEEID (composite key, Spec D-41)
    assert entries[0]["CHANGEID3"] == 0
    assert entries[0]["CHANGE"] == 1  # upsert

    update_record(path, fields, 0, {"SHIFTID": 2})
    entries = read_dbf(jpath)
    assert [e["NUMBER"] for e in entries] == [1, 2]
    assert entries[1]["CHANGEID1"] == 5 and entries[1]["CHANGEID2"] == 40
    assert entries[1]["CHANGE"] == 1

    delete_record(path, fields, 0)
    entries = read_dbf(jpath)
    assert [e["NUMBER"] for e in entries] == [1, 2, 3]
    assert entries[2]["CHANGEID1"] == 5 and entries[2]["CHANGEID2"] == 40
    assert entries[2]["CHANGE"] == 2  # deleted


def test_journal_not_journaled(tmp_path):
    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC)
    jpath = str(tmp_path / "5MASHI-L.DBF")
    append_record(path, get_table_fields(path), {"ID": 1, "EMPLOYEEID": 2})
    # exactly one journal entry; no 5MASHI-L-L.DBF appeared
    assert len(read_dbf(jpath)) == 1
    assert not (tmp_path / "5MASHI-L-L.DBF").exists()


def test_missing_journal_is_skipped(tmp_path):
    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC, with_journal=False)
    # must not raise despite the missing -L companion
    append_record(path, get_table_fields(path), {"ID": 1, "EMPLOYEEID": 2})
    assert len(read_dbf(path)) == 1


# ─── W4: CDX invalidation ─────────────────────────────────────────────────────


def test_stale_cdx_removed_after_write(tmp_path):
    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC)
    cdx = tmp_path / "5MASHI.CDX"
    jcdx = tmp_path / "5MASHI-L.CDX"
    cdx.write_bytes(b"stale")
    jcdx.write_bytes(b"stale")

    append_record(path, get_table_fields(path), {"ID": 1, "EMPLOYEEID": 2})
    assert not cdx.exists()  # main index invalidated (Spec D-14)
    assert not jcdx.exists()  # journal index invalidated too


# ─── D-19: text fields keep the UTF-16 contract ───────────────────────────────


def test_text_field_utf16_byte_format(tmp_path):
    spec = [("ID", "N", 11, 0), ("NAME", "C", 40, 0)]
    path = _make_table(tmp_path, "5GROUP", spec, with_journal=False)
    append_record(path, get_table_fields(path), {"ID": 1, "NAME": "Müller"})

    raw = _raw_record(path, spec)
    name_bytes = raw[11 : 11 + 40]
    expected = "Müller".encode("utf-16-le") + b"\x00\x00"
    assert name_bytes.startswith(expected)
    assert name_bytes[len(expected) :] == b"\x20" * (40 - len(expected))
    assert read_dbf(path)[0]["NAME"] == "Müller"


# ─── H-1: PACK (Spec 1.14 / D-11 / D-74) ──────────────────────────────────────


def test_pack_table_removes_deleted_records(tmp_path):
    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC)
    fields = get_table_fields(path)
    for i in range(1, 4):
        append_record(path, fields, {"ID": i, "EMPLOYEEID": 40 + i})
    delete_record(path, fields, 1)  # ID=2 als gelöscht markieren

    from sp5lib.dbf_writer import pack_table

    cdx = tmp_path / "5MASHI.CDX"
    cdx.write_bytes(b"stale")

    removed = pack_table(str(path))
    assert removed == 1

    # Physisch entfernt: Header-Count 2, keine 0x2A-Records, EOF-Marker am Ende
    data = open(path, "rb").read()
    assert struct.unpack_from("<I", data, 4)[0] == 2
    header_size = struct.unpack_from("<H", data, 8)[0]
    record_size = struct.unpack_from("<H", data, 10)[0]
    assert len(data) == header_size + 2 * record_size + 1
    assert data[-1] == 0x1A
    assert all(
        data[header_size + i * record_size] != 0x2A for i in range(2)
    )
    assert [r["ID"] for r in read_dbf(str(path))] == [1, 3]

    # D-14: CDX der Haupttabelle invalidiert
    assert not cdx.exists()


def test_pack_table_zaps_journal(tmp_path):
    """Spec D-74: Komprimieren leert das -L-Journal (Zähler-Reset auf 0)."""
    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC)
    jpath = tmp_path / "5MASHI-L.DBF"
    fields = get_table_fields(path)
    append_record(path, fields, {"ID": 1, "EMPLOYEEID": 40})
    delete_record(path, fields, 0)
    assert len(read_dbf(str(jpath))) == 2  # append + delete journalisiert
    jcdx = tmp_path / "5MASHI-L.CDX"
    jcdx.write_bytes(b"stale")

    from sp5lib.dbf_writer import pack_table

    pack_table(str(path))

    jdata = open(jpath, "rb").read()
    assert struct.unpack_from("<I", jdata, 4)[0] == 0  # Zap: 0 Records
    assert read_dbf(str(jpath)) == []
    assert jdata[-1] == 0x1A
    assert not jcdx.exists()

    # Zähler-Reset: nächster Journaleintrag beginnt wieder bei NUMBER=1
    append_record(path, get_table_fields(path), {"ID": 9, "EMPLOYEEID": 41})
    entries = read_dbf(str(jpath))
    assert [e["NUMBER"] for e in entries] == [1]


def test_pack_table_noop_without_deleted_records(tmp_path):
    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC)
    fields = get_table_fields(path)
    append_record(path, fields, {"ID": 1, "EMPLOYEEID": 40})
    before = open(path, "rb").read()

    from sp5lib.dbf_writer import pack_table

    assert pack_table(str(path)) == 0
    assert open(path, "rb").read() == before  # Datei unangetastet


def test_compact_database_packs_all_tables(tmp_path):
    from sp5lib.database import SP5Database
    from sp5lib.dbf_reader import read_dbf as _read

    path = _make_table(tmp_path, "5MASHI", MASHI_SPEC)
    fields = get_table_fields(path)
    for i in range(1, 4):
        append_record(path, fields, {"ID": i, "EMPLOYEEID": 40})
    delete_record(path, fields, 0)
    delete_record(path, fields, 2)

    db = SP5Database(str(tmp_path))
    result = db.compact_database()

    assert result["total_records_removed"] == 2
    detail = next(d for d in result["details"] if d["file"] == "5MASHI.DBF")
    assert detail["removed"] == 2
    assert detail["active"] == 1
    assert [r["ID"] for r in _read(str(path))] == [2]
    # Fassaden-Cache invalidiert: _read liefert den gepackten Stand
    assert len(db._read("MASHI")) == 1
