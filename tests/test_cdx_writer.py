"""Tests for the OPTIONAL FoxPro .CDX writer (sp5lib.cdx_writer).

The writer is OFF by default; these tests exercise the opt-in path. Coverage:
  * byte-exact reproduction of every sample-DB .CDX (skipped when the
    reference data — local/scratch only — is not present);
  * a self round-trip: build a .CDX, walk its B-tree, and confirm the indexed
    record numbers come out in key order for numeric, composite-character and
    multi-page (interior + multiple leaves) cases;
  * the dbf_writer toggle: default DELETES the stale .CDX, opt-in REBUILDS it.

All non-reference fixtures are synthetic.
"""

import glob
import importlib
import os
import struct
from datetime import date

import pytest

from sp5lib import cdx_writer
from sp5lib.dbf_reader import read_dbf

REF_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "meta-openschichtplaner5",
    "_sp5-reference",
    "daten",
    "Daten",
)
HAVE_REF = os.path.isdir(REF_DIR)


# ─── synthetic DBF builders (mirrors tests/test_writer_parity.py) ──────────────


def _field_descriptor(name, ftype, length, dec=0):
    name_bytes = name.upper().encode("ascii")[:11].ljust(11, b"\x00")
    return name_bytes + ftype.encode("ascii") + b"\x00" * 4 + bytes([length, dec]) + b"\x00" * 14


def _make_dbf_bytes(fields_spec, records):
    n_fields = len(fields_spec)
    record_size = 1 + sum(f[2] for f in fields_spec)
    header_size = 32 + 32 * n_fields + 1
    hdr = bytearray(32)
    hdr[0] = 0x03
    today = date.today()
    hdr[1], hdr[2], hdr[3] = today.year % 100, today.month, today.day
    struct.pack_into("<I", hdr, 4, len(records))
    struct.pack_into("<H", hdr, 8, header_size)
    struct.pack_into("<H", hdr, 10, record_size)
    field_bytes = b"".join(_field_descriptor(*f) for f in fields_spec)
    out = bytearray(bytes(hdr) + field_bytes + b"\x0d")
    for rec in records:
        out += b"\x20"  # active
        for (_name, ftype, length, *_), value in zip(fields_spec, rec, strict=True):
            if ftype == "N":
                out += str(int(value)).rjust(length).encode("ascii")[:length]
            else:
                out += str(value).encode("ascii").ljust(length)[:length]
    out += b"\x1a"
    return bytes(out)


def _make_table(tmp_path, name, fields_spec, records):
    path = tmp_path / f"{name}.DBF"
    path.write_bytes(_make_dbf_bytes(fields_spec, records))
    return str(path)


# ─── a minimal CDX reader, used only to verify round-trips ─────────────────────


def _double_from_numeric_key(b: bytes) -> float:
    b = bytes(b[:8])
    if b[0] & 0x80:
        x = bytes([b[0] & 0x7F]) + b[1:]
    else:
        x = bytes([b[0] ^ 0xFF]) + bytes(c ^ 0xFF for c in b[1:])
    return struct.unpack(">d", x)[0]


def walk_index(cdx_bytes: bytes):
    """Walk the single tag's B-tree leaves left→right, returning [(recno, key)]."""
    d = cdx_bytes
    th = d[0x400:0x600]
    klen = struct.unpack_from("<H", th, 12)[0]
    off = struct.unpack_from("<I", th, 0)[0]

    def is_leaf(pg):
        return struct.unpack_from("<H", pg, 0)[0] & 0x02

    # descend to the leftmost leaf
    while True:
        pg = d[off : off + 512]
        if is_leaf(pg):
            break
        off = struct.unpack_from(">I", pg, 12 + klen + 4)[0]  # first child ptr (BE)

    out = []
    while 0 < off < len(d):
        pg = d[off : off + 512]
        nkeys = struct.unpack_from("<H", pg, 2)[0]
        rmask = struct.unpack_from("<I", pg, 14)[0]
        dmask, tmask = pg[18], pg[19]
        rbits, dbits = pg[20], pg[21]
        eb = pg[23]
        pos = 512
        prev = b""
        for k in range(nkeys):
            v = int.from_bytes(pg[24 + k * eb : 24 + k * eb + eb], "little")
            recno = v & rmask
            dup = (v >> rbits) & dmask
            trail = (v >> (rbits + dbits)) & tmask
            nnew = klen - dup - trail
            pos -= nnew
            prev = prev[:dup] + pg[pos : pos + nnew]
            out.append((recno, prev + b"\x00" * trail))
        right = struct.unpack_from("<i", pg, 8)[0]
        if right < 0:
            break
        off = right
    return out, klen


# ─── byte-exact parity against the real sample DB (skips if absent) ────────────


@pytest.mark.skipif(not HAVE_REF, reason="sample reference DB not present")
@pytest.mark.parametrize("cdx_path", sorted(glob.glob(os.path.join(REF_DIR, "*.CDX"))))
def test_byte_exact_against_sample_db(cdx_path):
    dbf = cdx_path[:-4] + ".DBF"
    if not os.path.exists(dbf):
        pytest.skip("no matching DBF")
    key_expr = cdx_writer._read_key_expr(cdx_path)
    if key_expr is None:
        pytest.skip("no readable key expression")
    tag_name = cdx_writer._read_tag_name(cdx_path)
    counter = cdx_writer._read_counter(cdx_path, 0)
    built = cdx_writer.build_cdx_bytes(dbf, key_expr, counter, tag_name)
    assert built == open(cdx_path, "rb").read(), f"CDX mismatch for {cdx_path}"


# ─── self round-trips on synthetic tables ──────────────────────────────────────


def test_numeric_key_roundtrip(tmp_path):
    records = [(i, f"G{i}") for i in (5, 1, 9, 3, 7)]
    path = _make_table(tmp_path, "T", [("ID", "N", 11), ("NAME", "C", 10)], records)
    built = cdx_writer.build_cdx_bytes(path, "ID", counter=len(records))
    walked, klen = walk_index(built)
    assert klen == 8
    ids = [int(_double_from_numeric_key(k)) for _, k in walked]
    assert ids == sorted(ids) == [1, 3, 5, 7, 9]
    # recno is the 1-based physical position; verify it points at the right row
    for recno, key in walked:
        assert records[recno - 1][0] == int(_double_from_numeric_key(key))


def test_composite_character_key_roundtrip(tmp_path):
    records = [(2, 41), (1, 40), (2, 39), (1, 44), (3, 10)]
    path = _make_table(tmp_path, "T", [("ID", "N", 11), ("EMPLOYEEID", "N", 11)], records)
    expr = "STR(ID,11)+STR(EMPLOYEEID,11)"
    built = cdx_writer.build_cdx_bytes(path, expr, counter=len(records))
    walked, klen = walk_index(built)
    assert klen == 22
    keys = [k[:22] for _, k in walked]
    assert keys == sorted(keys)  # character keys sort lexicographically
    # each key decodes back to its STR(ID,11)+STR(EMP,11)
    for recno, key in walked:
        rid, emp = records[recno - 1]
        assert key[:22] == (str(rid).rjust(11) + str(emp).rjust(11)).encode("ascii")


def test_multipage_btree_roundtrip(tmp_path):
    # 200 numeric keys force a split into multiple leaf pages + an interior root.
    records = [(i, f"N{i}") for i in range(1, 201)]
    path = _make_table(tmp_path, "T", [("ID", "N", 11), ("NAME", "C", 10)], records)
    built = cdx_writer.build_cdx_bytes(path, "ID", counter=len(records))
    # more than one leaf page -> file grew beyond the 6-page single-leaf layout
    assert len(built) > 6 * 512
    walked, _ = walk_index(built)
    ids = [int(_double_from_numeric_key(k)) for _, k in walked]
    assert ids == list(range(1, 201))  # every key, in order, across all leaves


def test_deleted_records_excluded(tmp_path):
    path = _make_table(tmp_path, "T", [("ID", "N", 11)], [(1,), (2,), (3,)])
    # flag record index 1 (ID=2) as deleted
    data = bytearray(open(path, "rb").read())
    header_size = struct.unpack_from("<H", data, 8)[0]
    record_size = struct.unpack_from("<H", data, 10)[0]
    data[header_size + 1 * record_size] = 0x2A
    open(path, "wb").write(data)
    built = cdx_writer.build_cdx_bytes(path, "ID", counter=2)
    walked, _ = walk_index(built)
    ids = [int(_double_from_numeric_key(k)) for _, k in walked]
    assert ids == [1, 3]  # the .NOT. DELETED() filter dropped ID=2


# ─── the dbf_writer toggle ─────────────────────────────────────────────────────


def _make_indexed_table(tmp_path, monkeypatch):
    """A synthetic table + a real CDX built by our own writer (so it is valid)."""
    path = _make_table(tmp_path, "T", [("ID", "N", 11)], [(1,), (2,), (3,)])
    cdx_path = path[:-4] + ".CDX"
    built = cdx_writer.build_cdx_bytes(path, "ID", counter=3)
    open(cdx_path, "wb").write(built)
    return path, cdx_path


def test_default_invalidates_cdx(tmp_path, monkeypatch):
    monkeypatch.delenv("SP5_CDX_WRITE", raising=False)
    import sp5lib.dbf_writer as w

    importlib.reload(w)
    assert w.WRITE_CDX is False
    path, cdx_path = _make_indexed_table(tmp_path, monkeypatch)
    w.append_record(path, w.get_table_fields(path), {"ID": 4})
    assert not os.path.exists(cdx_path)  # default: stale CDX deleted


def test_optin_rebuilds_cdx(tmp_path, monkeypatch):
    monkeypatch.setenv("SP5_CDX_WRITE", "1")
    import sp5lib.dbf_writer as w

    importlib.reload(w)
    assert w.WRITE_CDX is True
    try:
        path, cdx_path = _make_indexed_table(tmp_path, monkeypatch)
        w.append_record(path, w.get_table_fields(path), {"ID": 4})
        assert os.path.exists(cdx_path)  # opt-in: CDX rebuilt, not deleted
        built = open(cdx_path, "rb").read()
        walked, _ = walk_index(built)
        ids = [int(_double_from_numeric_key(k)) for _, k in walked]
        assert ids == [1, 2, 3, 4]  # new record indexed in order
        assert len(read_dbf(path)) == 4
    finally:
        monkeypatch.delenv("SP5_CDX_WRITE", raising=False)
        importlib.reload(w)  # restore default for the rest of the session


def test_optin_falls_back_when_no_cdx(tmp_path, monkeypatch):
    # Opt-in but the table has no .CDX at all -> writer returns None, nothing to do.
    monkeypatch.setenv("SP5_CDX_WRITE", "1")
    import sp5lib.dbf_writer as w

    importlib.reload(w)
    try:
        path = _make_table(tmp_path, "T", [("ID", "N", 11)], [(1,)])
        # no CDX written; append must still succeed and create no CDX
        w.append_record(path, w.get_table_fields(path), {"ID": 2})
        assert not os.path.exists(path[:-4] + ".CDX")
        assert len(read_dbf(path)) == 2
    finally:
        monkeypatch.delenv("SP5_CDX_WRITE", raising=False)
        importlib.reload(w)


def test_unsupported_key_expr_raises():
    with pytest.raises(ValueError):
        cdx_writer._parse_key_expr("UPPER(NAME)")
