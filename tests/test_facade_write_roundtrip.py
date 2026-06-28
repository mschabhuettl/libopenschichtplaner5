"""Fassaden-Schreibwege end-to-end: schreiben → zurücklesen → Wert identisch.

Ergänzt die Byte-Parität aus test_writer_parity.py (dbf_writer-Ebene) um die
High-Level-Schreibmethoden von SP5Database für die Bewegungs- und
Stammdaten-Tabellen. Geprüft wird je Schreibweg:

  * der Round-Trip (geschriebener Wert == zurückgelesener Wert),
  * das Mitwachsen des -L-Journals (eine Zeile je Schreibvorgang),
  * die CDX-Invalidierung (Index der Tabelle nach dem Schreiben gelöscht),
  * F-Format-Felder (5BOOK.VALUE, 5LEAEN.ENTITLEMNT) und UTF-16-Textfelder
    (5NOTE.TEXT1 inkl. Umlaute) bleiben über den Round-Trip erhalten.

Alle Fixtures sind synthetisch — kein Originaldatenbestand involviert.
"""

import struct
from datetime import date

import pytest

from sp5lib.database import SP5Database
from sp5lib.dbf_reader import read_dbf

# ── Minimal-DBF-Bau (Layout wie tests/test_dbf.py / test_writer_parity.py) ──


def _field_descriptor(name, ftype, length, dec=0):
    name_bytes = name.upper().encode("ascii")[:11].ljust(11, b"\x00")
    return name_bytes + ftype.encode("ascii") + b"\x00" * 4 + bytes([length, dec]) + b"\x00" * 14


def _make_dbf_bytes(fields_spec):
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


_JOURNAL_SPEC = [
    ("NUMBER", "N", 11, 0),
    ("CHANGEID1", "N", 11, 0),
    ("CHANGEID2", "N", 11, 0),
    ("CHANGEID3", "N", 11, 0),
    ("CHANGE", "N", 1, 0),
]


# Schema-Auszüge der Beispiel-DB (dbf_schema_dump): nur die Felder, die die
# jeweilige Schreibmethode befüllt.
_SCHEMAS = {
    "5MASHI": [
        ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
        ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("TYPE", "N", 5),
        ("RESERVED", "C", 20),
    ],
    "5ABSEN": [
        ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
        ("LEAVETYPID", "N", 11), ("TYPE", "N", 5), ("INTERVAL", "N", 5),
        ("START", "N", 5), ("END", "N", 5), ("RESERVED", "C", 20),
    ],
    "5NOTE": [
        ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
        ("TEXT1", "C", 252), ("TEXT2", "C", 252), ("RESERVED", "C", 20),
    ],
    "5BOOK": [
        ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
        ("TYPE", "N", 5), ("VALUE", "F", 19), ("NOTE", "C", 200),
        ("RESERVED", "C", 20),
    ],
    "5LEAEN": [
        ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("YEAR", "N", 11),
        ("LEAVETYPID", "N", 11), ("ENTITLEMNT", "F", 19), ("REST", "F", 19),
        ("INDAYS", "N", 1), ("RESERVED", "C", 20),
    ],
    "5PERIO": [
        ("ID", "N", 11), ("GROUPID", "N", 11), ("START", "D", 8),
        ("END", "D", 8), ("COLOR", "N", 11), ("DESCRIPT", "C", 200),
        ("RESERVED", "C", 20),
    ],
}


@pytest.fixture
def db(tmp_path):
    """Synthetische SP5-DB mit den fünf bewegungsnahen Tabellen + Journalen."""
    for name, spec in _SCHEMAS.items():
        (tmp_path / f"{name}.DBF").write_bytes(_make_dbf_bytes(spec))
        (tmp_path / f"{name}-L.DBF").write_bytes(_make_dbf_bytes(_JOURNAL_SPEC))
        # Stale-Index anlegen, damit die Invalidierung etwas zu löschen hat.
        (tmp_path / f"{name}.CDX").write_bytes(b"\x00" * 512)
    return SP5Database(str(tmp_path))


def _jcount(db, table):
    import os
    p = os.path.join(db.db_path, f"{table}-L.DBF")
    return len(read_dbf(p))


def _cdx_gone(db, table):
    import os
    return not os.path.exists(os.path.join(db.db_path, f"{table}.CDX"))


# ── 5MASHI (Dienste) ────────────────────────────────────────────────────────


def test_mashi_roundtrip_journal_cdx(db):
    db.add_schedule_entry(employee_id=40, date_str="2026-07-15", shift_id=3)
    rows = db._read("MASHI")
    assert len(rows) == 1
    r = rows[0]
    assert r["EMPLOYEEID"] == 40 and r["DATE"] == "2026-07-15" and r["SHIFTID"] == 3
    assert _jcount(db, "5MASHI") == 1
    assert _cdx_gone(db, "5MASHI")

    n = db.delete_schedule_entry(40, "2026-07-15")
    assert n == 1
    assert _jcount(db, "5MASHI") == 2  # zweiter Eintrag (CHANGE=2)


# ── 5ABSEN (Abwesenheiten, inkl. stundenweise) ──────────────────────────────


def test_absence_fullday_roundtrip(db):
    db.add_absence(40, "2026-07-17", leave_type_id=2, interval=0)
    r = db._read("ABSEN")[0]
    assert r["LEAVETYPID"] == 2 and r["INTERVAL"] == 0
    assert r["START"] == 0 and r["END"] == 0
    assert _jcount(db, "5ABSEN") == 1


def test_absence_hourly_start_end_roundtrip(db):
    db.add_absence(40, "2026-07-18", leave_type_id=2, interval=3, start=480, end=720)
    r = db._read("ABSEN")[0]
    assert r["INTERVAL"] == 3 and r["START"] == 480 and r["END"] == 720


# ── 5NOTE (Notizen, UTF-16-Text mit Umlauten) ───────────────────────────────


def test_note_utf16_text_roundtrip(db):
    text = "Notiz mit Umlauten: äöü ß €"
    rec = db.add_note("2026-07-19", text, employee_id=40)
    r = db._read("NOTE")[0]
    assert r["ID"] == rec["id"]
    assert r["TEXT1"] == text  # UTF-16-LE über den Round-Trip erhalten
    assert _jcount(db, "5NOTE") == 1

    db.update_note(rec["id"], text1="Geänderte Notiz")
    assert db._read("NOTE")[0]["TEXT1"] == "Geänderte Notiz"


# ── 5BOOK (Kontobuchung, F-Format-Wert) ─────────────────────────────────────


def test_booking_f_format_value_roundtrip(db):
    rec = db.create_booking(40, "2026-07-20", booking_type=0, value=7.5, note="Test")
    r = db._read("BOOK")[0]
    assert r["ID"] == rec["id"]
    assert abs(r["VALUE"] - 7.5) < 1e-6
    assert r["NOTE"] == "Test"
    assert _jcount(db, "5BOOK") == 1

    assert db.delete_booking(rec["id"]) == 1


def test_booking_negative_f_value_roundtrip(db):
    db.create_booking(40, "2026-07-21", booking_type=1, value=-3.25, note="")
    assert abs(db._read("BOOK")[0]["VALUE"] - (-3.25)) < 1e-6


def test_update_booking_roundtrip(db):
    # P-VOLLERFASSUNG MitarbeiterErfassen.41: Buchung bearbeiten (statt nur
    # Anlegen+Löschen). update_booking ändert nur die übergebenen Felder.
    rec = db.create_booking(40, "2026-07-22", booking_type=0, value=5.0, note="alt")
    out = db.update_booking(rec["id"], value=8.5, note="neu")
    assert out is not None
    r = next(x for x in db._read("BOOK") if x["ID"] == rec["id"])
    assert abs(r["VALUE"] - 8.5) < 1e-6
    assert r["NOTE"] == "neu"
    # Datum/Typ unverändert (nicht mitgegeben)
    assert r["TYPE"] == 0
    # Teil-Update nur des Datums lässt den (geänderten) Wert stehen
    db.update_booking(rec["id"], date_str="2026-08-01")
    r2 = next(x for x in db._read("BOOK") if x["ID"] == rec["id"])
    assert str(r2["DATE"]) == "2026-08-01"
    assert abs(r2["VALUE"] - 8.5) < 1e-6


def test_update_booking_unknown_id_returns_none(db):
    assert db.update_booking(999999, value=1.0) is None


# ── 5PERIO (gekennzeichnete Zeiträume, Bearbeiten) ──────────────────────────


def test_update_period_roundtrip(db):
    # P-VOLLERFASSUNG GruppenErfassen.20: gekennzeichneten Zeitraum bearbeiten
    # (statt nur Anlegen+Löschen). update_period ändert nur die übergebenen Felder.
    rec = db.create_period({
        "group_id": 2, "start": "2026-07-01", "end": "2026-07-31",
        "color": 255, "description": "Sommer",
    })
    out = db.update_period(rec["id"], {"end": "2026-08-15", "description": "Sommer+"})
    assert out is not None
    r = next(x for x in db._read("PERIO") if x["ID"] == rec["id"])
    assert str(r["END"]) == "2026-08-15"
    assert r["DESCRIPT"] == "Sommer+"
    # Start/Gruppe/Farbe unverändert (nicht mitgegeben)
    assert str(r["START"]) == "2026-07-01"
    assert r["GROUPID"] == 2
    assert r["COLOR"] == 255


def test_update_period_unknown_id_returns_none(db):
    assert db.update_period(999999, {"description": "x"}) is None


# ── 5LEAEN (Urlaubsanspruch, F-Format) ──────────────────────────────────────


def test_leave_entitlement_f_format_roundtrip(db):
    db.set_leave_entitlement(40, 2026, days=30.0, leave_type_id=2)
    rows = db._read("LEAEN")
    assert len(rows) == 1
    r = rows[0]
    assert r["EMPLOYEEID"] == 40 and r["YEAR"] == 2026
    assert abs(r["ENTITLEMNT"] - 30.0) < 1e-6
    assert _jcount(db, "5LEAEN") == 1

    # Upsert: erneutes Setzen ersetzt den Satz, aktiv bleibt genau einer.
    db.set_leave_entitlement(40, 2026, days=25.5, leave_type_id=2)
    rows = db._read("LEAEN")
    assert len(rows) == 1
    assert abs(rows[0]["ENTITLEMNT"] - 25.5) < 1e-6
