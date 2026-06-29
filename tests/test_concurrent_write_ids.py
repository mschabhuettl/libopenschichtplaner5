"""Nebenläufige Schreibvorgänge dürfen keine doppelten IDs vergeben und keinen
fremden Datensatz verändern (P0-1).

Hintergrund: ``add_schedule_entry`` & Co. vergaben die ID als ``max(ID)+1`` aus
einem Lesevorgang, der NICHT unter demselben Lock lief wie das anschließende
Append. Unter Last (FastAPI bedient synchrone Endpunkte aus einem Threadpool)
lasen mehrere Schreiber denselben ``max`` und schrieben dieselbe ID. Zwei Sätze
mit gleicher ID ließen ``find_all_records(ID=…)`` mehrere Treffer liefern, sodass
ein ID-adressiertes Update/Delete den falschen (fremden) Satz traf — die vom
Maintainer beobachteten „unter Last vertauschten Schichten".

Der Fix vergibt die ID atomar INNERHALB des exklusiven Append-Locks
(``append_record(..., autoid_field="ID")``). Diese Tests decken das nebenläufige
Szenario ab und prüfen zusätzlich, dass kein bestehender Satz verändert wird.
"""

import struct
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier

import pytest

from sp5lib.database import SP5Database
from sp5lib.dbf_reader import read_dbf
from sp5lib.dbf_writer import append_record, get_table_fields


def _field_descriptor(name, ftype, length, dec=0):
    nb = name.upper().encode("ascii")[:11].ljust(11, b"\x00")
    return nb + ftype.encode("ascii") + b"\x00" * 4 + bytes([length, dec]) + b"\x00" * 14


def _make_dbf_bytes(fields_spec):
    n = len(fields_spec)
    rec = 1 + sum(f[2] for f in fields_spec)
    hs = 32 + 32 * n + 1
    hdr = bytearray(32)
    hdr[0] = 0x03
    t = date.today()
    hdr[1], hdr[2], hdr[3] = t.year % 100, t.month, t.day
    struct.pack_into("<I", hdr, 4, 0)
    struct.pack_into("<H", hdr, 8, hs)
    struct.pack_into("<H", hdr, 10, rec)
    return bytes(hdr) + b"".join(_field_descriptor(*f) for f in fields_spec) + b"\x0d" + b"\x1a"


_MASHI = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("TYPE", "N", 5),
    ("RESERVED", "C", 20),
]
_JOURNAL = [
    ("NUMBER", "N", 11, 0), ("CHANGEID1", "N", 11, 0), ("CHANGEID2", "N", 11, 0),
    ("CHANGEID3", "N", 11, 0), ("CHANGE", "N", 1, 0),
]


@pytest.fixture
def mashi_db(tmp_path):
    """5MASHI mit Journal + 2000 vorab eingetragenen Diensten (realistische Größe)."""
    (tmp_path / "5MASHI.DBF").write_bytes(_make_dbf_bytes(_MASHI))
    (tmp_path / "5MASHI-L.DBF").write_bytes(_make_dbf_bytes(_JOURNAL))
    fp = str(tmp_path / "5MASHI.DBF")
    fields = get_table_fields(fp)
    for i in range(1, 2001):
        append_record(fp, fields, {
            "ID": i, "EMPLOYEEID": (i % 60) + 1,
            "DATE": f"2026-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}",
            "SHIFTID": (i % 8) + 1, "WORKPLACID": 0, "TYPE": 0, "RESERVED": "",
        })
    db = SP5Database(str(tmp_path))
    db._invalidate_cache("MASHI")
    return db, fp


def test_concurrent_add_schedule_entries_get_distinct_ids(mashi_db):
    """24 gleichzeitige add_schedule_entry → 24 verschiedene IDs, keine Dublette."""
    db, fp = mashi_db
    writers = 24
    barrier = Barrier(writers)
    ids: list[int] = []

    def writer(k):
        barrier.wait()  # alle gleichzeitig starten → maximale Kollisionschance
        rec = db.add_schedule_entry(
            employee_id=1000 + k, date_str="2026-07-15", shift_id=3, schedule_type=0
        )
        ids.append(rec["ID"])

    with ThreadPoolExecutor(max_workers=writers) as ex:
        list(ex.map(writer, range(writers)))

    # Zurückgegebene IDs eindeutig …
    assert len(set(ids)) == writers, f"doppelte zurückgegebene IDs: {sorted(ids)}"
    # … und auch in der DBF eindeutig (kein verlorenes/überschriebenes Append).
    db._invalidate_cache("MASHI")
    new_rows = [r for r in read_dbf(fp) if (r.get("EMPLOYEEID") or 0) >= 1000]
    new_ids = [r["ID"] for r in new_rows]
    assert len(new_rows) == writers, "nicht jeder nebenläufige Schreibvorgang persistiert"
    assert len(set(new_ids)) == writers, f"doppelte IDs in der DBF: {sorted(new_ids)}"


def test_concurrent_writes_do_not_alter_existing_records(mashi_db):
    """Round-Trip/Byte-Parität: nach nebenläufigen Appends sind ALLE vorbestehenden
    Sätze (ID 1..2000) unverändert — kein Schreibvorgang hat einen fremden Satz
    getroffen."""
    db, fp = mashi_db
    before = {r["ID"]: r for r in read_dbf(fp) if r["ID"] <= 2000}

    barrier = Barrier(16)

    def writer(k):
        barrier.wait()
        db.add_schedule_entry(
            employee_id=2000 + k, date_str="2026-08-01", shift_id=5, schedule_type=0
        )

    with ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(writer, range(16)))

    db._invalidate_cache("MASHI")
    after = {r["ID"]: r for r in read_dbf(fp) if r["ID"] <= 2000}
    assert after == before, "ein vorbestehender Satz wurde durch ein Append verändert"


def test_append_record_autoid_is_atomic(tmp_path):
    """Primitive-Ebene: 32 gleichzeitige append_record(autoid_field='ID') auf eine
    Datei vergeben 32 paarweise verschiedene IDs."""
    (tmp_path / "T.DBF").write_bytes(_make_dbf_bytes(_MASHI))
    fp = str(tmp_path / "T.DBF")
    fields = get_table_fields(fp)
    n = 32
    barrier = Barrier(n)
    assigned: list[int] = []

    def writer(_):
        rec = {"ID": 0, "EMPLOYEEID": 7, "DATE": "2026-01-01",
               "SHIFTID": 1, "WORKPLACID": 0, "TYPE": 0, "RESERVED": ""}
        barrier.wait()
        append_record(fp, fields, rec, autoid_field="ID")
        assigned.append(rec["ID"])

    with ThreadPoolExecutor(max_workers=n) as ex:
        list(ex.map(writer, range(n)))

    assert sorted(assigned) == list(range(1, n + 1)), f"IDs nicht lückenlos/eindeutig: {sorted(assigned)}"
    file_ids = sorted(r["ID"] for r in read_dbf(fp))
    assert file_ids == list(range(1, n + 1)), f"DBF-IDs falsch: {file_ids}"
