"""Tests für das sp5lib-CLI (info/dump/validate/sync).

Alle Fixtures sind synthetisch und werden in tmp_path gebaut — keine
Original-Beispieldatenbank nötig.
"""

import csv
import io
import json
import struct
from datetime import date

import pytest

from sp5lib.cli import _target_url, main
from sp5lib.dbf_writer import append_record, get_table_fields

# ─── helpers: synthetische DBF-Tabellen bauen ─────────────────────────────────


def _field_descriptor(name: bytes, ftype: str, length: int, dec: int = 0) -> bytes:
    return name[:11].ljust(11, b"\x00") + ftype.encode("ascii") + b"\x00" * 4 + bytes(
        [length, dec]
    ) + b"\x00" * 14


def _create_table(path, fields_spec, rows=()):
    """Leere .DBF-Datei mit Header anlegen und *rows* via dbf_writer anhängen."""
    record_size = 1 + sum(f[2] for f in fields_spec)
    header_size = 32 + 32 * len(fields_spec) + 1
    hdr = bytearray(32)
    hdr[0] = 0x03
    today = date.today()
    hdr[1], hdr[2], hdr[3] = today.year % 100, today.month, today.day
    struct.pack_into("<H", hdr, 8, header_size)
    struct.pack_into("<H", hdr, 10, record_size)
    descriptors = b"".join(
        _field_descriptor(f[0].encode("ascii"), f[1], f[2], f[3]) for f in fields_spec
    )
    path.write_bytes(bytes(hdr) + descriptors + b"\x0d" + b"\x1a")
    fields = get_table_fields(str(path))
    for row in rows:
        append_record(str(path), fields, row)


@pytest.fixture
def db_dir(tmp_path):
    _create_table(
        tmp_path / "5EMPL.DBF",
        [("ID", "N", 11, 0), ("NAME", "C", 40, 0), ("FIRSTNAME", "C", 40, 0), ("HIDE", "L", 1, 0)],
        [
            {"ID": 1, "NAME": "Müller", "FIRSTNAME": "Anna", "HIDE": False},
            {"ID": 2, "NAME": "Huber", "FIRSTNAME": "Max", "HIDE": False},
        ],
    )
    _create_table(tmp_path / "5GROUP.DBF", [("ID", "N", 11, 0), ("NAME", "C", 40, 0)])
    _create_table(
        tmp_path / "5BUILD.DBF",
        [("ID", "N", 11, 0), ("BUILD", "N", 11, 0)],
        [{"ID": 1, "BUILD": 17}],
    )
    return tmp_path


# ─── info ─────────────────────────────────────────────────────────────────────


def test_info_lists_tables_and_build(db_dir, capsys):
    assert main(["info", str(db_dir)]) == 0
    out = capsys.readouterr().out
    assert "5EMPL.DBF" in out and "5GROUP.DBF" in out
    empl_line = next(line for line in out.splitlines() if "5EMPL" in line)
    assert empl_line.split() == ["5EMPL.DBF", "2", "4"]
    assert "Gesamt: 3 Tabellen, 3 Records" in out
    assert "Schichtplaner5-Build: 17" in out


def test_info_rejects_missing_dir(tmp_path, capsys):
    assert main(["info", str(tmp_path / "nope")]) == 2
    assert "kein Verzeichnis" in capsys.readouterr().err


def test_info_empty_dir(tmp_path, capsys):
    assert main(["info", str(tmp_path)]) == 1
    assert "Keine .DBF-Dateien" in capsys.readouterr().err


# ─── dump ─────────────────────────────────────────────────────────────────────


def test_dump_json_default(db_dir, capsys):
    assert main(["dump", str(db_dir), "5EMPL"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert [r["NAME"] for r in data] == ["Müller", "Huber"]
    assert data[0]["FIRSTNAME"] == "Anna"


def test_dump_limit_and_short_table_name(db_dir, capsys):
    # "EMPL" (ohne 5-Präfix, ohne .DBF) wird zu 5EMPL.DBF aufgelöst
    assert main(["dump", str(db_dir), "EMPL", "--json", "--limit", "1"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1 and data[0]["ID"] == 1


def test_dump_csv(db_dir, capsys):
    assert main(["dump", str(db_dir), "5empl.dbf", "--csv"]) == 0
    rows = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))
    assert len(rows) == 2
    assert rows[0]["NAME"] == "Müller"


def test_dump_csv_empty_table_has_header(db_dir, capsys):
    assert main(["dump", str(db_dir), "5GROUP", "--csv"]) == 0
    out = capsys.readouterr().out
    assert out.splitlines()[0] == "ID,NAME"


def test_dump_unknown_table(db_dir, capsys):
    assert main(["dump", str(db_dir), "5NOPE"]) == 1
    assert "nicht gefunden" in capsys.readouterr().err


# ─── validate ─────────────────────────────────────────────────────────────────


def test_validate_ok(db_dir, capsys):
    assert main(["validate", str(db_dir)]) == 0
    out = capsys.readouterr().out
    assert "OK       5EMPL.DBF (2 Records)" in out
    assert "Alle 3 Tabellen fehlerfrei" in out


def test_validate_truncated_header(db_dir, capsys):
    (db_dir / "5BAD.DBF").write_bytes(b"\x03\x00")
    assert main(["validate", str(db_dir)]) == 1
    out = capsys.readouterr().out
    assert "FEHLER   5BAD.DBF" in out
    assert "1 von 4 Tabellen mit Problemen" in out


def test_validate_bad_field_name_encoding(db_dir, capsys):
    # Feldname mit Nicht-ASCII-Byte → get_table_fields liefert U+FFFD
    hdr = bytearray(32)
    hdr[0] = 0x03
    struct.pack_into("<H", hdr, 8, 32 + 32 + 1)
    struct.pack_into("<H", hdr, 10, 1 + 11)
    bad_descriptor = _field_descriptor(b"NA\xffME", "C", 11)
    (db_dir / "5UGLY.DBF").write_bytes(bytes(hdr) + bad_descriptor + b"\x0d" + b"\x1a")
    assert main(["validate", str(db_dir)]) == 1
    assert "ENCODING 5UGLY.DBF" in capsys.readouterr().out


# ─── sync ─────────────────────────────────────────────────────────────────────


def test_target_url_variants():
    assert _target_url("sqlite:/tmp/x.db") == "sqlite:////tmp/x.db"
    assert _target_url("postgres://u:p@h:5432/db") == "postgresql://u:p@h:5432/db"
    assert _target_url("postgres:postgresql://u@h/db") == "postgresql://u@h/db"
    assert _target_url("postgres:u:p@h/db") == "postgresql://u:p@h/db"
    with pytest.raises(ValueError):
        _target_url("mysql://x")
    with pytest.raises(ValueError):
        _target_url("sqlite:")


def test_sync_to_sqlite(db_dir, tmp_path, capsys):
    target = tmp_path / "sp5.db"
    assert main(["sync", str(db_dir), "--target", f"sqlite:{target}"]) == 0
    out = capsys.readouterr().out
    assert "employees" in out and "2" in out
    assert target.exists()

    from sqlalchemy.orm import Session

    from sp5lib.orm import get_engine
    from sp5lib.orm.models import Employee

    engine = get_engine(f"sqlite:///{target}")
    with Session(engine) as session:
        names = sorted(e.name for e in session.query(Employee).all())
    assert names == ["Huber", "Müller"]


def test_sync_bad_target(db_dir, capsys):
    assert main(["sync", str(db_dir), "--target", "mysql:foo"]) == 2
    assert "Unbekanntes Target" in capsys.readouterr().err
