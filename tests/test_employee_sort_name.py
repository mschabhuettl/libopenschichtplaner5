"""Mitarbeiter-Reihenfolge: Original-Default ist „Ansicht > Sortierung > Name".

get_employees liefert alphabetisch nach Nachname, dann Vorname — unabhängig
von POSITION (die dem Original-Sortiermodus „Vorgabe" entspricht und als Feld
erhalten bleibt). Alle Fixtures sind synthetisch.
"""

import struct
from datetime import date

from sp5lib.database import SP5Database
from sp5lib.dbf_reader import get_table_fields
from sp5lib.dbf_writer import append_record


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


_EMPL_SPEC = [
    ("ID", "N", 11),
    ("POSITION", "N", 11),
    ("NAME", "C", 200),
    ("FIRSTNAME", "C", 60),
    ("SHORTNAME", "C", 60),
    ("HIDE", "N", 1),
    ("WORKDAYS", "C", 16),
]


def test_get_employees_sorted_by_name_then_firstname(tmp_path):
    path = tmp_path / "5EMPL.DBF"
    path.write_bytes(_make_dbf_bytes(_EMPL_SPEC))
    fields = get_table_fields(str(path))
    # POSITION-Reihenfolge absichtlich NICHT alphabetisch
    rows = [
        (1, 1, "Zimmer", "Udo"),
        (2, 2, "Anders", "Kerstin"),
        (3, 3, "Maier", "Berta"),
        (4, 4, "Maier", "Anna"),
    ]
    for eid, pos, name, first in rows:
        append_record(str(path), fields, {
            "ID": eid, "POSITION": pos, "NAME": name, "FIRSTNAME": first,
            "SHORTNAME": "", "HIDE": 0, "WORKDAYS": "1 1 1 1 1 0 0 0",
        })

    emps = SP5Database(str(tmp_path)).get_employees()
    got = [(e["NAME"], e["FIRSTNAME"]) for e in emps]
    assert got == [
        ("Anders", "Kerstin"),
        ("Maier", "Anna"),      # gleicher Nachname -> Vorname entscheidet
        ("Maier", "Berta"),
        ("Zimmer", "Udo"),
    ]
