"""Ersatzsuche / Notfallplan: Eignungslogik für Vertretungen.

Prüft die harten Eignungskriterien (Bereich/Gruppe 5GRASG,
Beschäftigungszeitraum 5EMPL, Verfügbarkeit 5MASHI/5SPSHI/5ABSEN,
Schichtrestriktion 5RESTR) auf der reinen Rechen-Funktion und über die
Fassade. Alle Fixtures sind synthetisch.
"""

import struct
from datetime import date

import pytest

from sp5lib import calculations as calc
from sp5lib.database import SP5Database

# ── reine Funktion ──────────────────────────────────────────────────────────

EMP = calc.EmployeeContext(workdays=(True,) * 8)
NO_HOL: dict = {}
WED = date(2026, 7, 15)  # Mittwoch -> Tagesindex 2


def _elig(**over):
    kw = dict(
        is_hidden=False,
        in_group=True,
        busy_dates=set(),
        absent_dates=set(),
        restrictions=[],
    )
    kw.update(over)
    return calc.is_eligible_replacement(EMP, WED, 3, NO_HOL, **kw)


def test_eligible_when_all_criteria_met():
    assert _elig() == (True, None)


def test_excluded_when_hidden():
    assert _elig(is_hidden=True) == (False, "ausgeblendet")


def test_excluded_when_outside_group():
    assert _elig(in_group=False) == (False, "nicht im Bereich")


def test_excluded_when_already_scheduled():
    assert _elig(busy_dates={WED}) == (False, "bereits eingeteilt")


def test_excluded_when_absent():
    assert _elig(absent_dates={WED}) == (False, "abwesend")


def test_excluded_by_restriction_on_weekday():
    ok, reason = _elig(restrictions=[{"SHIFTID": 3, "WEEKDAY": 2}])
    assert not ok and reason == "Schichtrestriktion (5RESTR)"


def test_restriction_for_other_shift_does_not_block():
    assert _elig(restrictions=[{"SHIFTID": 99, "WEEKDAY": 2}]) == (True, None)


def test_restriction_for_other_weekday_does_not_block():
    assert _elig(restrictions=[{"SHIFTID": 3, "WEEKDAY": 4}]) == (True, None)


def test_excluded_when_outside_employment_period():
    emp = calc.EmployeeContext(workdays=(True,) * 8, emp_start=date(2026, 8, 1))
    ok, reason = calc.is_eligible_replacement(
        emp, WED, 3, NO_HOL,
        is_hidden=False, in_group=True,
        busy_dates=set(), absent_dates=set(), restrictions=[],
    )
    assert not ok and reason == "nicht im Beschäftigungszeitraum"


def test_restriction_on_holiday_uses_holiday_slot():
    holidays = {WED: 0}  # WED ist Feiertag -> Tagesindex 7
    assert calc.is_restricted([{"SHIFTID": 3, "WEEKDAY": 7}], 3, calc.day_index(WED, holidays))


# ── Fassade gegen synthetische DB ───────────────────────────────────────────


def _field_descriptor(name, ftype, length, dec=0):
    nb = name.upper().encode("ascii")[:11].ljust(11, b"\x00")
    return nb + ftype.encode("ascii") + b"\x00" * 4 + bytes([length, dec]) + b"\x00" * 14


def _make_dbf(spec):
    rec_size = 1 + sum(f[2] for f in spec)
    hdr_size = 32 + 32 * len(spec) + 1
    hdr = bytearray(32)
    hdr[0] = 0x03
    today = date.today()
    hdr[1], hdr[2], hdr[3] = today.year % 100, today.month, today.day
    struct.pack_into("<I", hdr, 4, 0)
    struct.pack_into("<H", hdr, 8, hdr_size)
    struct.pack_into("<H", hdr, 10, rec_size)
    return bytes(hdr) + b"".join(_field_descriptor(*f) for f in spec) + b"\x0d" + b"\x1a"


def _write(path, spec, records):
    path.write_bytes(_make_dbf(spec))
    from sp5lib.dbf_writer import append_record, get_table_fields

    fields = get_table_fields(str(path))
    for r in records:
        append_record(str(path), fields, r)


_EMPL = [
    ("ID", "N", 11), ("NAME", "C", 80), ("FIRSTNAME", "C", 80),
    ("SHORTNAME", "C", 16), ("HRSWEEK", "F", 19), ("WORKDAYS", "C", 16),
    ("HIDE", "N", 1), ("EMPSTART", "D", 8), ("EMPEND", "D", 8),
    ("FUNCTION", "C", 40), ("RESERVED", "C", 20),
]
_GRASG = [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("GROUPID", "N", 11), ("RESERVED", "C", 20)]
_MASHI = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("TYPE", "N", 5), ("RESERVED", "C", 20),
]
_SPSHI = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("NAME", "C", 80), ("SHIFTID", "N", 11), ("RESERVED", "C", 20),
]
_ABSEN = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("LEAVETYPID", "N", 11), ("TYPE", "N", 5), ("INTERVAL", "N", 5),
    ("START", "N", 5), ("END", "N", 5), ("RESERVED", "C", 20),
]
_RESTR = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("WEEKDAY", "N", 5),
    ("SHIFTID", "N", 11), ("RESTRICT", "N", 5), ("RESERVED", "C", 20),
]
_SHIFT = [
    ("ID", "N", 11), ("NAME", "C", 80), ("SHORTNAME", "C", 16),
    ("HIDE", "N", 1), ("POSITION", "N", 11), ("RESERVED", "C", 20),
]
_GROUP = [("ID", "N", 11), ("NAME", "C", 80), ("HIDE", "N", 1), ("POSITION", "N", 11), ("RESERVED", "C", 20)]
_HOLID = [("ID", "N", 11), ("DATE", "D", 8), ("NAME", "C", 80), ("INTERVAL", "N", 5), ("RESERVED", "C", 20)]


@pytest.fixture
def db(tmp_path):
    # 5 MA: 1 ausgefallen, 2 geeignet, 1 fremd-Gruppe, 1 abwesend.
    _write(tmp_path / "5EMPL.DBF", _EMPL, [
        {"ID": 1, "NAME": "Ausfall", "SHORTNAME": "AUS", "WORKDAYS": "1 1 1 1 1 1 1 1"},
        {"ID": 2, "NAME": "Geeignet", "FIRSTNAME": "Anna", "SHORTNAME": "GEA", "WORKDAYS": "1 1 1 1 1 1 1 1"},
        {"ID": 3, "NAME": "Geeignet", "FIRSTNAME": "Bert", "SHORTNAME": "GEB", "WORKDAYS": "1 1 1 1 1 1 1 1"},
        {"ID": 4, "NAME": "Fremd", "SHORTNAME": "FRE", "WORKDAYS": "1 1 1 1 1 1 1 1"},
        {"ID": 5, "NAME": "Abwesend", "SHORTNAME": "ABW", "WORKDAYS": "1 1 1 1 1 1 1 1"},
    ])
    _write(tmp_path / "5GROUP.DBF", _GROUP, [
        {"ID": 10, "NAME": "Team A", "HIDE": 0, "POSITION": 1},
        {"ID": 20, "NAME": "Team B", "HIDE": 0, "POSITION": 2},
    ])
    _write(tmp_path / "5GRASG.DBF", _GRASG, [
        {"ID": 1, "EMPLOYEEID": 1, "GROUPID": 10},
        {"ID": 2, "EMPLOYEEID": 2, "GROUPID": 10},
        {"ID": 3, "EMPLOYEEID": 3, "GROUPID": 10},
        {"ID": 4, "EMPLOYEEID": 5, "GROUPID": 10},
        {"ID": 5, "EMPLOYEEID": 4, "GROUPID": 20},  # nur Team B
    ])
    _write(tmp_path / "5SHIFT.DBF", _SHIFT, [
        {"ID": 100, "NAME": "Frühdienst", "SHORTNAME": "F", "HIDE": 0, "POSITION": 1},
    ])
    # MA5 ist am Stichtag abwesend.
    _write(tmp_path / "5ABSEN.DBF", _ABSEN, [
        {"ID": 1, "EMPLOYEEID": 5, "DATE": "2026-07-15", "LEAVETYPID": 1, "INTERVAL": 0},
    ])
    _write(tmp_path / "5MASHI.DBF", _MASHI, [])
    _write(tmp_path / "5SPSHI.DBF", _SPSHI, [])
    _write(tmp_path / "5RESTR.DBF", _RESTR, [])
    _write(tmp_path / "5HOLID.DBF", _HOLID, [])
    return SP5Database(str(tmp_path))


def test_facade_returns_only_group_members_available(db):
    cands = db.eligible_replacements("2026-07-15", 100, absent_employee_id=1)
    ids = {c["id"] for c in cands}
    # MA2, MA3 geeignet; MA4 (Team B) ausgeschlossen; MA5 abwesend; MA1 = Ausfall.
    assert ids == {2, 3}


def test_facade_excludes_already_scheduled(db, tmp_path):
    from sp5lib.dbf_writer import append_record, get_table_fields

    mp = str(tmp_path / "5MASHI.DBF")
    append_record(mp, get_table_fields(mp), {"ID": 1, "EMPLOYEEID": 2, "DATE": "2026-07-15", "SHIFTID": 100})
    db._invalidate_cache("MASHI")
    ids = {c["id"] for c in db.eligible_replacements("2026-07-15", 100, absent_employee_id=1)}
    assert ids == {3}  # MA2 ist nun eingeteilt


def test_facade_excludes_restricted(db, tmp_path):
    from sp5lib.dbf_writer import append_record, get_table_fields

    rp = str(tmp_path / "5RESTR.DBF")
    # MA3 am Mittwoch (Index 2) für Schicht 100 gesperrt.
    append_record(rp, get_table_fields(rp), {"ID": 1, "EMPLOYEEID": 3, "WEEKDAY": 2, "SHIFTID": 100, "RESTRICT": 1})
    db._invalidate_cache("RESTR")
    ids = {c["id"] for c in db.eligible_replacements("2026-07-15", 100, absent_employee_id=1)}
    assert ids == {2}


def test_facade_explicit_group_filter(db):
    # Team B (20) hat nur MA4 -> als Vertretung für Ausfall (Team A) geeignet,
    # wenn man den Bereich explizit auf Team B legt.
    ids = {c["id"] for c in db.eligible_replacements("2026-07-15", 100, absent_employee_id=1, group_id=20)}
    assert ids == {4}


def test_facade_excludes_outside_employment(db, tmp_path):
    # MA3 EMPEND vor Stichtag -> ausgeschlossen.
    _write(tmp_path / "5EMPL.DBF", _EMPL, [
        {"ID": 1, "NAME": "Ausfall", "SHORTNAME": "AUS", "WORKDAYS": "1 1 1 1 1 1 1 1"},
        {"ID": 2, "NAME": "Geeignet", "SHORTNAME": "GEA", "WORKDAYS": "1 1 1 1 1 1 1 1"},
        {"ID": 3, "NAME": "Ausgeschieden", "SHORTNAME": "GEB", "WORKDAYS": "1 1 1 1 1 1 1 1", "EMPEND": "2026-06-30"},
    ])
    db._invalidate_cache("EMPL")
    ids = {c["id"] for c in db.eligible_replacements("2026-07-15", 100, absent_employee_id=1)}
    assert ids == {2}
