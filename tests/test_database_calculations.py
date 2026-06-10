"""Fassaden-Tests: SP5Database ist auf sp5lib.calculations verdrahtet.

Prüft die in docs/parity-lib-calc.md gemeldeten Abweichungen über die
SP5Database-Fassade gegen eine synthetische Mini-DB (tmp_path). Anker ist der
normative Handbuch-Fall (Spec 3.3.3 Nr. 8): Dezember 2014 = 157,85 Sollstunden.
"""

import struct
from datetime import date

import pytest

from sp5lib.database import SP5Database
from sp5lib.dbf_writer import append_record, get_table_fields

# ─── DBF-Fixture-Helfer (Muster aus test_writer_parity.py) ────────────────────


def _field_descriptor(name, ftype, length, dec=0):
    name_bytes = name.upper().encode("ascii")[:11].ljust(11, b"\x00")
    return (
        name_bytes + ftype.encode("ascii") + b"\x00" * 4 + bytes([length, dec]) + b"\x00" * 14
    )


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


SPECS = {
    "5EMPL": [
        ("ID", "N", 11), ("NAME", "C", 40), ("FIRSTNAME", "C", 40),
        ("SHORTNAME", "C", 12), ("NUMBER", "C", 10), ("POSITION", "N", 11),
        ("HIDE", "N", 1), ("WORKDAYS", "C", 16), ("CALCBASE", "N", 11),
        ("HRSDAY", "F", 10), ("HRSWEEK", "F", 10), ("HRSMONTH", "F", 10),
        ("HRSTOTAL", "F", 10), ("DEDUCTHOL", "N", 1),
        ("EMPSTART", "D", 8), ("EMPEND", "D", 8),
    ],
    "5HOLID": [("ID", "N", 11), ("DATE", "D", 8), ("NAME", "C", 40), ("INTERVAL", "N", 11)],
    "5SHIFT": [("ID", "N", 11), ("NAME", "C", 40), ("SHORTNAME", "C", 12),
               ("POSITION", "N", 11), ("HIDE", "N", 1), ("NOEXTRA", "N", 1)]
              + [(f"STARTEND{i}", "C", 36) for i in range(8)]
              + [(f"DURATION{i}", "F", 10) for i in range(8)],
    "5MASHI": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("SHIFTID", "N", 11),
               ("DATE", "D", 8), ("WORKPLACID", "N", 11)],
    "5SPSHI": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("SHIFTID", "N", 11),
               ("DATE", "D", 8), ("NAME", "C", 40), ("SHORTNAME", "C", 12),
               ("STARTEND", "C", 36), ("DURATION", "F", 10), ("NOEXTRA", "N", 1),
               ("TYPE", "N", 11), ("WORKPLACID", "N", 11)],
    "5ABSEN": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
               ("LEAVETYPID", "N", 11), ("TYPE", "N", 11), ("INTERVAL", "N", 11),
               ("START", "N", 11), ("END", "N", 11)],
    "5BOOK": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
              ("TYPE", "N", 11), ("VALUE", "F", 12), ("NOTE", "C", 80)],
    "5OVER": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
              ("HOURS", "F", 10)],
    "5LEAVT": [("ID", "N", 11), ("NAME", "C", 40), ("SHORTNAME", "C", 12),
               ("POSITION", "N", 11), ("HIDE", "N", 1), ("CHARGETYP", "N", 11),
               ("CHARGEHRS", "F", 10), ("DEDUCTACT", "N", 1), ("DEDUCTOVT", "N", 1),
               ("ENTITLED", "N", 1), ("STDENTIT", "F", 10), ("CARRYFWD", "N", 1),
               ("COUNTALL", "N", 1)],
    "5LEAEN": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("YEAR", "N", 11),
               ("LEAVETYPID", "N", 11), ("ENTITLEMNT", "F", 10), ("REST", "F", 10),
               ("INDAYS", "N", 1)],
    "5XCHAR": [("ID", "N", 11), ("NAME", "C", 40), ("POSITION", "N", 11),
               ("START", "N", 11), ("END", "N", 11), ("VALIDITY", "N", 11),
               ("VALIDDAYS", "C", 16), ("HOLRULE", "N", 11), ("DATE", "D", 8),
               ("HIDE", "N", 1)],
    "5CYCLE": [("ID", "N", 11), ("NAME", "C", 40), ("POSITION", "N", 11),
               ("SIZE", "N", 11), ("UNIT", "N", 11), ("HIDE", "N", 1)],
    "5CYENT": [("ID", "N", 11), ("CYCLEEID", "N", 11), ("INDEX", "N", 11),
               ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11)],
    "5CYASS": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("CYCLEID", "N", 11),
               ("START", "D", 8), ("END", "D", 8), ("ENTRANCE", "N", 11)],
}


def make_db(tmp_path, rows_by_table):
    """Erzeuge eine Mini-DB; fehlende Tabellen liest die Fassade als leer."""
    for table, rows in rows_by_table.items():
        spec = [(n, t, ln, 0) for n, t, ln in SPECS[table]]
        path = tmp_path / f"{table}.DBF"
        path.write_bytes(_make_dbf(spec))
        fields = get_table_fields(str(path))
        for row in rows:
            append_record(str(path), fields, row)
    return SP5Database(str(tmp_path))


# Normativer MA: Mo-Fr, CALCBASE=1 (Woche), 7,7/38,5 h, DEDUCTHOL=1
EMP_WEEK = {
    "ID": 1, "NAME": "Muster", "FIRSTNAME": "Max", "SHORTNAME": "MM",
    "POSITION": 1, "HIDE": 0, "WORKDAYS": "1 1 1 1 1 0 0 0", "CALCBASE": 1,
    "HRSDAY": 7.7, "HRSWEEK": 38.5, "HRSMONTH": 0.0, "HRSTOTAL": 0.0,
    "DEDUCTHOL": 1,
}
HOLIDAYS_2014 = [
    {"ID": 1, "DATE": "2014-12-25", "NAME": "1. Weihnachtstag", "INTERVAL": 0},
    {"ID": 2, "DATE": "2014-12-26", "NAME": "2. Weihnachtstag", "INTERVAL": 0},
    {"ID": 3, "DATE": "2014-12-31", "NAME": "Silvester", "INTERVAL": 1},
]
# Tagschicht Mo-Fr 06:00-14:00, 8 h; kein Sa/So/Ft-Fenster
DAY_SHIFT = {"ID": 1, "NAME": "Tagschicht", "SHORTNAME": "T", "POSITION": 1, "HIDE": 0,
             "NOEXTRA": 0}
for _i in range(5):
    DAY_SHIFT[f"STARTEND{_i}"] = "06:00-14:00"
    DAY_SHIFT[f"DURATION{_i}"] = 8.0
URLAUB = {"ID": 1, "NAME": "Urlaub", "SHORTNAME": "U", "POSITION": 1, "HIDE": 0,
          "CHARGETYP": 1, "CHARGEHRS": 0.0, "DEDUCTACT": 0, "DEDUCTOVT": 0,
          "ENTITLED": 1, "STDENTIT": 30.0, "CARRYFWD": 1, "COUNTALL": 1}
SONDERURLAUB = {"ID": 14, "NAME": "Sonderurlaub", "SHORTNAME": "SU", "POSITION": 2,
                "HIDE": 0, "CHARGETYP": 1, "CHARGEHRS": 0.0, "DEDUCTACT": 0,
                "DEDUCTOVT": 0, "ENTITLED": 1, "STDENTIT": 2.0, "CARRYFWD": 0,
                "COUNTALL": 1}


def test_normative_december_2014_via_facade(tmp_path):
    """Spec 3.3.3 Nr. 8: Soll Dez. 2014 = 157,85 h (Befund 1)."""
    db = make_db(tmp_path, {"5EMPL": [EMP_WEEK], "5HOLID": HOLIDAYS_2014})
    stats = db.get_statistics(2014, 12)
    assert stats[0]["target_hours"] == pytest.approx(157.85)
    balance = db.calculate_time_balance(1, 2014)
    assert balance["months"][11]["target_hours"] == pytest.approx(157.85)
    year_row = db.get_employee_stats_year(1, 2014)["months"][11]
    assert year_row["target_hours"] == pytest.approx(157.85)
    schedule_row = db.get_schedule_year(2014, 1)[11]
    assert schedule_row["target_hours"] == pytest.approx(157.85)


def test_shift_hours_use_day_index(tmp_path):
    """Befund 2: Mo-Fr-Schicht am Samstag bzw. Feiertag zählt 0 Stunden."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5HOLID": HOLIDAYS_2014,
        "5SHIFT": [DAY_SHIFT],
        "5MASHI": [
            {"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-06"},  # Sa
            {"ID": 2, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-25"},  # Ft
            {"ID": 3, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01"},  # Mo
        ],
    })
    stats = db.get_statistics(2014, 12)
    assert stats[0]["actual_hours"] == pytest.approx(8.0)  # nur der Montag


def test_special_shift_replaces_duty(tmp_path):
    """Befund Spec 3.4.4 Nr. 12: Arbeitszeitabweichung ersetzt den Dienst (6,0 statt 14,0)."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5SHIFT": [DAY_SHIFT],
        "5MASHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01"}],
        "5SPSHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01",
                    "DURATION": 6.0, "NOEXTRA": 0, "TYPE": 0}],
    })
    stats = db.get_statistics(2014, 12)
    assert stats[0]["actual_hours"] == pytest.approx(6.0)


def test_absence_charging_and_no_suppression(tmp_path):
    """Befund 3: Schicht 8 h + Krankheit (CHARGETYP=1) am selben Tag = 15,7 h."""
    krank = dict(URLAUB, ID=3, NAME="Krankheit", SHORTNAME="K", ENTITLED=0,
                 CARRYFWD=0, STDENTIT=0.0)
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5SHIFT": [DAY_SHIFT],
        "5LEAVT": [krank],
        "5MASHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-03"}],
        "5ABSEN": [{"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-03",
                    "LEAVETYPID": 3, "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0}],
    })
    stats = db.get_statistics(2014, 12)
    assert stats[0]["actual_hours"] == pytest.approx(8.0 + 7.7)


def test_booking_types_separated(tmp_path):
    """Befund 4 (Repro): Ist +10 und Soll +10 ⇒ Saldo-Wirkung 0."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5HOLID": HOLIDAYS_2014,
        "5BOOK": [
            {"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-15", "TYPE": 0,
             "VALUE": 10.0, "NOTE": ""},
            {"ID": 2, "EMPLOYEEID": 1, "DATE": "2014-12-15", "TYPE": 1,
             "VALUE": 10.0, "NOTE": ""},
        ],
    })
    months = db.calculate_time_balance(1, 2014)["months"]
    dec = months[11]
    assert dec["actual_hours"] == pytest.approx(10.0)
    assert dec["target_hours"] == pytest.approx(157.85 + 10.0)
    assert dec["saldo"] == pytest.approx(10.0 - (157.85 + 10.0))


def test_over_flows_into_overtime_account_not_saldo(tmp_path):
    """Befund 5: 5OVER berührt den Saldo nicht, nur das Überstundenkonto."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5OVER": [{"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-10", "HOURS": 5.0}],
        "5BOOK": [{"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-11", "TYPE": 2,
                   "VALUE": 2.0, "NOTE": ""}],
    })
    balance = db.calculate_time_balance(1, 2014)
    dec = balance["months"][11]
    assert dec["saldo"] == pytest.approx(dec["actual_hours"] - dec["target_hours"])
    assert dec["actual_hours"] == 0.0  # weder 5OVER noch TYPE=2 im Ist
    assert dec["adjustment"] == pytest.approx(7.0)  # informativ: Überstundenkonto
    assert balance["total_overtime_account"] == pytest.approx(7.0)


def test_carry_forward_is_type0_booking(tmp_path):
    """Spec 3.6.2 Nr. 6: Übertrag = TYPE-0-Buchung am 1.1.; TYPE=2 bleibt frei."""
    db = make_db(tmp_path, {"5EMPL": [EMP_WEEK], "5BOOK": []})
    db.set_carry_forward(1, 2015, 12.5)
    bookings = db.get_bookings(year=2015, employee_id=1)
    assert len(bookings) == 1
    assert bookings[0]["type"] == 0
    assert bookings[0]["date"] == "2015-01-01"
    cf = db.get_carry_forward(1, 2015)
    assert cf["hours"] == pytest.approx(12.5)
    # Ersetzen statt Anhäufen
    db.set_carry_forward(1, 2015, 4.0)
    assert db.get_carry_forward(1, 2015)["hours"] == pytest.approx(4.0)
    assert len(db.get_bookings(year=2015, employee_id=1)) == 1


def test_cycle_assignments_expand_into_hours(tmp_path):
    """Befund 9: 5CYASS-Dienste fließen ohne Materialisierung in die Iststunden."""
    cycle = {"ID": 8, "NAME": "1-Woche", "POSITION": 1, "SIZE": 1, "UNIT": 1, "HIDE": 0}
    entries = [{"ID": i + 1, "CYCLEEID": 8, "INDEX": i, "SHIFTID": 1, "WORKPLACID": 0}
               for i in range(5)]  # Mo-Fr Tagschicht
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5SHIFT": [DAY_SHIFT],
        "5CYCLE": [cycle],
        "5CYENT": entries,
        "5CYASS": [{"ID": 1, "EMPLOYEEID": 1, "CYCLEID": 8,
                    "START": "2014-12-01", "ENTRANCE": 0}],
    })
    stats = db.get_statistics(2014, 12)
    # Dez. 2014: 23 Mo-Fr-Tage; mit Feiertagskalender gilt am 25./26. und am
    # halben 31.12. der Tagindex 7 (Spec 3.4.3 Nr. 5) — kein Ft-Fenster ⇒ 0 h.
    (tmp_path / "hol").mkdir()
    db_holidays = make_db(tmp_path / "hol", {
        "5EMPL": [EMP_WEEK], "5SHIFT": [DAY_SHIFT], "5HOLID": HOLIDAYS_2014,
        "5CYCLE": [cycle], "5CYENT": entries,
        "5CYASS": [{"ID": 1, "EMPLOYEEID": 1, "CYCLEID": 8,
                    "START": "2014-12-01", "ENTRANCE": 0}],
    })
    assert stats[0]["actual_hours"] == pytest.approx(23 * 8.0)
    assert db_holidays.get_statistics(2014, 12)[0]["actual_hours"] == pytest.approx(20 * 8.0)
