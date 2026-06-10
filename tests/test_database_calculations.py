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
    "5GROUP": [("ID", "N", 11), ("NAME", "C", 40), ("SHORTNAME", "C", 12),
               ("POSITION", "N", 11), ("HIDE", "N", 1)],
    "5GRASG": [("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("GROUPID", "N", 11)],
    "5SHDEM": [("ID", "N", 11), ("GROUPID", "N", 11), ("WEEKDAY", "N", 5),
               ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("MIN", "N", 5),
               ("MAX", "N", 5)],
    "5SPDEM": [("ID", "N", 11), ("GROUPID", "N", 11), ("DATE", "D", 8),
               ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("MIN", "N", 5),
               ("MAX", "N", 5)],
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


# ─── 3.7 Urlaub: Balance und Jahresabschluss (Befunde 6 und 11) ───────────────


def _leave_db(tmp_path, absen_rows, leaen_rows):
    return make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5LEAVT": [URLAUB, SONDERURLAUB],
        "5LEAEN": leaen_rows,
        "5ABSEN": absen_rows,
    })


def test_leave_balance_per_type_and_half_days(tmp_path):
    """Befund 11: Verbrauch nach 3.5.2 (Halbtag = 0,5), kein Default-Anspruch."""
    absen = [
        {"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-06-02", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0},
        {"ID": 2, "EMPLOYEEID": 1, "DATE": "2014-06-03", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 1, "START": 0, "END": 0},  # halber Tag
        {"ID": 3, "EMPLOYEEID": 1, "DATE": "2014-06-04", "LEAVETYPID": 14,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0},
    ]
    leaen = [
        {"ID": 1, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 1,
         "ENTITLEMNT": 30.0, "REST": 2.0, "INDAYS": 1},
        {"ID": 2, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 14,
         "ENTITLEMNT": 2.0, "REST": 0.0, "INDAYS": 1},
    ]
    db = _leave_db(tmp_path, absen, leaen)
    bal = db.get_leave_balance(1, 2014)
    assert bal["entitlement"] == pytest.approx(32.0)
    assert bal["carry_forward"] == pytest.approx(2.0)
    assert bal["used"] == pytest.approx(2.5)  # 1 + 0,5 Urlaub + 1 Sonderurlaub
    assert bal["remaining"] == pytest.approx(31.5)
    # Gap C-6 (Spec 3.9.3 Nr. 6): Aufschlüsselung je Art zusätzlich zur Summe
    by_type = {t["leave_type_id"]: t for t in bal["by_type"]}
    assert by_type[1]["used"] == pytest.approx(1.5)
    assert by_type[1]["remaining"] == pytest.approx(30.5)  # 30 + 2 − 1,5
    assert by_type[14]["used"] == pytest.approx(1.0)
    assert by_type[14]["remaining"] == pytest.approx(1.0)
    # Ohne 5LEAEN-Satz: kein erfundener Default-Anspruch
    (tmp_path / "leer").mkdir()
    empty = _leave_db(tmp_path / "leer", [], [])
    assert empty.get_leave_balance(1, 2014)["entitlement"] == 0.0


def test_set_leave_entitlement_keeps_floats(tmp_path):
    """Befund 6 (Halbtage): 12,5 / 0,5 dürfen nicht zu 12 / 0 werden."""
    db = _leave_db(tmp_path, [], [])
    db.set_leave_entitlement(1, 2014, days=12.5, carry_forward=0.5, leave_type_id=1)
    rows = db.get_leave_entitlements(year=2014, employee_id=1)
    assert rows[0]["entitlement"] == pytest.approx(12.5)
    assert rows[0]["carry_forward"] == pytest.approx(0.5)


def test_annual_close_per_type_no_cap(tmp_path):
    """Befund 6: je Art fortschreiben, CARRYFWD beachten, kein 10-Tage-Cap."""
    absen = [
        {"ID": i, "EMPLOYEEID": 1, "DATE": f"2014-06-{i + 1:02d}", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0}
        for i in range(1, 6)  # 5 Tage Urlaub genommen
    ]
    leaen = [
        {"ID": 1, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 1,
         "ENTITLEMNT": 30.0, "REST": 2.0, "INDAYS": 1},
        {"ID": 2, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 14,
         "ENTITLEMNT": 2.0, "REST": 0.0, "INDAYS": 1},
    ]
    db = _leave_db(tmp_path, absen, leaen)
    result = db.run_annual_close(2014)
    # Urlaub (CARRYFWD=1): Rest 30+2-5 = 27 — ungedeckelt, kein Cap bei 10
    assert result["total_carry_forward"] == pytest.approx(27.0)
    # Sonderurlaub (CARRYFWD=0): Rest 2 verfällt
    assert result["total_forfeited"] == pytest.approx(2.0)
    rows = {r["leave_type_id"]: r for r in db.get_leave_entitlements(year=2015, employee_id=1)}
    # Je Art fortgeschrieben — kein Kollabieren auf LEAVETYPID=0; Sonderurlaub
    # (CARRYFWD=0) wird ohne Dialog-Option gar nicht verarbeitet (Spec 3.7.2)
    assert set(rows) == {1}
    assert rows[1]["entitlement"] == pytest.approx(30.0)  # STDENTIT-Vorbelegung
    assert rows[1]["carry_forward"] == pytest.approx(27.0)


def test_forfeit_rest_cuts_to_consumption(tmp_path):
    """Spec 3.7.3 Nr. 8 (Gap A-3): REST wird auf den Verbrauch bis zum Stichtag
    gekürzt, nie erhöht; ENTITLEMNT bleibt unberührt; dry_run schreibt nicht."""
    absen = [
        {"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-02-10", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0},  # 1 Tag vor Stichtag
        {"ID": 2, "EMPLOYEEID": 1, "DATE": "2014-06-02", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0},  # nach Stichtag
    ]
    leaen = [
        {"ID": 1, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 1,
         "ENTITLEMNT": 30.0, "REST": 5.0, "INDAYS": 1},
        {"ID": 2, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 14,
         "ENTITLEMNT": 2.0, "REST": 0.0, "INDAYS": 1},  # REST 0 → keine Kürzung
    ]
    db = _leave_db(tmp_path, absen, leaen)

    # Vorschau: Kürzung 5 → 1 (Verbrauch bis 31.3.), kein Schreiben
    preview = db.forfeit_rest("2014-03-31", dry_run=True)
    assert preview["dry_run"] is True
    assert len(preview["cuts"]) == 1
    cut = preview["cuts"][0]
    assert (cut["leave_type_id"], cut["old_rest"], cut["new_rest"]) == (1, 5.0, 1.0)
    assert preview["total_forfeited"] == pytest.approx(4.0)
    rows = {r["leave_type_id"]: r for r in db.get_leave_entitlements(year=2014, employee_id=1)}
    assert rows[1]["carry_forward"] == pytest.approx(5.0)  # unverändert

    # Produktiv: 5LEAEN.REST wird gekürzt, ENTITLEMNT bleibt
    result = db.forfeit_rest("2014-03-31")
    assert result["total_forfeited"] == pytest.approx(4.0)
    rows = {r["leave_type_id"]: r for r in db.get_leave_entitlements(year=2014, employee_id=1)}
    assert rows[1]["carry_forward"] == pytest.approx(1.0)
    assert rows[1]["entitlement"] == pytest.approx(30.0)
    assert rows[14]["carry_forward"] == pytest.approx(0.0)

    # Idempotent: zweiter Lauf kürzt nichts mehr (REST == Verbrauch)
    again = db.forfeit_rest("2014-03-31")
    assert again["cuts"] == []
    assert again["total_forfeited"] == pytest.approx(0.0)


# ─── 6.3/4.2 Zyklusdienste im Plan-Lesepfad (Gap B-2) ─────────────────────────


def _cycle_db(tmp_path, extra=None):
    """1-Wochen-Zyklus Mo-Fr Tagschicht ab 1.12.2014 für MA 1."""
    cycle = {"ID": 8, "NAME": "1-Woche", "POSITION": 1, "SIZE": 1, "UNIT": 1, "HIDE": 0}
    entries = [{"ID": i + 1, "CYCLEEID": 8, "INDEX": i, "SHIFTID": 1, "WORKPLACID": 0}
               for i in range(5)]
    tables = {
        "5EMPL": [EMP_WEEK],
        "5SHIFT": [DAY_SHIFT],
        "5CYCLE": [cycle],
        "5CYENT": entries,
        "5CYASS": [{"ID": 1, "EMPLOYEEID": 1, "CYCLEID": 8,
                    "START": "2014-12-01", "ENTRANCE": 0}],
    }
    tables.update(extra or {})
    return make_db(tmp_path, tables)


def test_schedule_read_paths_expand_cycles(tmp_path):
    """Spec 6.3/4.2: Zyklusdienste erscheinen ohne Materialisierung im Plan;
    materialisierte 5MASHI-Tage gewinnen, generierte Einträge tragen
    source='cycle'."""
    db = _cycle_db(tmp_path, {
        # Mo 1.12. ist materialisiert → kein Zyklus-Duplikat
        "5MASHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01"}],
        # Mi 3.12.: Arbeitszeitabweichung (SHIFTID gesetzt) ersetzt den Zyklusdienst
        "5SPSHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-03",
                    "STARTEND": "06:00-12:00", "DURATION": 6.0, "NOEXTRA": 0,
                    "TYPE": 1}],
    })

    entries = db.get_schedule(2014, 12)
    by_date: dict = {}
    for e in entries:
        by_date.setdefault(e["date"], []).append(e)
    # Mo 1.12.: genau ein Eintrag (5MASHI), kein Zyklus-Duplikat
    assert len(by_date["2014-12-01"]) == 1
    assert by_date["2014-12-01"][0].get("source") is None
    # Di 2.12.: generierter Zykluseintrag
    di = [e for e in by_date["2014-12-02"] if e["kind"] == "shift"]
    assert len(di) == 1 and di[0]["source"] == "cycle" and di[0]["shift_id"] == 1
    # Mi 3.12.: Abweichung ersetzt den Zyklusdienst (nur special_shift)
    assert [e["kind"] for e in by_date["2014-12-03"]] == ["special_shift"]
    # Sa 6.12.: zyklusfreier Tag → kein Eintrag
    assert "2014-12-06" not in by_date

    # Tagesansicht: Di 2.12. zeigt den Zyklusdienst
    day = {r["employee_id"]: r for r in db.get_schedule_day("2014-12-02")}
    assert day[1]["kind"] == "shift" and day[1]["source"] == "cycle"
    assert day[1]["shift_id"] == 1
    # Tagesansicht: Mo 1.12. zeigt den materialisierten Dienst (kein cycle)
    day = {r["employee_id"]: r for r in db.get_schedule_day("2014-12-01")}
    assert day[1]["kind"] == "shift" and day[1]["source"] is None

    # Wochenansicht: Mo materialisiert, Di-Fr aus dem Zyklus
    week = db.get_schedule_week("2014-12-01")
    by_day = {d["date"]: d["entries"][0] for d in week["days"]}
    assert by_day["2014-12-01"]["source"] is None
    assert by_day["2014-12-02"]["source"] == "cycle"
    assert by_day["2014-12-05"]["shift_id"] == 1
    assert by_day["2014-12-06"]["kind"] is None  # Sa frei


# ─── 3.9.1 Freier Auswertungszeitraum (Gap C-1) ───────────────────────────────


def test_statistics_free_period(tmp_path):
    """Spec 3.9.1: get_statistics über [von, bis] — Monatskomfort bleibt."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5HOLID": HOLIDAYS_2014,
        "5SHIFT": [DAY_SHIFT],
        "5MASHI": [
            {"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01"},
            {"ID": 2, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-15"},
        ],
    })
    by_month = db.get_statistics(2014, 12)[0]
    by_period = db.get_statistics(date_from="2014-12-01", date_to="2014-12-31")[0]
    assert by_period == by_month

    # Teilzeitraum 1.–7.12.: nur der Dienst am 1.12., Soll = 1 Woche (38,5)
    partial = db.get_statistics(date_from="2014-12-01", date_to="2014-12-07")[0]
    assert partial["actual_hours"] == pytest.approx(8.0)
    assert partial["target_hours"] == pytest.approx(38.5)
    assert partial["shifts_count"] == 1

    with pytest.raises(ValueError):
        db.get_statistics(date_from="2014-12-31", date_to="2014-12-01")
    with pytest.raises(ValueError):
        db.get_statistics()


# ─── 3.9.2/3.9.3 Personaltabelle (Gap C-3) ────────────────────────────────────


def test_personnel_table_standard_and_dynamic_columns(tmp_path):
    """Spec 3.9.2/3.9.3: Standard-Spalten, Spalten je Schicht-/Abwesenheitsart
    und der Urlaubs-Doppelwert genommen/verbleibend bei Ein-Jahres-Zeitraum."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5HOLID": HOLIDAYS_2014,
        "5SHIFT": [DAY_SHIFT],
        "5LEAVT": [URLAUB],
        "5LEAEN": [{"ID": 1, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 1,
                    "ENTITLEMNT": 30.0, "REST": 0.0, "INDAYS": 1}],
        "5MASHI": [
            {"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01"},  # Mo
            {"ID": 2, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-07"},  # So
        ],
        "5SPSHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 0, "DATE": "2014-12-13",
                    "STARTEND": "08:00-12:00", "DURATION": 4.0, "NOEXTRA": 0,
                    "TYPE": 0}],
        "5ABSEN": [
            {"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-02", "LEAVETYPID": 1,
             "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0},   # ganzer Tag
            {"ID": 2, "EMPLOYEEID": 1, "DATE": "2014-12-03", "LEAVETYPID": 1,
             "TYPE": 0, "INTERVAL": 1, "START": 0, "END": 0},   # halber Tag
        ],
    })

    # Monatszeitraum: Standard- und dynamische Spalten, kein Doppelwert
    table = db.get_personnel_table("2014-12-01", "2014-12-31")
    assert table["one_year"] is False
    row = table["rows"][0]
    # Arbeitszeit: Mo 8 h (So-Dienst ohne So-Fenster = 0) + Sonderdienst 4 h
    assert row["arbeitszeit"] == pytest.approx(12.0)
    # Abwesenheit bezahlt: 1,5 Tage Urlaub à 7,7 h
    assert row["abwesenheit_bezahlt"] == pytest.approx(1.5 * 7.7)
    # EMP_WEEK ist der normative Fall (CALCBASE=1, DEDUCTHOL=1): 157,85 h
    assert row["sollstunden"] == pytest.approx(157.85)
    assert row["iststunden"] == pytest.approx(12.0 + 1.5 * 7.7)
    assert row["saldo"] == pytest.approx(row["iststunden"] - row["sollstunden"])
    assert row["sonntag"] == 1
    assert row["feiertag"] == 0
    assert row["sonderdienste"] == 1
    assert row["shift_counts"] == {1: 2}  # 3.9.3 Nr. 4 (nur 5MASHI/5CYASS)
    assert row["absence_days_by_type"][1] == pytest.approx(1.5)  # Nr. 5
    assert "leave_accounts" not in row

    # Ein-Jahres-Zeitraum: Doppelwert genommen/verbleibend (Nr. 6)
    table = db.get_personnel_table("2014-01-01", "2014-12-31")
    assert table["one_year"] is True
    acct = table["rows"][0]["leave_accounts"][1]
    assert acct["taken"] == pytest.approx(1.5)
    assert acct["remaining"] == pytest.approx(28.5)


# ─── 3.9.4 Personalauslastung (Gap C-4) ───────────────────────────────────────


def test_utilization_against_demand(tmp_path):
    """Spec 3.9.4 Nr. 8/9: ist<min ⇒ under, ist>max ⇒ over, sonst ok;
    Tage/Zellen ohne Bedarf ⇒ none; 5SPDEM überschreibt den Wochenbedarf."""
    emp2 = dict(EMP_WEEK, ID=2, NAME="Zweit", SHORTNAME="Z2")
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK, emp2],
        "5SHIFT": [DAY_SHIFT],
        "5GROUP": [{"ID": 1, "NAME": "Team", "SHORTNAME": "T", "POSITION": 1, "HIDE": 0}],
        "5GRASG": [{"ID": 1, "EMPLOYEEID": 1, "GROUPID": 1},
                   {"ID": 2, "EMPLOYEEID": 2, "GROUPID": 1}],
        # Wochenbedarf: Mo (Tagindex 0) min 2 / max 2 für Schicht 1
        "5SHDEM": [{"ID": 1, "GROUPID": 1, "WEEKDAY": 0, "SHIFTID": 1,
                    "WORKPLACID": 0, "MIN": 2, "MAX": 2}],
        # Tagesbedarf Mo 8.12.: min 1 / max 1 (überschreibt Wochenbedarf)
        "5SPDEM": [{"ID": 1, "GROUPID": 1, "DATE": "2014-12-08", "SHIFTID": 1,
                    "WORKPLACID": 0, "MIN": 1, "MAX": 1}],
        "5MASHI": [
            # Mo 1.12.: nur MA 1 eingeteilt → 1 < min 2 ⇒ under
            {"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01"},
            # Mo 8.12.: beide eingeteilt, SPDEM max 1 ⇒ over
            {"ID": 2, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-08"},
            {"ID": 3, "EMPLOYEEID": 2, "SHIFTID": 1, "DATE": "2014-12-08"},
            # Mo 15.12.: beide eingeteilt → min 2 erfüllt ⇒ ok
            {"ID": 4, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-15"},
            {"ID": 5, "EMPLOYEEID": 2, "SHIFTID": 1, "DATE": "2014-12-15"},
        ],
    })
    days = {r["day"]: r for r in db.get_utilization(2014, 12)}

    assert days[1]["status"] == "under"
    assert days[1]["cells"][0]["assigned"] == 1
    assert days[1]["required_count"] == 2

    assert days[8]["status"] == "over"
    assert days[8]["cells"][0]["source"] == "SPDEM"
    assert days[8]["required_count"] == 1

    assert days[15]["status"] == "ok"
    assert days[15]["scheduled_count"] == 2

    # Di 2.12.: kein Bedarf definiert ⇒ none, required None (kein erfundenes 3)
    assert days[2]["status"] == "none"
    assert days[2]["required_count"] is None
    assert days[2]["cells"] == []


# ─── 3.8 Zuschläge (Befunde 7 und 8, Orakel-Stammdaten der Spec) ──────────────

SUN_CHARGE = {"ID": 1, "NAME": "Sonntagstunden", "POSITION": 1, "START": 0, "END": 0,
              "VALIDITY": 0, "VALIDDAYS": "0 0 0 0 0 0 1", "HOLRULE": 2, "HIDE": 0}
NIGHT_CHARGE = {"ID": 3, "NAME": "Nachtstunden", "POSITION": 3, "START": 1200,
                "END": 360, "VALIDITY": 0, "VALIDDAYS": "1 1 1 1 1 1 1",
                "HOLRULE": 0, "HIDE": 0}
# Bereitschaftsdienst: Fenster So 12:00-24:00, DURATION 4 (≠ Fensterlänge)
STANDBY = {"ID": 7, "NAME": "Bereitschaft", "SHORTNAME": "B", "POSITION": 7,
           "HIDE": 0, "NOEXTRA": 0, "STARTEND6": "12:00-24:00", "DURATION6": 4.0}


def test_sunday_charge_window_intersection(tmp_path):
    """Befunde 7+8: Sonntags-Maske aktiv, Fensterschnitt statt DURATION (12,0 statt 0/4)."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5SHIFT": [STANDBY],
        "5XCHAR": [SUN_CHARGE],
        # So 7.12.2014
        "5MASHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 7, "DATE": "2014-12-07"}],
    })
    result = {r["charge_name"]: r for r in db.calculate_extracharge_hours(2014, 12)}
    assert result["Sonntagstunden"]["hours"] == pytest.approx(12.0)
    # Gap C-8 (Spec 3.9.1): freier Zeitraum statt Monatszwang
    per = {r["charge_name"]: r for r in db.calculate_extracharge_hours(
        date_from="2014-12-01", date_to="2014-12-31")}
    assert per == result
    narrow = {r["charge_name"]: r for r in db.calculate_extracharge_hours(
        date_from="2014-12-08", date_to="2014-12-14")}
    assert narrow["Sonntagstunden"]["hours"] == pytest.approx(0.0)  # So 7.12. außerhalb
    with pytest.raises(ValueError):
        db.calculate_extracharge_hours(date_from="2014-12-31", date_to="2014-12-01")


def test_night_charge_crosses_midnight(tmp_path):
    """Nachtschicht 22-06 ∩ Nachtfenster 20-06 = 8 h über zwei Kalendertage."""
    night_shift = {"ID": 3, "NAME": "Nachtschicht", "SHORTNAME": "N", "POSITION": 3,
                   "HIDE": 0, "NOEXTRA": 0}
    for i in range(5):
        night_shift[f"STARTEND{i}"] = "22:00-06:00"
        night_shift[f"DURATION{i}"] = 8.0
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5SHIFT": [night_shift],
        "5XCHAR": [NIGHT_CHARGE],
        "5MASHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 3, "DATE": "2014-12-01"}],
    })
    result = {r["charge_name"]: r for r in db.calculate_extracharge_hours(2014, 12)}
    assert result["Nachtstunden"]["hours"] == pytest.approx(8.0)


def test_spshi_without_shiftid_uses_own_window(tmp_path):
    """Befund 8 Nr. 7: 5SPSHI ohne SHIFTID zählt mit eigenem STARTEND-Fenster."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5SHIFT": [],
        "5XCHAR": [SUN_CHARGE],
        # So 14.12.2014, eigenes Fenster 08:00-18:00
        "5SPSHI": [{"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 0, "DATE": "2014-12-14",
                    "STARTEND": "08:00-18:00", "DURATION": 10.0, "NOEXTRA": 0,
                    "TYPE": 0}],
    })
    result = {r["charge_name"]: r for r in db.calculate_extracharge_hours(2014, 12)}
    assert result["Sonntagstunden"]["hours"] == pytest.approx(10.0)
