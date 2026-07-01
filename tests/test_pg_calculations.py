"""Berechnungs-Äquivalenz: SP5PostgresDatabase == SP5Database.

Dieselben Fixture-Zeilen (DBF-Schlüsselform wie in test_database_calculations)
treiben beide Backends — die Mini-DBF-DB über make_db und den ORM-Spiegel über
die orm_url-Fixture (SQLite-Stellvertreter; echtes PostgreSQL bei gesetzter
DATABASE_URL, siehe conftest). Die Fassaden get_statistics,
get_personnel_table, get_utilization und forfeit_rest müssen identische
Ergebnisse liefern, weil beide auf dieselben sp5lib.calculations-Funktionen
verdrahtet sind.

PG-Schema-Grenzen (bewusst): keine CALCBASE/HRSTOTAL/DEDUCTHOL-Spalten
(HRSMONTH > 0 wird als Monatsbasis gerechnet) und keine COUNTALL/DEDUCT*/
CHARGEHRS-Spalten an der Abwesenheitsart — die Fixtures setzen diese
DBF-Felder auf 0, damit beide Pfade dieselbe Semantik sehen.
"""

import pytest
from test_database_calculations import make_db

from sp5lib.orm.models import (
    Absence,
    AccountBooking,
    Cycle,
    CycleAssignment,
    Employee,
    Group,
    GroupAssignment,
    Holiday,
    LeaveEntitlement,
    LeaveType,
    OvertimeEntry,
    ShiftAssignment,
    ShiftDemand,
    SpecialDemand,
    SpecialShift,
)
from sp5lib.orm.models import (
    Shift as OrmShift,
)
from sp5lib.orm.models_pg import CycleEntry, ExtraCharge
from sp5lib.pg_database import SP5PostgresDatabase

# ─── gemeinsame Fixture-Zeilen (DBF-Schlüsselform) ────────────────────────────

EMP1 = {
    "ID": 1, "NAME": "Muster", "FIRSTNAME": "Max", "SHORTNAME": "MM",
    "POSITION": 1, "HIDE": 0, "WORKDAYS": "1 1 1 1 1 0 0 0", "CALCBASE": 0,
    "HRSDAY": 8.0, "HRSWEEK": 0.0, "HRSMONTH": 0.0, "HRSTOTAL": 0.0,
    "DEDUCTHOL": 0,
}
EMP2 = dict(EMP1, ID=2, NAME="Zweit", FIRSTNAME="Zoe", SHORTNAME="ZZ", POSITION=2)
# Monatsbasis: DBF über CALCBASE=2, PG über die HRSMONTH-Brücke
EMP3 = dict(
    EMP1, ID=3, NAME="Monat", FIRSTNAME="Mia", SHORTNAME="MO", POSITION=3,
    CALCBASE=2, HRSMONTH=160.0,
)

DAY_SHIFT = {"ID": 1, "NAME": "Tagschicht", "SHORTNAME": "T", "POSITION": 1,
             "HIDE": 0, "NOEXTRA": 0}
for _i in range(5):
    DAY_SHIFT[f"STARTEND{_i}"] = "06:00-14:00"
    DAY_SHIFT[f"DURATION{_i}"] = 8.0

URLAUB = {"ID": 1, "NAME": "Urlaub", "SHORTNAME": "U", "POSITION": 1, "HIDE": 0,
          "CHARGETYP": 1, "CHARGEHRS": 0.0, "DEDUCTACT": 0, "DEDUCTOVT": 0,
          "ENTITLED": 1, "STDENTIT": 30.0, "CARRYFWD": 0, "COUNTALL": 0}
KRANK = dict(URLAUB, ID=3, NAME="Krankheit", SHORTNAME="K", POSITION=2,
             ENTITLED=0, STDENTIT=0.0)

HOLIDAYS = [
    {"ID": 1, "DATE": "2014-12-25", "NAME": "1. Weihnachtstag", "INTERVAL": 0},
    {"ID": 2, "DATE": "2014-12-26", "NAME": "2. Weihnachtstag", "INTERVAL": 0},
]

ROWS = {
    "5EMPL": [EMP1, EMP2, EMP3],
    "5GROUP": [{"ID": 1, "NAME": "Team", "SHORTNAME": "T", "POSITION": 1, "HIDE": 0}],
    "5GRASG": [{"ID": i, "EMPLOYEEID": i, "GROUPID": 1} for i in (1, 2, 3)],
    "5SHIFT": [DAY_SHIFT],
    "5LEAVT": [URLAUB, KRANK],
    "5HOLID": HOLIDAYS,
    "5MASHI": [
        {"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01", "WORKPLACID": 0},
        {"ID": 2, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-06", "WORKPLACID": 0},  # Sa: 0 h
        {"ID": 3, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-25", "WORKPLACID": 0},  # Ft: 0 h
        {"ID": 4, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-08", "WORKPLACID": 0},
        {"ID": 5, "EMPLOYEEID": 2, "SHIFTID": 1, "DATE": "2014-12-08", "WORKPLACID": 0},
        {"ID": 6, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-15", "WORKPLACID": 0},
        {"ID": 7, "EMPLOYEEID": 2, "SHIFTID": 1, "DATE": "2014-12-15", "WORKPLACID": 0},
    ],
    "5SPSHI": [
        # Arbeitszeitabweichung (SHIFTID gesetzt) ersetzt den Dienst am 1.12.
        {"ID": 1, "EMPLOYEEID": 1, "SHIFTID": 1, "DATE": "2014-12-01",
         "NAME": "Kurz", "SHORTNAME": "Kz", "STARTEND": "06:00-12:00",
         "DURATION": 6.0, "NOEXTRA": 0, "TYPE": 1, "WORKPLACID": 0},
        # reiner Sonderdienst mit eigenen Stunden
        {"ID": 2, "EMPLOYEEID": 1, "SHIFTID": 0, "DATE": "2014-12-13",
         "NAME": "Messe", "SHORTNAME": "Me", "STARTEND": "08:00-12:00",
         "DURATION": 4.0, "NOEXTRA": 0, "TYPE": 0, "WORKPLACID": 0},
    ],
    "5ABSEN": [
        {"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-03", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0},
        {"ID": 2, "EMPLOYEEID": 1, "DATE": "2014-12-04", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 1, "START": 0, "END": 0},  # halber Tag
        {"ID": 3, "EMPLOYEEID": 2, "DATE": "2014-12-10", "LEAVETYPID": 3,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0},
    ],
    "5BOOK": [
        {"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-15", "TYPE": 0,
         "VALUE": 10.0, "NOTE": ""},
        {"ID": 2, "EMPLOYEEID": 1, "DATE": "2014-12-15", "TYPE": 1,
         "VALUE": 10.0, "NOTE": ""},
        # Überstundenkonto-Buchung (TYPE=2): nicht im Saldo, nur informativ
        {"ID": 3, "EMPLOYEEID": 1, "DATE": "2014-12-20", "TYPE": 2,
         "VALUE": 5.0, "NOTE": ""},
    ],
    "5OVER": [
        {"ID": 1, "EMPLOYEEID": 1, "DATE": "2014-12-10", "HOURS": 3.0},
    ],
    # Frühzuschlag: Fenster 06:00-12:00, alle Wochentage gültig
    "5XCHAR": [
        {"ID": 1, "NAME": "Frühzuschlag", "POSITION": 1, "START": 360,
         "END": 720, "VALIDITY": 0, "VALIDDAYS": "1 1 1 1 1 1 1",
         "HOLRULE": 0, "HIDE": 0},
    ],
    "5LEAEN": [
        {"ID": 1, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 1,
         "ENTITLEMNT": 30.0, "REST": 5.0, "INDAYS": 1},
    ],
    "5CYCLE": [{"ID": 8, "NAME": "1-Woche", "POSITION": 1, "SIZE": 1, "UNIT": 1, "HIDE": 0}],
    "5CYENT": [{"ID": i + 1, "CYCLEEID": 8, "INDEX": i, "SHIFTID": 1, "WORKPLACID": 0}
               for i in range(5)],
    "5CYASS": [{"ID": 1, "EMPLOYEEID": 2, "CYCLEID": 8, "START": "2014-12-01",
                "ENTRANCE": 0}],
    "5SHDEM": [{"ID": 1, "GROUPID": 1, "WEEKDAY": 0, "SHIFTID": 1,
                "WORKPLACID": 0, "MIN": 2, "MAX": 2}],
    "5SPDEM": [{"ID": 1, "GROUPID": 1, "DATE": "2014-12-08", "SHIFTID": 1,
                "WORKPLACID": 0, "MIN": 1, "MAX": 1}],
}

# ─── ORM-Seeder: dieselben Zeilen in den SQLite-Spiegel ───────────────────────

_MAKERS = {
    "5EMPL": lambda r: Employee(
        id=r["ID"], name=r["NAME"], firstname=r["FIRSTNAME"],
        shortname=r["SHORTNAME"], position=r["POSITION"], hide=bool(r["HIDE"]),
        hrsday=r["HRSDAY"], hrsweek=r["HRSWEEK"], hrsmonth=r["HRSMONTH"],
        workdays=r["WORKDAYS"],
    ),
    "5GROUP": lambda r: Group(
        id=r["ID"], name=r["NAME"], shortname=r["SHORTNAME"],
        position=r["POSITION"], hide=bool(r["HIDE"]),
    ),
    "5GRASG": lambda r: GroupAssignment(
        id=r["ID"], employee_id=r["EMPLOYEEID"], group_id=r["GROUPID"],
    ),
    "5SHIFT": lambda r: OrmShift(
        id=r["ID"], name=r["NAME"], shortname=r["SHORTNAME"],
        position=r["POSITION"], hide=bool(r["HIDE"]),
        **{f"duration{i}": r.get(f"DURATION{i}", 0.0) for i in range(8)},
        **{f"startend{i}": r.get(f"STARTEND{i}", "") for i in range(8)},
    ),
    "5LEAVT": lambda r: LeaveType(
        id=r["ID"], name=r["NAME"], shortname=r["SHORTNAME"],
        position=r["POSITION"], hide=bool(r["HIDE"]),
        entitled=bool(r["ENTITLED"]), stdentit=r["STDENTIT"],
        chargetyp=r["CHARGETYP"],
    ),
    "5HOLID": lambda r: Holiday(
        id=r["ID"], date=r["DATE"], name=r["NAME"], interval=r["INTERVAL"],
    ),
    "5MASHI": lambda r: ShiftAssignment(
        id=r["ID"], employee_id=r["EMPLOYEEID"], date=r["DATE"],
        shift_id=r["SHIFTID"], workplace_id=r["WORKPLACID"],
    ),
    "5SPSHI": lambda r: SpecialShift(
        id=r["ID"], employee_id=r["EMPLOYEEID"], date=r["DATE"],
        name=r["NAME"], shortname=r["SHORTNAME"], shift_id=r["SHIFTID"],
        workplace_id=r["WORKPLACID"], entry_type=r["TYPE"],
        startend=r["STARTEND"], duration=r["DURATION"], noextra=r["NOEXTRA"],
    ),
    "5ABSEN": lambda r: Absence(
        id=r["ID"], employee_id=r["EMPLOYEEID"], date=r["DATE"],
        leave_type_id=r["LEAVETYPID"], entry_type=r["TYPE"],
        interval=r["INTERVAL"], start=r["START"], end=r["END"],
    ),
    "5BOOK": lambda r: AccountBooking(
        id=r["ID"], employee_id=r["EMPLOYEEID"], date=r["DATE"],
        booking_type=r["TYPE"], value=r["VALUE"], note=r["NOTE"],
    ),
    "5OVER": lambda r: OvertimeEntry(
        id=r["ID"], employee_id=r["EMPLOYEEID"], date=r["DATE"],
        hours=r["HOURS"],
    ),
    "5XCHAR": lambda r: ExtraCharge(
        id=r["ID"], name=r["NAME"], position=r["POSITION"], start=r["START"],
        end=r["END"], validity=r["VALIDITY"], validdays=r["VALIDDAYS"],
        holrule=r["HOLRULE"], hide=bool(r["HIDE"]),
    ),
    "5LEAEN": lambda r: LeaveEntitlement(
        id=r["ID"], employee_id=r["EMPLOYEEID"], year=r["YEAR"],
        leave_type_id=r["LEAVETYPID"], entitlement=r["ENTITLEMNT"],
        carry_forward=r["REST"], in_days=bool(r["INDAYS"]),
    ),
    "5CYCLE": lambda r: Cycle(
        id=r["ID"], name=r["NAME"], position=r["POSITION"], size=r["SIZE"],
        unit=r["UNIT"], hide=bool(r["HIDE"]),
    ),
    "5CYENT": lambda r: CycleEntry(
        id=r["ID"], cycle_id=r["CYCLEEID"], index=r["INDEX"],
        shift_id=r["SHIFTID"], workplace_id=r["WORKPLACID"],
    ),
    "5CYASS": lambda r: CycleAssignment(
        id=r["ID"], employee_id=r["EMPLOYEEID"], cycle_id=r["CYCLEID"],
        start=r["START"], end="", entrance=str(r["ENTRANCE"]),
    ),
    "5SHDEM": lambda r: ShiftDemand(
        id=r["ID"], group_id=r["GROUPID"], weekday=r["WEEKDAY"],
        shift_id=r["SHIFTID"], workplace_id=r["WORKPLACID"],
        min_staff=r["MIN"], max_staff=r["MAX"],
    ),
    "5SPDEM": lambda r: SpecialDemand(
        id=r["ID"], group_id=r["GROUPID"], date=r["DATE"],
        shift_id=r["SHIFTID"], workplace_id=r["WORKPLACID"],
        min_staff=r["MIN"], max_staff=r["MAX"],
    ),
}


def make_pg(orm_url, rows_by_table):
    db = SP5PostgresDatabase(orm_url)
    db.init_db()
    with db._session() as s:
        for table, rows in rows_by_table.items():
            for row in rows:
                s.add(_MAKERS[table](row))
    return db


@pytest.fixture
def both(tmp_path, orm_url):
    dbf_dir = tmp_path / "dbf"
    dbf_dir.mkdir()
    pg = make_pg(orm_url, ROWS)
    yield make_db(dbf_dir, ROWS), pg
    pg._engine.dispose()


# ─── Äquivalenztests ──────────────────────────────────────────────────────────


def test_statistics_equivalent(both):
    dbf, pg = both
    dbf_rows = dbf.get_statistics(2014, 12)
    pg_rows = pg.get_statistics(2014, 12)
    assert pg_rows == dbf_rows
    # Plausibilitäts-Anker (nicht nur "beide gleich falsch"):
    by_id = {r["employee_id"]: r for r in pg_rows}
    # MA 1: Abweichung 6 h (ersetzt 1.12.) + Mo 8./15. je 8 h + Sa/Ft 0 h
    #       + Sonderdienst 4 h + 1,5 Tage Urlaub à 8 h + Ist-Buchung 10 h
    assert by_id[1]["actual_hours"] == pytest.approx(6 + 16 + 4 + 12 + 10)
    # MA 3 (Monatsbasis): voller Monat = HRSMONTH
    assert by_id[3]["target_hours"] == pytest.approx(160.0)
    assert by_id[1]["group_name"] == "Team"
    assert by_id[2]["sick_days"] == 1


def test_statistics_free_period_equivalent(both):
    dbf, pg = both
    assert pg.get_statistics(date_from="2014-12-01", date_to="2014-12-31") == \
        dbf.get_statistics(date_from="2014-12-01", date_to="2014-12-31")
    assert pg.get_statistics(date_from="2014-12-01", date_to="2014-12-07") == \
        dbf.get_statistics(date_from="2014-12-01", date_to="2014-12-07")
    with pytest.raises(ValueError):
        pg.get_statistics(date_from="2014-12-31", date_to="2014-12-01")
    with pytest.raises(ValueError):
        pg.get_statistics()


def test_personnel_table_equivalent(both):
    dbf, pg = both
    assert pg.get_personnel_table("2014-12-01", "2014-12-31") == \
        dbf.get_personnel_table("2014-12-01", "2014-12-31")
    # Ein-Jahres-Zeitraum inkl. Doppelwert genommen/verbleibend (5LEAEN)
    pg_year = pg.get_personnel_table("2014-01-01", "2014-12-31")
    assert pg_year == dbf.get_personnel_table("2014-01-01", "2014-12-31")
    # MA 1 (Muster) explizit adressieren - die Zeilen sind namenssortiert
    row_muster = next(r for r in pg_year["rows"] if r["employee_id"] == 1)
    acct = row_muster["leave_accounts"][1]
    assert acct["taken"] == pytest.approx(1.5)
    assert acct["remaining"] == pytest.approx(30 + 5 - 1.5)
    with pytest.raises(ValueError):
        pg.get_personnel_table("2014-12-31", "2014-12-01")


def test_utilization_equivalent(both):
    dbf, pg = both
    pg_days = pg.get_utilization(2014, 12)
    assert pg_days == dbf.get_utilization(2014, 12)
    by_day = {r["day"]: r for r in pg_days}
    # Mo 1.12.: MA 1 manuell + MA 2 per Zyklus = 2 = min ⇒ ok (Zyklus zählt)
    assert by_day[1]["status"] == "ok"
    assert by_day[1]["cells"][0]["assigned"] == 2
    # Mo 22.12.: nur MA 2 per Zyklus ⇒ 1 < min 2 ⇒ under
    assert by_day[22]["status"] == "under"
    assert by_day[22]["cells"][0]["assigned"] == 1
    assert by_day[8]["cells"][0]["source"] == "SPDEM"
    assert by_day[8]["status"] == "over"
    assert by_day[15]["status"] == "ok"
    assert by_day[2]["required_count"] is None or by_day[2]["cells"]


def test_forfeit_rest_equivalent(both):
    dbf, pg = both
    preview_dbf = dbf.forfeit_rest("2014-12-05", dry_run=True)
    preview_pg = pg.forfeit_rest("2014-12-05", dry_run=True)
    assert preview_pg == preview_dbf
    # Verbrauch bis 5.12. = 1,5 Tage < REST 5 → Kürzung auf 1,5
    assert preview_pg["cuts"][0]["new_rest"] == pytest.approx(1.5)

    result_pg = pg.forfeit_rest("2014-12-05")
    result_dbf = dbf.forfeit_rest("2014-12-05")
    assert result_pg == result_dbf
    assert result_pg["total_forfeited"] == pytest.approx(3.5)

    # persistiert: PG carry_forward == DBF REST == 1,5; zweiter Lauf leer
    rows = dbf.get_leave_entitlements(year=2014, employee_id=1)
    assert rows[0]["carry_forward"] == pytest.approx(1.5)
    with pg._session() as s:
        from sqlalchemy import select
        leaen = s.scalars(select(LeaveEntitlement)).all()
        assert leaen[0].carry_forward == pytest.approx(1.5)
    assert pg.forfeit_rest("2014-12-05")["cuts"] == []
    assert dbf.forfeit_rest("2014-12-05")["cuts"] == []


def test_time_balance_equivalent(both):
    dbf, pg = both
    pg_tb = pg.calculate_time_balance(1, 2014)
    assert pg_tb == dbf.calculate_time_balance(1, 2014)
    dez = pg_tb["months"][11]
    # 5OVER 10.12. (+3 h) und 5BOOK TYPE=2 (+5 h): nur informativ, nicht im Saldo
    assert dez["overtime_adjustment"] == pytest.approx(3.0)
    assert dez["booking_adjustment"] == pytest.approx(5.0)
    assert dez["actual_hours"] == pytest.approx(48.0)  # wie Statistik-Anker
    assert dez["saldo"] == pytest.approx(dez["actual_hours"] - dez["target_hours"])
    assert pg_tb["total_overtime_account"] == pytest.approx(8.0)
    # unbekannter Mitarbeiter: leeres Dict wie SP5Database
    assert pg.calculate_time_balance(99, 2014) == dbf.calculate_time_balance(99, 2014) == {}


def test_zeitkonto_equivalent(both):
    dbf, pg = both
    assert pg.get_zeitkonto(2014) == dbf.get_zeitkonto(2014)
    assert pg.get_zeitkonto(2014, employee_id=1) == dbf.get_zeitkonto(2014, employee_id=1)
    assert pg.get_zeitkonto(2014, group_id=1) == dbf.get_zeitkonto(2014, group_id=1)


def test_employee_stats_equivalent(both):
    dbf, pg = both
    pg_year = pg.get_employee_stats_year(1, 2014)
    assert pg_year == dbf.get_employee_stats_year(1, 2014)
    dez = pg_year["months"][11]
    assert dez["shifts_count"] == 7    # 5 × 5MASHI + 2 × 5SPSHI
    assert dez["weekend_shifts"] == 2  # Sa 6.12. + Messe Sa 13.12.
    assert dez["vacation_days"] == 2   # zwei Urlaubs-Sätze (Tageszählung je Record)
    assert dez["night_shifts"] == 0
    assert pg.get_employee_stats_month(1, 2014, 12) == \
        dbf.get_employee_stats_month(1, 2014, 12)
    assert pg.get_employee_stats_year(99, 2014) == \
        dbf.get_employee_stats_year(99, 2014) == {}


def test_schedule_year_equivalent(both):
    dbf, pg = both
    for eid in (1, 2, 3):
        assert pg.get_schedule_year(2014, eid) == dbf.get_schedule_year(2014, eid)
    dez = pg.get_schedule_year(2014, 1)[11]
    assert dez["shifts"] == 7
    assert dez["absences"] == 2
    # reine Arbeitszeit (GetWorkHours): 6 (ersetzt) + 8 + 8 + 4 (Messe)
    assert dez["actual_hours"] == pytest.approx(26.0)
    assert dez["label_counts"]["T"] == 5


def test_extracharge_equivalent(both):
    dbf, pg = both
    assert pg.calculate_extracharge_hours(2014, 12) == \
        dbf.calculate_extracharge_hours(2014, 12)
    pg_one = pg.calculate_extracharge_hours(2014, 12, employee_id=1)
    assert pg_one == dbf.calculate_extracharge_hours(2014, 12, employee_id=1)
    # Frühfenster 06-12: 1.12. (6 h, Abweichung), 8./15.12. (je 6 h), Messe 13.12. (4 h)
    assert pg_one[0]["hours"] == pytest.approx(22.0)
    assert pg_one[0]["shift_count"] == 4
    assert pg.calculate_extracharge_hours(date_from="2014-12-01", date_to="2014-12-07") == \
        dbf.calculate_extracharge_hours(date_from="2014-12-01", date_to="2014-12-07")


def test_leave_balance_equivalent(both):
    dbf, pg = both
    bal = pg.get_leave_balance(1, 2014)
    assert bal == dbf.get_leave_balance(1, 2014)
    assert bal["entitlement"] == pytest.approx(30.0)
    assert bal["carry_forward"] == pytest.approx(5.0)
    assert bal["used"] == pytest.approx(1.5)
    assert bal["remaining"] == pytest.approx(33.5)
    assert bal["has_custom_entitlement"] is True
    # MA 2 ohne 5LEAEN-Satz: kein Default-Anspruch (wie das Original)
    bal2 = pg.get_leave_balance(2, 2014)
    assert bal2 == dbf.get_leave_balance(2, 2014)
    assert bal2["total"] == pytest.approx(0.0)
    assert pg.get_leave_balance_group(2014, 1) == dbf.get_leave_balance_group(2014, 1)


def test_annual_close_not_implemented(both):
    _, pg = both
    with pytest.raises(NotImplementedError):
        pg.get_annual_close_preview(2014)
    with pytest.raises(NotImplementedError):
        pg.run_annual_close(2014)
