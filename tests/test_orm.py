"""Tests for the Phase-2 / Phase-3 ORM models, repositories and DBF sync.

These run entirely against an in-memory SQLite database (no external
fixtures), mirroring the self-contained style of test_dbf.py. They cover:
- Phase 2: Shift / LeaveType / Workplace models, repositories, to_dict().
- Phase 3: ShiftAssignment (MASHI) / SpecialShift (SPSHI) / Absence (ABSEN)
  models, date-range repositories, and the schedule sync (invalid dates
  skipped, FK-tolerant integer references).
- The sync_groups dangling super_id fix (dangling parent -> NULL, no abort).
DBF reads are exercised via _read_dbf patched to canned rows.
"""

import pytest

from sp5lib.orm import (
    Absence,
    AbsenceRepository,
    AccountBooking,
    AccountBookingRepository,
    Cycle,
    CycleAssignment,
    CycleAssignmentRepository,
    CycleRepository,
    Employee,
    Group,
    GroupAssignment,
    Holiday,
    HolidayRepository,
    LeaveEntitlement,
    LeaveEntitlementRepository,
    LeaveType,
    LeaveTypeRepository,
    OvertimeEntry,
    OvertimeEntryRepository,
    Period,
    PeriodRepository,
    Restriction,
    RestrictionRepository,
    ScheduleEntry,
    Shift,
    ShiftAssignment,
    ShiftAssignmentRepository,
    ShiftDemand,
    ShiftDemandRepository,
    ShiftRepository,
    SpecialDemand,
    SpecialDemandRepository,
    SpecialShift,
    SpecialShiftRepository,
    Workplace,
    WorkplaceRepository,
    get_engine,
    init_db,
)
from sp5lib.orm.base import session_scope


@pytest.fixture
def engine(orm_url):
    """Fresh engine (SQLite stand-in or DATABASE_URL) with all ORM tables created."""
    eng = get_engine(orm_url)
    init_db(eng)
    yield eng
    eng.dispose()


# ─── schema / importability ───────────────────────────────────────────────────


def test_init_db_creates_phase2_tables(engine):
    from sqlalchemy import inspect

    tables = set(inspect(engine).get_table_names())
    assert {"shifts", "leave_types", "workplaces"} <= tables


def test_models_reexported_from_models_pg_are_identical():
    # Single source of truth: models_pg re-exports the canonical classes.
    from sp5lib.orm import models, models_pg

    assert models.Shift is models_pg.Shift
    assert models.LeaveType is models_pg.LeaveType
    assert models.Workplace is models_pg.Workplace


# ─── repositories: list() / get() ───────────────────────────────────────────────


def test_shift_repository_list_orders_by_position_and_hides(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                Shift(id=1, name="Spät", position=2),
                Shift(id=2, name="Früh", position=1),
                Shift(id=3, name="Alt", position=3, hide=True),
            ]
        )
        session.flush()
        repo = ShiftRepository(session)

        visible = repo.list()
        assert [s.id for s in visible] == [2, 1]  # ordered by position, hidden excluded

        all_shifts = repo.list(include_hidden=True)
        assert [s.id for s in all_shifts] == [2, 1, 3]


def test_shift_repository_get(engine):
    with session_scope(engine) as session:
        session.add(Shift(id=5, name="Nacht"))
        session.flush()
        repo = ShiftRepository(session)
        assert repo.get(5).name == "Nacht"
        assert repo.get(999) is None


def test_leave_type_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                LeaveType(id=1, name="Urlaub", position=1),
                LeaveType(id=2, name="Krank", position=2, hide=True),
            ]
        )
        session.flush()
        repo = LeaveTypeRepository(session)
        assert [lt.id for lt in repo.list()] == [1]
        assert [lt.id for lt in repo.list(include_hidden=True)] == [1, 2]
        assert repo.get(1).name == "Urlaub"
        assert repo.get(2) is None or repo.get(2).name == "Krank"


def test_workplace_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                Workplace(id=1, name="Empfang", position=2),
                Workplace(id=2, name="Lager", position=1),
            ]
        )
        session.flush()
        repo = WorkplaceRepository(session)
        assert [w.id for w in repo.list()] == [2, 1]
        assert repo.get(2).name == "Lager"
        assert repo.get(404) is None


# ─── to_dict() shape ────────────────────────────────────────────────────────────


def test_shift_to_dict_shape():
    d = Shift(id=1, name="Früh", shortname="F", duration0=8.0, startend0="08:00-16:00").to_dict()
    assert d["ID"] == 1
    assert d["NAME"] == "Früh"
    assert d["SHORTNAME"] == "F"
    assert d["DURATION0"] == 8.0
    assert d["STARTEND0"] == "08:00-16:00"
    # all eight weekday slots are present
    assert all(f"DURATION{i}" in d and f"STARTEND{i}" in d for i in range(8))


def test_leave_type_to_dict_shape():
    d = LeaveType(id=2, name="Urlaub", entitled=True, stdentit=30.0).to_dict()
    assert d["ID"] == 2 and d["NAME"] == "Urlaub"
    assert d["ENTITLED"] is True and d["STDENTIT"] == 30.0
    assert {"COLORTEXT", "COLORBAR", "COLORBK"} <= d.keys()


def test_workplace_to_dict_shape(engine):
    # Column defaults (position/hide/colors) are applied by SQLAlchemy on flush,
    # so build the row through a session to assert the full defaulted dict.
    with session_scope(engine) as session:
        session.add(Workplace(id=3, name="Lager", shortname="LG"))
        session.flush()
        d = WorkplaceRepository(session).get(3).to_dict()
    assert d == {
        "ID": 3,
        "NAME": "Lager",
        "SHORTNAME": "LG",
        "POSITION": 0,
        "HIDE": False,
        "COLORTEXT": 0,
        "COLORBAR": 0,
        "COLORBK": 16777215,
    }


# ─── sync.py: DBF -> ORM upsert (with _read_dbf patched) ─────────────────────────


def _patch_dbf(monkeypatch, table_rows):
    """Patch sync._read_dbf to serve canned rows keyed by DBF table name."""
    from sp5lib.orm import sync

    def fake_read(_daten_path, table_name):
        return table_rows.get(table_name, [])

    monkeypatch.setattr(sync, "_read_dbf", fake_read)


def test_sync_shifts_insert_and_update(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {"SHIFT": [{"ID": 1, "NAME": "Früh", "SHORTNAME": "F", "POSITION": 1,
                    "DURATION0": 8.0, "STARTEND0": "08:00-16:00", "COLORBK": 100}]},
    )
    with session_scope(engine) as session:
        assert sync.sync_shifts(session, "/x") == 1
        s = ShiftRepository(session).get(1)
        assert s.name == "Früh" and s.duration0 == 8.0 and s.colorbk == 100

    # second run with changed data updates the same row (upsert, no duplicate)
    _patch_dbf(monkeypatch, {"SHIFT": [{"ID": 1, "NAME": "Frühdienst", "POSITION": 1}]})
    with session_scope(engine) as session:
        assert sync.sync_shifts(session, "/x") == 1
        repo = ShiftRepository(session)
        assert repo.get(1).name == "Frühdienst"
        assert len(repo.list(include_hidden=True)) == 1


def test_sync_leave_types_and_workplaces(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "LEAVT": [{"ID": 1, "NAME": "Urlaub", "ENTITLED": True, "STDENTIT": 30.0}],
            "WOPL": [{"ID": 1, "NAME": "Empfang", "SHORTNAME": "EMP"}],
        },
    )
    with session_scope(engine) as session:
        assert sync.sync_leave_types(session, "/x") == 1
        assert sync.sync_workplaces(session, "/x") == 1
        assert LeaveTypeRepository(session).get(1).entitled is True
        assert WorkplaceRepository(session).get(1).shortname == "EMP"


def test_sync_skips_rows_without_id(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(monkeypatch, {"WOPL": [{"ID": 0, "NAME": "ignored"}, {"NAME": "no-id"}]})
    with session_scope(engine) as session:
        assert sync.sync_workplaces(session, "/x") == 0


def test_sync_all_includes_phase2_tables(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "SHIFT": [{"ID": 1, "NAME": "Früh"}],
            "LEAVT": [{"ID": 1, "NAME": "Urlaub"}],
            "WOPL": [{"ID": 1, "NAME": "Empfang"}],
        },
    )
    stats = sync.sync_all(engine, "/x")
    assert stats["shifts"] == 1
    assert stats["leave_types"] == 1
    assert stats["workplaces"] == 1


# ─── Phase 3: schedule models / repositories ────────────────────────────────────


def test_init_db_creates_phase3_tables(engine):
    from sqlalchemy import inspect

    tables = set(inspect(engine).get_table_names())
    assert {"schedule_entries", "special_shifts", "absences"} <= tables


def test_schedule_entry_is_alias_of_shift_assignment():
    from sp5lib.orm import models, models_pg

    assert ScheduleEntry is ShiftAssignment
    assert models_pg.ScheduleEntry is ShiftAssignment
    assert models_pg.SpecialShift is models.SpecialShift
    assert models_pg.Absence is models.Absence


def test_shift_assignment_repository_date_and_employee_filter(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                ShiftAssignment(id=1, employee_id=10, date="2026-01-05", shift_id=1),
                ShiftAssignment(id=2, employee_id=10, date="2026-01-20", shift_id=2),
                ShiftAssignment(id=3, employee_id=20, date="2026-01-10", shift_id=1),
                ShiftAssignment(id=4, employee_id=10, date="2026-02-01", shift_id=1),
            ]
        )
        session.flush()
        repo = ShiftAssignmentRepository(session)

        # full range ordered by date
        assert [e.id for e in repo.list()] == [1, 3, 2, 4]
        # date window (inclusive)
        assert [e.id for e in repo.list(date_from="2026-01-01", date_to="2026-01-31")] == [1, 3, 2]
        # employee filter
        assert [e.id for e in repo.list(employee_id=10)] == [1, 2, 4]
        # combined
        assert [
            e.id
            for e in repo.list(date_from="2026-01-01", date_to="2026-01-31", employee_id=10)
        ] == [1, 2]
        assert repo.get(3).employee_id == 20
        assert repo.get(99) is None


def test_special_shift_and_absence_repositories(engine):
    with session_scope(engine) as session:
        session.add(SpecialShift(id=1, employee_id=10, date="2026-03-01", name="Extra"))
        session.add(Absence(id=1, employee_id=10, date="2026-03-02", leave_type_id=5))
        session.flush()
        assert SpecialShiftRepository(session).list(employee_id=10)[0].name == "Extra"
        assert AbsenceRepository(session).get(1).leave_type_id == 5
        assert AbsenceRepository(session).list(date_from="2026-03-02")[0].id == 1
        assert AbsenceRepository(session).list(date_to="2026-03-01") == []


def test_shift_assignment_to_dict_mirrors_dbf_keys():
    d = ShiftAssignment(id=7, employee_id=10, date="2026-01-05", shift_id=3,
                        workplace_id=2, entry_type=1).to_dict()
    assert d == {
        "ID": 7, "DATE": "2026-01-05", "EMPLOYEEID": 10,
        "SHIFTID": 3, "WORKPLACID": 2, "TYPE": 1,
    }


def test_absence_to_dict_mirrors_dbf_keys():
    d = Absence(id=3, employee_id=10, date="2026-01-05", leave_type_id=5,
                entry_type=0, interval=1, start=480, end=1020).to_dict()
    assert d["LEAVETYPID"] == 5 and d["EMPLOYEEID"] == 10
    assert d["START"] == 480 and d["END"] == 1020 and d["INTERVAL"] == 1


# ─── Phase 3: schedule sync ─────────────────────────────────────────────────────


def test_sync_shift_assignments_skips_invalid_dates(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {"MASHI": [
            {"ID": 1, "DATE": "2026-01-05", "EMPLOYEEID": 10, "SHIFTID": 1, "WORKPLACID": 2},
            {"ID": 2, "DATE": "", "EMPLOYEEID": 10, "SHIFTID": 1},          # blank date
            {"ID": 3, "DATE": "not-a-date", "EMPLOYEEID": 10, "SHIFTID": 1},  # garbage
        ]},
    )
    with session_scope(engine) as session:
        assert sync.sync_shift_assignments(session, "/x") == 1
        rows = ShiftAssignmentRepository(session).list()
        assert [r.id for r in rows] == [1]
        assert rows[0].workplace_id == 2


def test_sync_absences_leavetype_spelling_tolerance(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {"ABSEN": [
            {"ID": 1, "DATE": "2026-01-05", "EMPLOYEEID": 10, "LEAVETYPID": 5},
            {"ID": 2, "DATE": "2026-01-06", "EMPLOYEEID": 11, "LEAVETYPEID": 7},  # alt spelling
        ]},
    )
    with session_scope(engine) as session:
        assert sync.sync_absences(session, "/x") == 2
        repo = AbsenceRepository(session)
        assert repo.get(1).leave_type_id == 5
        assert repo.get(2).leave_type_id == 7


def test_sync_schedule_tolerates_dangling_references(engine, monkeypatch):
    # employee_id / shift_id are plain integers (no FK), so a row referencing a
    # non-existent employee/shift must still sync without error.
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {"SPSHI": [{"ID": 1, "DATE": "2026-01-05", "EMPLOYEEID": 9999, "SHIFTID": 8888}]},
    )
    with session_scope(engine) as session:
        assert sync.sync_special_shifts(session, "/x") == 1
        assert SpecialShiftRepository(session).get(1).employee_id == 9999


def test_sync_groups_dangling_super_id_set_null(engine, monkeypatch):
    # Regression for the v1.2.0 defect: a super_id pointing at a missing group
    # must be nulled (not raise FOREIGN KEY constraint failed and abort sync).
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {"GROUP": [
            {"ID": 1, "NAME": "Wurzel", "SUPERID": 0},
            {"ID": 2, "NAME": "Kind", "SUPERID": 61},   # group 61 does not exist
            {"ID": 3, "NAME": "Echt", "SUPERID": 1},    # valid parent
        ]},
    )
    with session_scope(engine) as session:
        assert sync.sync_groups(session, "/x") == 3
        assert session.get(Group, 2).super_id is None   # dangling -> NULL
        assert session.get(Group, 3).super_id == 1       # valid kept


def test_sync_all_includes_phase3_tables(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "MASHI": [{"ID": 1, "DATE": "2026-01-05", "EMPLOYEEID": 10, "SHIFTID": 1}],
            "SPSHI": [{"ID": 1, "DATE": "2026-01-06", "EMPLOYEEID": 10}],
            "ABSEN": [{"ID": 1, "DATE": "2026-01-07", "EMPLOYEEID": 10, "LEAVETYPID": 5}],
        },
    )
    stats = sync.sync_all(engine, "/x")
    assert stats["shift_assignments"] == 1
    assert stats["special_shifts"] == 1
    assert stats["absences"] == 1


# ─── Phase 4: holiday / period models + repositories ────────────────────────────


def test_init_db_creates_phase4_tables(engine):
    from sqlalchemy import inspect

    tables = set(inspect(engine).get_table_names())
    assert {"holidays", "periods"} <= tables


def test_holiday_reexported_from_models_pg():
    from sp5lib.orm import models, models_pg

    assert models.Holiday is models_pg.Holiday
    assert models.Period is models_pg.Period


def test_holiday_repository_year_filter(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                Holiday(id=1, date="2025-12-25", name="Weihnachten 2025"),
                Holiday(id=2, date="2026-01-01", name="Neujahr 2026"),
                Holiday(id=3, date="2026-05-01", name="Tag der Arbeit", interval=1),
            ]
        )
        session.flush()
        repo = HolidayRepository(session)
        assert [h.id for h in repo.list()] == [1, 2, 3]
        # year 2026: the two 2026 entries, plus the recurring one (id 3 already 2026)
        ids_2026 = {h.id for h in repo.list(year=2026)}
        assert ids_2026 == {2, 3}
        # recurring holiday shows up even when querying a different year
        assert 3 in {h.id for h in repo.list(year=2030)}
        assert repo.get(1).name == "Weihnachten 2025"


def test_period_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                Period(id=1, group_id=1, start="2026-01-01", end="2026-03-31", description="Q1"),
                Period(id=2, group_id=1, start="2026-04-01", end="2026-06-30", description="Q2"),
                Period(id=3, group_id=2, start="2026-01-01", end="2026-12-31", description="Jahr"),
            ]
        )
        session.flush()
        repo = PeriodRepository(session)
        assert [p.id for p in repo.list()] == [1, 3, 2]  # ordered by start, then id
        assert [p.id for p in repo.list(group_id=1)] == [1, 2]
        assert [p.id for p in repo.list(date_from="2026-04-01")] == [2]
        assert repo.get(3).description == "Jahr"


def test_holiday_and_period_to_dict_mirror_dbf_keys():
    assert Holiday(id=1, date="2026-01-01", name="Neujahr", interval=1).to_dict() == {
        "ID": 1, "DATE": "2026-01-01", "NAME": "Neujahr", "INTERVAL": 1,
    }
    d = Period(id=1, group_id=2, start="2026-01-01", end="2026-12-31",
               color=255, description="Jahr").to_dict()
    assert d == {
        "ID": 1, "GROUPID": 2, "START": "2026-01-01", "END": "2026-12-31",
        "COLOR": 255, "DESCRIPT": "Jahr",
    }


# ─── Phase 4 sync ───────────────────────────────────────────────────────────────


def test_sync_holidays_and_periods(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "HOLID": [
                {"ID": 1, "DATE": "2026-01-01", "NAME": "Neujahr", "INTERVAL": 1},
                {"ID": 2, "DATE": "", "NAME": "kaputt"},  # invalid date -> skipped
            ],
            "PERIO": [
                {"ID": 1, "GROUPID": 1, "START": "2026-01-01", "END": "2026-03-31",
                 "COLOR": 255, "DESCRIPT": "Q1"},
            ],
        },
    )
    with session_scope(engine) as session:
        assert sync.sync_holidays(session, "/x") == 1
        assert sync.sync_periods(session, "/x") == 1
        assert HolidayRepository(session).get(1).interval == 1
        # DESCRIPT (not NAME) carries the period label
        assert PeriodRepository(session).get(1).description == "Q1"


# ─── Teil A: sync_group_assignments UNIQUE / dedup / dangling fix ────────────────


def test_sync_group_assignments_non_unique_dbf_ids(engine, monkeypatch):
    # Regression for the v1.3.0 defect: 5GRASG.DBF IDs are not unique (per-group
    # running index). The sync must not use them as PK (no UNIQUE/IntegrityError),
    # must de-duplicate (employee, group) pairs, and must skip dangling refs.
    from sqlalchemy import select

    from sp5lib.orm import sync

    with session_scope(engine) as session:
        session.add_all([Employee(id=i, name=f"E{i}") for i in (40, 41, 42, 43, 44)])
        session.add_all([Group(id=i, name=f"G{i}") for i in (51, 54, 2, 55)])
        session.flush()

    _patch_dbf(
        monkeypatch,
        {"GRASG": [
            {"ID": 1, "EMPLOYEEID": 40, "GROUPID": 51},
            {"ID": 2, "EMPLOYEEID": 41, "GROUPID": 54},
            {"ID": 2, "EMPLOYEEID": 42, "GROUPID": 54},   # repeated DBF ID, distinct pair
            {"ID": 1, "EMPLOYEEID": 44, "GROUPID": 2},    # repeated DBF ID, distinct pair
            {"ID": 3, "EMPLOYEEID": 43, "GROUPID": 55},
            {"ID": 9, "EMPLOYEEID": 40, "GROUPID": 51},   # duplicate pair -> deduped
            {"ID": 10, "EMPLOYEEID": 999, "GROUPID": 51},  # dangling employee -> skipped
        ]},
    )
    with session_scope(engine) as session:
        # must not raise IntegrityError; returns the count of unique valid pairs
        assert sync.sync_group_assignments(session, "/x") == 5
        rows = list(session.scalars(select(GroupAssignment)).all())
        pairs = {(r.employee_id, r.group_id) for r in rows}
        assert pairs == {(40, 51), (41, 54), (42, 54), (44, 2), (43, 55)}
        # PKs are autoincrement (not the non-unique DBF IDs)
        assert len({r.id for r in rows}) == 5


def test_sync_all_includes_phase4_tables(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "HOLID": [{"ID": 1, "DATE": "2026-01-01", "NAME": "Neujahr"}],
            "PERIO": [{"ID": 1, "GROUPID": 1, "START": "2026-01-01", "END": "2026-12-31",
                       "DESCRIPT": "Jahr"}],
        },
    )
    stats = sync.sync_all(engine, "/x")
    assert stats["holidays"] == 1
    assert stats["periods"] == 1


# ─── Phase 5: bookings / overtime / leave entitlements ──────────────────────────


def test_init_db_creates_phase5_tables(engine):
    from sqlalchemy import inspect

    tables = set(inspect(engine).get_table_names())
    assert {"bookings_pg", "overtime_records", "leave_entitlements"} <= tables


def test_phase5_legacy_aliases():
    from sp5lib.orm import Booking, OvertimeRecord, models_pg

    assert Booking is AccountBooking
    assert OvertimeRecord is OvertimeEntry
    assert models_pg.Booking is AccountBooking
    assert models_pg.OvertimeRecord is OvertimeEntry
    assert models_pg.LeaveEntitlement is LeaveEntitlement


def test_account_booking_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                AccountBooking(id=1, employee_id=10, date="2026-01-05", value=2.5),
                AccountBooking(id=2, employee_id=10, date="2026-02-10", value=-1.0),
                AccountBooking(id=3, employee_id=20, date="2026-01-20", value=8.0),
            ]
        )
        session.flush()
        repo = AccountBookingRepository(session)
        assert [b.id for b in repo.list()] == [1, 3, 2]  # by date
        assert [b.id for b in repo.list(employee_id=10)] == [1, 2]
        assert [b.id for b in repo.list(date_from="2026-01-01", date_to="2026-01-31")] == [1, 3]
        assert repo.get(3).value == 8.0
        assert repo.get(99) is None


def test_overtime_entry_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                OvertimeEntry(id=1, employee_id=10, date="2026-01-31", hours=5.0),
                OvertimeEntry(id=2, employee_id=11, date="2026-02-28", hours=-2.0),
            ]
        )
        session.flush()
        repo = OvertimeEntryRepository(session)
        assert [o.id for o in repo.list(employee_id=10)] == [1]
        assert [o.id for o in repo.list(date_from="2026-02-01")] == [2]
        assert repo.get(1).hours == 5.0


def test_leave_entitlement_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                LeaveEntitlement(id=1, employee_id=10, year=2025, leave_type_id=1, entitlement=30.0),
                LeaveEntitlement(id=2, employee_id=10, year=2026, leave_type_id=1, entitlement=32.0),
                LeaveEntitlement(id=3, employee_id=20, year=2026, leave_type_id=1, entitlement=28.0),
            ]
        )
        session.flush()
        repo = LeaveEntitlementRepository(session)
        assert [e.id for e in repo.list(year=2026)] == [2, 3]
        assert [e.id for e in repo.list(employee_id=10)] == [1, 2]
        assert [e.id for e in repo.list(year=2026, employee_id=10)] == [2]
        assert repo.get(1).entitlement == 30.0


def test_phase5_to_dict_mirror_dbf_keys():
    assert AccountBooking(id=1, employee_id=10, date="2026-01-05", booking_type=2,
                          value=2.5, note="x").to_dict() == {
        "ID": 1, "EMPLOYEEID": 10, "DATE": "2026-01-05", "TYPE": 2, "VALUE": 2.5, "NOTE": "x",
    }
    assert OvertimeEntry(id=1, employee_id=10, date="2026-01-31", hours=5.0).to_dict() == {
        "ID": 1, "EMPLOYEEID": 10, "DATE": "2026-01-31", "HOURS": 5.0,
    }
    d = LeaveEntitlement(id=1, employee_id=10, year=2026, leave_type_id=1,
                         entitlement=30.0, carry_forward=2.0, in_days=True).to_dict()
    assert d == {
        "ID": 1, "EMPLOYEEID": 10, "YEAR": 2026, "LEAVETYPID": 1,
        "ENTITLEMNT": 30.0, "REST": 2.0, "INDAYS": True,
    }


def test_sync_book_overtime_entitlements(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "BOOK": [
                {"ID": 1, "EMPLOYEEID": 10, "DATE": "2026-01-05", "TYPE": 1,
                 "VALUE": 2.5, "NOTE": "Gutschrift"},
                {"ID": 2, "EMPLOYEEID": 10, "DATE": "", "VALUE": 1.0},  # invalid date -> skip
            ],
            "OVER": [
                {"ID": 1, "EMPLOYEEID": 10, "DATE": "2026-01-31", "HOURS": 5.0},
            ],
            "LEAEN": [
                {"ID": 1, "EMPLOYEEID": 10, "YEAR": 2026, "LEAVETYPID": 1,
                 "ENTITLEMNT": 30.0, "REST": 2.5, "INDAYS": True},
            ],
        },
    )
    with session_scope(engine) as session:
        assert sync.sync_book(session, "/x") == 1            # one skipped (blank date)
        assert sync.sync_overtime(session, "/x") == 1
        assert sync.sync_leave_entitlements(session, "/x") == 1
        assert AccountBookingRepository(session).get(1).note == "Gutschrift"
        assert OvertimeEntryRepository(session).get(1).hours == 5.0
        le = LeaveEntitlementRepository(session).get(1)
        assert le.entitlement == 30.0 and le.carry_forward == 2.5 and le.in_days is True


def test_sync_all_includes_phase5_tables(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "BOOK": [{"ID": 1, "EMPLOYEEID": 10, "DATE": "2026-01-05", "VALUE": 2.5}],
            "OVER": [{"ID": 1, "EMPLOYEEID": 10, "DATE": "2026-01-31", "HOURS": 5.0}],
            "LEAEN": [{"ID": 1, "EMPLOYEEID": 10, "YEAR": 2026, "ENTITLEMNT": 30.0}],
        },
    )
    stats = sync.sync_all(engine, "/x")
    assert stats["bookings"] == 1
    assert stats["overtime"] == 1
    assert stats["leave_entitlements"] == 1


# ─── Phase 6: demand / cycles / restrictions ────────────────────────────────────


def test_init_db_creates_phase6_tables(engine):
    from sqlalchemy import inspect

    tables = set(inspect(engine).get_table_names())
    assert {
        "staffing_requirements", "special_demands", "cycles",
        "cycle_assignments", "restrictions",
    } <= tables


def test_phase6_aliases_and_reexport():
    from sp5lib.orm import StaffingRequirement, models, models_pg

    assert StaffingRequirement is ShiftDemand
    assert models_pg.StaffingRequirement is ShiftDemand
    assert models_pg.Cycle is models.Cycle is Cycle
    assert models_pg.CycleAssignment is CycleAssignment
    assert models_pg.Restriction is Restriction


def test_shift_demand_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                ShiftDemand(id=1, shift_id=1, weekday=1, group_id=5, min_staff=2, max_staff=4),
                ShiftDemand(id=2, shift_id=2, weekday=1, group_id=5, min_staff=1, max_staff=2),
                ShiftDemand(id=3, shift_id=1, weekday=2, group_id=5, min_staff=3, max_staff=3),
            ]
        )
        session.flush()
        repo = ShiftDemandRepository(session)
        assert [d.id for d in repo.list(shift_id=1)] == [1, 3]
        assert [d.id for d in repo.list(weekday=1)] == [1, 2]
        assert [d.id for d in repo.list(shift_id=1, weekday=2)] == [3]
        assert repo.get(2).max_staff == 2


def test_special_demand_repository(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                SpecialDemand(id=1, date="2026-01-05", shift_id=1, min_staff=2, max_staff=3),
                SpecialDemand(id=2, date="2026-02-10", shift_id=2, min_staff=1, max_staff=1),
            ]
        )
        session.flush()
        repo = SpecialDemandRepository(session)
        assert [d.id for d in repo.list(date_from="2026-02-01")] == [2]
        assert [d.id for d in repo.list(shift_id=1)] == [1]
        assert repo.get(1).max_staff == 3


def test_cycle_repository_hide_filter(engine):
    with session_scope(engine) as session:
        session.add_all(
            [
                Cycle(id=1, name="Rotation A", position=1),
                Cycle(id=2, name="Alt", position=2, hide=True),
            ]
        )
        session.flush()
        repo = CycleRepository(session)
        assert [c.id for c in repo.list()] == [1]
        assert [c.id for c in repo.list(include_hidden=True)] == [1, 2]
        assert repo.get(1).name == "Rotation A"


def test_cycle_assignment_and_restriction_repositories(engine):
    with session_scope(engine) as session:
        session.add(CycleAssignment(id=1, employee_id=10, cycle_id=1, start="2026-01-01"))
        session.add(CycleAssignment(id=2, employee_id=11, cycle_id=1, start="2026-01-01"))
        session.add(Restriction(id=1, employee_id=10, shift_id=3, weekday=1, reason="kein Nachtdienst"))
        session.flush()
        ca_repo = CycleAssignmentRepository(session)
        assert [a.id for a in ca_repo.list(cycle_id=1)] == [1, 2]
        assert [a.id for a in ca_repo.list(employee_id=11)] == [2]
        r_repo = RestrictionRepository(session)
        assert r_repo.list(employee_id=10)[0].reason == "kein Nachtdienst"
        assert r_repo.list(shift_id=3)[0].id == 1


def test_phase6_to_dict_mirror_dbf_keys():
    assert ShiftDemand(id=1, group_id=5, weekday=1, shift_id=2, workplace_id=0,
                       min_staff=2, max_staff=4).to_dict() == {
        "ID": 1, "GROUPID": 5, "WEEKDAY": 1, "SHIFTID": 2, "WORKPLACID": 0, "MIN": 2, "MAX": 4,
    }
    assert SpecialDemand(id=1, group_id=0, date="2026-01-05", shift_id=2, workplace_id=0,
                         min_staff=1, max_staff=3).to_dict() == {
        "ID": 1, "GROUPID": 0, "DATE": "2026-01-05", "SHIFTID": 2, "WORKPLACID": 0,
        "MIN": 1, "MAX": 3,
    }
    assert Restriction(id=1, employee_id=10, shift_id=3, weekday=1, restrict=1,
                       reason="x").to_dict() == {
        "ID": 1, "EMPLOYEEID": 10, "SHIFTID": 3, "WEEKDAY": 1, "RESTRICT": 1, "RESERVED": "x",
    }


def test_sync_phase6_tables(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "SHDEM": [{"ID": 1, "GROUPID": 5, "WEEKDAY": 1, "SHIFTID": 2, "MIN": 2, "MAX": 4}],
            "SPDEM": [
                {"ID": 1, "GROUPID": 0, "DATE": "2026-01-05", "SHIFTID": 2, "MIN": 1, "MAX": 3},
                {"ID": 2, "DATE": "", "SHIFTID": 2},  # invalid date -> skip
            ],
            "CYCLE": [{"ID": 1, "NAME": "Rotation A", "SIZE": 14, "UNIT": 1}],
            "RESTR": [{"ID": 1, "EMPLOYEEID": 10, "SHIFTID": 3, "WEEKDAY": 1,
                       "RESTRICT": 1, "RESERVED": "kein Nachtdienst"}],
        },
    )
    with session_scope(engine) as session:
        assert sync.sync_shift_demand(session, "/x") == 1
        assert sync.sync_special_demand(session, "/x") == 1   # one skipped
        assert sync.sync_cycles(session, "/x") == 1
        assert sync.sync_restrictions(session, "/x") == 1
        assert ShiftDemandRepository(session).get(1).min_staff == 2
        assert CycleRepository(session).get(1).size == 14
        assert RestrictionRepository(session).get(1).reason == "kein Nachtdienst"


def test_sync_cycle_assignments_non_unique_ids_and_dedup(engine, monkeypatch):
    # Like 5GRASG: the DBF ID may repeat. Use autoincrement PK (no IntegrityError)
    # and de-duplicate (employee_id, cycle_id, start).
    from sqlalchemy import select

    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {"CYASS": [
            {"ID": 1, "EMPLOYEEID": 10, "CYCLEID": 1, "START": "2026-01-01"},
            {"ID": 1, "EMPLOYEEID": 11, "CYCLEID": 1, "START": "2026-01-01"},  # repeated ID
            {"ID": 2, "EMPLOYEEID": 10, "CYCLEID": 1, "START": "2026-01-01"},  # duplicate pair
            {"ID": 3, "EMPLOYEEID": 0, "CYCLEID": 1, "START": "2026-01-01"},   # no employee -> skip
        ]},
    )
    with session_scope(engine) as session:
        assert sync.sync_cycle_assignments(session, "/x") == 2  # (10,1,..) and (11,1,..)
        rows = list(session.scalars(select(CycleAssignment)).all())
        assert {(r.employee_id, r.cycle_id) for r in rows} == {(10, 1), (11, 1)}
        assert len({r.id for r in rows}) == 2  # autoincrement PKs


def test_sync_all_includes_phase6_and_covers_19_tables(engine, monkeypatch):
    from sp5lib.orm import sync

    _patch_dbf(
        monkeypatch,
        {
            "SHDEM": [{"ID": 1, "SHIFTID": 1, "WEEKDAY": 1, "MIN": 1, "MAX": 2}],
            "SPDEM": [{"ID": 1, "DATE": "2026-01-05", "SHIFTID": 1, "MIN": 1, "MAX": 2}],
            "CYCLE": [{"ID": 1, "NAME": "A"}],
            "CYASS": [{"ID": 1, "EMPLOYEEID": 10, "CYCLEID": 1, "START": "2026-01-01"}],
            "RESTR": [{"ID": 1, "EMPLOYEEID": 10, "SHIFTID": 1}],
        },
    )
    stats = sync.sync_all(engine, "/x")
    assert stats["shift_demand"] == 1
    assert stats["special_demand"] == 1
    assert stats["cycles"] == 1
    assert stats["cycle_assignments"] == 1
    assert stats["restrictions"] == 1
    # full read-mirror coverage: 19 logical tables
    assert len(stats) == 19
