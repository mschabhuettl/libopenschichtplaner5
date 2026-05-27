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
    Group,
    LeaveType,
    LeaveTypeRepository,
    ScheduleEntry,
    Shift,
    ShiftAssignment,
    ShiftAssignmentRepository,
    ShiftRepository,
    SpecialShift,
    SpecialShiftRepository,
    Workplace,
    WorkplaceRepository,
    get_engine,
    init_db,
)
from sp5lib.orm.base import session_scope


@pytest.fixture
def engine():
    """Fresh in-memory SQLite engine with all ORM tables created."""
    eng = get_engine("sqlite:///:memory:")
    init_db(eng)
    return eng


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
