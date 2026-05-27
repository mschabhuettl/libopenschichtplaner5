"""Tests for the Phase-2 ORM models, repositories and DBF sync.

These run entirely against an in-memory SQLite database (no external
fixtures), mirroring the self-contained style of test_dbf.py. They cover the
Shift / LeaveType / Workplace models added in Phase 2: schema creation via
init_db, repository list/get semantics, to_dict() shape, and the DBF -> ORM
upsert in sync.py (with _read_dbf patched to canned rows).
"""

import pytest

from sp5lib.orm import (
    LeaveType,
    LeaveTypeRepository,
    Shift,
    ShiftRepository,
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
