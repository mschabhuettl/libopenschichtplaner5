# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2026-05-27

ORM Phase 5 — account bookings, overtime and leave entitlements (the data
behind the time-account / overtime / leave-balance features). Additive and
backward compatible.

### Added

- **`AccountBooking`** (`bookings_pg`, from `5BOOK.DBF`), **`OvertimeEntry`**
  (`overtime_records`, `5OVER.DBF`) and **`LeaveEntitlement`**
  (`leave_entitlements`, `5LEAEN.DBF`) ORM models, importable from
  `sp5lib.orm`, with `to_dict()` mirroring the real DBF keys. `init_db()`
  creates the tables.
- **`AccountBookingRepository`** and **`OvertimeEntryRepository`** with
  `list(date_from=None, date_to=None, employee_id=None)` + `get(id)`;
  **`LeaveEntitlementRepository`** with `list(year=None, employee_id=None)` +
  `get(id)`.
- DBF → ORM upsert `sync.sync_book`, `sync.sync_overtime`,
  `sync.sync_leave_entitlements`, wired into `sync.sync_all()`. `BOOK`/`OVER`
  rows with a blank/invalid `DATE` are skipped and logged. `sync_all()` now
  covers 14 tables.

### Changed

- `AccountBooking` / `OvertimeEntry` / `LeaveEntitlement` are defined
  canonically in `sp5lib.orm.models` and re-exported from
  `sp5lib.orm.models_pg`. The previous names **`Booking`** (→ `AccountBooking`)
  and **`OvertimeRecord`** (→ `OvertimeEntry`) remain importable as aliases
  (same tables `bookings_pg` / `overtime_records`), so existing imports keep
  working.

### Notes

- `LeaveEntitlement` maps the DBF fields `ENTITLEMNT` → `entitlement`,
  `REST` → `carry_forward`, `INDAYS` → `in_days`; `to_dict()` mirrors the DBF
  spellings (`ENTITLEMNT` / `REST` / `INDAYS`).

## [1.4.0] - 2026-05-27

ORM Phase 4 — reference tables (holidays, accounting periods) plus a sync
robustness fix that lets `sync_all()` run to completion on real data.

### Added

- **`Holiday`** (`holidays`, from `5HOLID.DBF`) and **`Period`** (`periods`,
  from `5PERIO.DBF`) ORM models, importable from `sp5lib.orm`, with `to_dict()`
  mirroring the real DBF keys. `init_db()` creates the tables.
- **`HolidayRepository`** with `list(year=None)` (a given year plus recurring
  `interval == 1` holidays) and `get(id)`.
- **`PeriodRepository`** with `list(date_from=None, date_to=None,
  group_id=None)` and `get(id)`.
- DBF → ORM upsert `sync.sync_holidays` and `sync.sync_periods`, wired into
  `sync.sync_all()`.

### Fixed

- `sync.sync_group_assignments` no longer aborts `sync_all` with
  `UNIQUE constraint failed: group_assignments.id`. The `ID` column in
  `5GRASG.DBF` is a per-group running index, not a global key, so it is no
  longer used as the primary key (the autoincrement `id` is). Assignments are
  de-duplicated on `(employee_id, group_id)`, and rows referencing a
  non-existent employee or group are skipped and logged. `sync_all()` now
  completes over the full set of tables.

### Changed

- `Holiday` is now defined canonically in `sp5lib.orm.models` and re-exported
  from `sp5lib.orm.models_pg` (used by `pg_database`); `Period` is likewise
  available from both. No behaviour change for existing imports.

### Notes

- `Period` maps the DBF `DESCRIPT` field (the period label) — the request
  referred to it as `NAME`, but the actual `5PERIO.DBF` field is `DESCRIPT`,
  which `to_dict()` mirrors (alongside `GROUPID` / `START` / `END` / `COLOR`).

## [1.3.0] - 2026-05-27

ORM Phase 3 — the time-based roster. Adds the schedule-entry tables to the
SQLAlchemy layer plus a sync robustness fix. Additive and backward compatible.

### Added

- **`ShiftAssignment`** (`schedule_entries`, from `5MASHI.DBF`),
  **`SpecialShift`** (`special_shifts`, `5SPSHI.DBF`) and **`Absence`**
  (`absences`, `5ABSEN.DBF`) ORM models, importable from `sp5lib.orm`, each with
  a `to_dict()` mirroring the DBF keys (`DATE` / `EMPLOYEEID` / `SHIFTID` /
  `LEAVETYPID` / …). `init_db()` creates the tables.
- **`ShiftAssignmentRepository`**, **`SpecialShiftRepository`**,
  **`AbsenceRepository`** with `list(date_from=None, date_to=None,
  employee_id=None)` (date-window + per-employee filtering) and `get(id)`.
- DBF → ORM upsert `sync.sync_shift_assignments`, `sync.sync_special_shifts`,
  `sync.sync_absences`, wired into `sync.sync_all()`. Blank/invalid `DATE`
  values are skipped and logged; references (employee/shift/leave-type) are
  plain indexed integers with no DB-level FK, so dirty legacy data syncs.

### Fixed

- `sync.sync_groups` no longer aborts `sync_all` with
  `FOREIGN KEY constraint failed` when `5GROUP.DBF` contains a `super_id` that
  points to a non-existent group. Dangling parent references are now resolved
  in a second pass: unknown references are set to `NULL` and logged. This also
  makes group ordering in the DBF irrelevant.

### Changed

- `ShiftAssignment`, `SpecialShift` and `Absence` are defined canonically in
  `sp5lib.orm.models` and re-exported from `sp5lib.orm.models_pg`. The former
  `ScheduleEntry` name (MASHI) remains importable as an alias of
  `ShiftAssignment`, so existing
  `from sp5lib.orm.models_pg import ScheduleEntry, SpecialShift, Absence`
  imports (used by `pg_database`) keep working unchanged.

## [1.2.0] - 2026-05-27

ORM Phase 2 — adds the next three core entities to the SQLAlchemy layer
(`sp5lib.orm`), mirroring the Phase 1 Employee/Group patterns. Additive and
backward compatible.

### Added

- **`Shift`** (`shifts`, from `5SHIFT.DBF`), **`LeaveType`** (`leave_types`,
  `5LEAVT.DBF`) and **`Workplace`** (`workplaces`, `5WOPL.DBF`) ORM models,
  importable directly from `sp5lib.orm`. `init_db()` creates the tables.
- **`ShiftRepository`**, **`LeaveTypeRepository`**, **`WorkplaceRepository`**
  with `list(include_hidden=False)` and `get(id)`.
- DBF → ORM upsert for the three tables via `sync.sync_shifts`,
  `sync.sync_leave_types`, `sync.sync_workplaces`; `sync.sync_all()` now also
  returns `shifts` / `leave_types` / `workplaces` counts.
- ORM unit tests (`tests/test_orm.py`, in-memory SQLite) covering models,
  repositories, `to_dict()` and the sync upsert.

### Changed

- `Shift`, `LeaveType` and `Workplace` are now defined canonically in
  `sp5lib.orm.models` and **re-exported** from `sp5lib.orm.models_pg` (single
  source of truth, identical `to_dict()`). Existing
  `from sp5lib.orm.models_pg import Shift, LeaveType, Workplace` imports keep
  working unchanged.

## [1.1.0] - 2026-05-26

Initial standalone release of **libopenschichtplaner5** (import name `sp5lib`).

This is the first release of the library as an independent, pip-installable
package. The code was extracted — **with its full git history** — from the
`backend/sp5lib/` directory of
[OpenSchichtplaner5](https://github.com/mschabhuettl/openschichtplaner5).
The `1.1.0` version preserves the version line the library carried inside that
project, so there is no regression for existing consumers; OpenSchichtplaner5
continues to import it unchanged as `sp5lib`.

### Added

- Packaging as the `libopenschichtplaner5` distribution (importable as `sp5lib`),
  publishable to PyPI with an sdist and a pure-Python wheel.
- `sp5lib.dbf_reader` — pure-Python DBF reader (UTF-16-LE detection, date
  parsing, field decoding) for the original Schichtplaner5 FoxPro/dBASE files.
- `sp5lib.dbf_writer` — safe DBF writer with exclusive `flock`, TOCTOU-safe
  record counting, rollback, and EOF-marker preservation.
- `sp5lib.database` — high-level `SP5Database` facade over the DBF tables
  (employees, shifts, schedule, absences, authentication, 2FA, …).
- `sp5lib.db_factory`, `sp5lib.sqlite_adapter`, `sp5lib.pg_database` — optional
  SQLite and PostgreSQL backends.
- `sp5lib.orm` — SQLAlchemy models (SQLite `models.py`, PostgreSQL
  `models_pg.py`), `repository`, and `sync`.
- `sp5lib.auto_migrate` — Alembic-based automatic migrations.
- `sp5lib.email_service` — SMTP notification emails with HTML-escaped templates.
- `sp5lib.color_utils` — FoxPro BGR ↔ hex/RGB color helpers.
- `py.typed` marker so type checkers consume the bundled type hints.
- `postgres` extra (`psycopg2-binary`) for the optional PostgreSQL backend.
- Continuous integration running ruff and pytest on Python 3.10–3.12.
- Release workflow publishing to PyPI via Trusted Publishing on `v*` tags.

### Notes

- Runtime dependencies: `SQLAlchemy`, `alembic`, `bcrypt`, `pyotp`, `packaging`.
- Requires Python 3.10 or newer.
- Licensed under the MIT License.

[1.1.0]: https://github.com/mschabhuettl/libopenschichtplaner5/releases/tag/v1.1.0
