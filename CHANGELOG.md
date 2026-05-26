# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
