# libopenschichtplaner5

[![CI](https://github.com/mschabhuettl/libopenschichtplaner5/actions/workflows/ci.yml/badge.svg)](https://github.com/mschabhuettl/libopenschichtplaner5/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The core library behind [**OpenSchichtplaner5**](https://github.com/mschabhuettl/openschichtplaner5) —
a pip-installable package that reads **and writes** the original *Schichtplaner5*
FoxPro/dBASE `.DBF` database files, so an open replacement can run on the exact same
data as the proprietary Windows tool, with no migration.

> **Import name:** the distribution is `libopenschichtplaner5`, but the importable
> package keeps its historical name **`sp5lib`** (like `pip install PyYAML` → `import yaml`).
> This keeps OpenSchichtplaner5's imports unchanged after the extraction.

## What's inside

| Module | Purpose |
|---|---|
| `sp5lib.dbf_reader` | Pure-Python DBF reader (UTF-16-LE detection, date parsing, field decode) |
| `sp5lib.dbf_writer` | Safe DBF writer — exclusive `flock`, TOCTOU-safe record count, rollback, EOF marker preservation; interop with a running original client via the `-L` change journal and CDX invalidation (stale `.CDX` of a modified table is deleted so the original rebuilds it) |
| `sp5lib.calculations` | Central, side-effect-free calculation layer implementing the original's rules (spec chapter 3): nominal/actual hours, absence crediting, leave accounts, surcharges, demand/utilization |
| `sp5lib.database` | High-level `SP5Database` facade over the DBF tables (employees, shifts, schedule, absences, statistics via `sp5lib.calculations`, auth, 2FA …) |
| `sp5lib.db_factory` / `sqlite_adapter` / `pg_database` | Optional SQLite / PostgreSQL backends; `pg_database` shares the calculation layer with the DBF facade (equivalence-tested) |
| `sp5lib.orm` | SQLAlchemy models (`models.py` SQLite, `models_pg.py` Postgres), `repository`, `sync` |
| `sp5lib.auto_migrate` | Alembic-based automatic migrations |
| `sp5lib.email_service` | SMTP notification emails (HTML-escaped templates) |
| `sp5lib.color_utils` | FoxPro BGR ↔ hex/RGB color helpers |

See [docs/architecture.md](docs/architecture.md) for the full module map and wiring.

## Installation

```bash
pip install "libopenschichtplaner5 @ git+https://github.com/mschabhuettl/libopenschichtplaner5.git"
# with the optional PostgreSQL backend:
pip install "libopenschichtplaner5[postgres] @ git+https://github.com/mschabhuettl/libopenschichtplaner5.git"
```

## Usage

```python
from sp5lib.database import SP5Database

db = SP5Database("/path/to/SP5/Daten")        # directory of .DBF files
for emp in db.get_employees():
    print(emp["ID"], emp["NAME"], emp["FIRSTNAME"])
```

Low-level DBF access:

```python
from sp5lib.dbf_reader import read_dbf
from sp5lib.dbf_writer import append_record, get_table_fields

rows   = read_dbf("/path/to/SP5/Daten/5EMPL.DBF")
fields = get_table_fields("/path/to/SP5/Daten/5EMPL.DBF")
append_record("/path/to/SP5/Daten/5NOTE.DBF", fields, {"ID": 1, "TEXT": "hello"})
```

## Dependencies

Runtime: `SQLAlchemy`, `alembic`, `bcrypt`, `pyotp`, `packaging`.
Optional: `psycopg2-binary` (via the `postgres` extra) for the PostgreSQL backend.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev,postgres]"
pytest
ruff check .
```

Optional golden regression suite against the original Schichtplaner 5 sample
database (local reference material, never committed):

```bash
SP5_GOLDEN_DB=/path/to/sp5/Daten pytest tests/test_golden_sample_db.py -v
```

### Docker (Build-/Test-Image)

The library ships no runtime service — the Dockerfile only provides a
reproducible lint + test environment (`python:3.12-slim`, stage `test` runs
`ruff check .` and `pytest`):

```bash
docker compose run --rm test
# equivalent: docker build --target test -t libopenschichtplaner5:test . && docker run --rm libopenschichtplaner5:test
```

## License

MIT — see [LICENSE](LICENSE). Extracted (with full git history) from
[OpenSchichtplaner5](https://github.com/mschabhuettl/openschichtplaner5)'s `backend/sp5lib/`.
