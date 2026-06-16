# libopenschichtplaner5

[![CI](https://github.com/mschabhuettl/libopenschichtplaner5/actions/workflows/ci.yml/badge.svg)](https://github.com/mschabhuettl/libopenschichtplaner5/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/libopenschichtplaner5?logo=pypi&logoColor=white)](https://pypi.org/project/libopenschichtplaner5/)
[![ghcr.io](https://img.shields.io/badge/ghcr.io-image-2496ED?logo=docker&logoColor=white)](https://github.com/mschabhuettl/libopenschichtplaner5/pkgs/container/libopenschichtplaner5)
[![GitHub release](https://img.shields.io/github/v/release/mschabhuettl/libopenschichtplaner5?logo=github)](https://github.com/mschabhuettl/libopenschichtplaner5/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The core library behind [**OpenSchichtplaner5**](https://github.com/mschabhuettl/openschichtplaner5) —
a pip-installable package that reads **and writes** the original *Schichtplaner5*
FoxPro/dBASE `.DBF` database files, so an open replacement can run on the exact same
data as the proprietary Windows tool, with no migration.

> **Import name:** the distribution is `libopenschichtplaner5`, but the importable
> package keeps its historical name **`sp5lib`** (like `pip install PyYAML` → `import yaml`).
> This keeps OpenSchichtplaner5's imports unchanged after the extraction.

**📚 Full documentation lives in the [project wiki](https://github.com/mschabhuettl/libopenschichtplaner5/wiki)** —
DBF format notes, calculation features, the ORM guide and development setup.
For a quick start see [Installation](#installation) and the [CLI](#cli--docker) below;
the in-repo module map is in [docs/architecture.md](docs/architecture.md).

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

Releases are published to [PyPI](https://pypi.org/project/libopenschichtplaner5/):

```bash
pip install libopenschichtplaner5
# with the optional PostgreSQL backend:
pip install "libopenschichtplaner5[postgres]"
```

For the latest development state straight from Git:

```bash
pip install "libopenschichtplaner5 @ git+https://github.com/mschabhuettl/libopenschichtplaner5.git"
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

## CLI & Docker

The package installs an `sp5lib` command for standalone work on a Schichtplaner5
database directory (the folder containing the `5*.DBF` files):

```bash
sp5lib info     /path/to/SP5/Daten                  # table overview: records per table, SP5 build
sp5lib dump     /path/to/SP5/Daten 5EMPL --limit 5  # table contents as JSON (default) or --csv
sp5lib validate /path/to/SP5/Daten                  # read all tables, report errors/encoding issues
sp5lib sync     /path/to/SP5/Daten --target sqlite:/tmp/sp5.db        # DBF → SQLite
sp5lib sync     /path/to/SP5/Daten --target postgres://user:pw@host/db  # DBF → PostgreSQL
```

The same CLI is the default stage of the Dockerfile (slim, non-root,
`ENTRYPOINT ["sp5lib"]`) — no local Python required. Each release publishes a
multi-arch image to the GitHub Container Registry, so no local build is needed:

```bash
# published image (multi-arch amd64+arm64, no build required):
docker run --rm -v /path/to/SP5/Daten:/data:ro \
  ghcr.io/mschabhuettl/libopenschichtplaner5:latest info /data   # or :1.12.0

# or build it locally:
docker build -t libopenschichtplaner5 .
docker run --rm -v /path/to/SP5/Daten:/data:ro libopenschichtplaner5 info /data
docker run --rm -v /path/to/SP5/Daten:/data:ro libopenschichtplaner5 dump /data 5EMPL --limit 5
docker run --rm -v /path/to/SP5/Daten:/data -v "$PWD":/out \
  libopenschichtplaner5 sync /data --target sqlite:/out/sp5.db

# or via compose (service "tools"):
SP5_DB_DIR=/path/to/SP5/Daten docker compose run --rm tools info /data
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

### Docker (Build-/Test-Stage)

Stage `test` of the Dockerfile provides a reproducible lint + test environment
(`python:3.12-slim`, runs `ruff check .` and `pytest`):

```bash
docker compose run --rm test
# equivalent: docker build --target test -t libopenschichtplaner5:test . && docker run --rm libopenschichtplaner5:test
```

## License

MIT — see [LICENSE](LICENSE). Extracted (with full git history) from
[OpenSchichtplaner5](https://github.com/mschabhuettl/openschichtplaner5)'s `backend/sp5lib/`.
