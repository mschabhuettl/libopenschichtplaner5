# SQLAlchemy ORM Layer — Migration Guide

## Overview

This package (`sp5lib/orm/`) introduces SQLAlchemy as a database-agnostic ORM
abstraction for OpenSchichtplaner5. It coexists with the legacy DBF-based data
layer and serves as a foundation for a future database migration (SQLite → PostgreSQL).

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  API Routers                      │
│           (FastAPI endpoints)                     │
├──────────────────────────────────────────────────┤
│                                                   │
│  ┌─────────────────┐   ┌──────────────────────┐  │
│  │  SP5Database     │   │  ORM Repositories    │  │
│  │  (database.py)   │   │  (repository.py)     │  │
│  │  ── Legacy ──    │   │  ── New ──           │  │
│  │  DBF files       │   │  SQLAlchemy models   │  │
│  └────────┬────────┘   └──────────┬───────────┘  │
│           │                       │               │
│    ┌──────▼──────┐    ┌──────────▼──────────┐    │
│    │  DBF Reader  │    │  SQLAlchemy Engine   │    │
│    │  DBF Writer  │    │  (SQLite / PG)       │    │
│    └─────────────┘    └─────────────────────┘    │
└──────────────────────────────────────────────────┘
```

## Package Structure

```
sp5lib/orm/
├── __init__.py       # Public API: get_engine, get_session, init_db
├── base.py           # Engine factory, session management, Base class
├── models.py         # ORM models: core + Shift/LeaveType/Workplace + ShiftAssignment/SpecialShift/Absence
├── repository.py     # Repositories for all of the above (master-data + schedule)
├── sync.py           # DBF → ORM sync utilities
└── README.md         # This file
```

## Key Design Decisions

### 1. Database-Agnostic via SQLAlchemy
All queries use the SQLAlchemy ORM API (no raw SQL). Switching from SQLite to
PostgreSQL requires only changing the connection URL:

```python
# Development (SQLite)
engine = get_engine("sqlite:///sp5.db")

# Production (PostgreSQL)
engine = get_engine("postgresql://user:pass@localhost:5432/sp5")
```

### 2. Repository Pattern
Data access is encapsulated in repository classes (`EmployeeRepository`,
`GroupRepository`). This provides:
- Clean separation between business logic and data access
- Easy unit testing with in-memory SQLite
- A single place to change when queries need optimization

### 3. Coexistence with DBF Layer
The ORM layer is **purely additive** — it does not modify or replace `database.py`.
Both layers can run in parallel during migration:
- DBF layer: remains the source of truth for the Windows desktop client
- ORM layer: serves the web frontend (once migrated)

### 4. DBF → ORM Sync
The `sync.py` module reads DBF files and upserts into the ORM database. This
enables a gradual migration where DBF data is periodically synced to the SQL
database.

## Proof-of-Concept Scope

This initial implementation covers:
- **Employee** model with all relevant fields
- **Group** model with hierarchical parent references
- **GroupAssignment** model (many-to-many: employees ↔ groups)
- Repository pattern with CRUD + search + member management
- 36 unit tests covering models, repositories, sessions, cascades

## Migration Roadmap

1. ✅ **Phase 1**: ORM models + repositories for Employees & Groups
2. ✅ **Phase 2**: ORM models + repositories + DBF sync for Shifts, LeaveTypes, Workplaces
3. ✅ **Phase 3**: ORM models + repositories + DBF sync for Schedule entries (MASHI → ShiftAssignment, SPSHI → SpecialShift, ABSEN → Absence)
4. ✅ **Phase 4**: ORM models + repositories + DBF sync for reference tables (5HOLID → Holiday, 5PERIO → Period); `sync_all()` now runs over all tables
5. **Next**: Wire ORM repositories into FastAPI routers (dual-read) — app-side
6. **Later**: Switch write path to ORM, keep DBF sync for legacy; then drop the DBF dependency and run on PostgreSQL

## Usage Example

```python
from sp5lib.orm import get_engine, init_db
from sp5lib.orm.base import session_scope
from sp5lib.orm.repository import EmployeeRepository, GroupRepository

engine = get_engine("sqlite:///sp5.db")
init_db(engine)

with session_scope(engine) as session:
    emp_repo = EmployeeRepository(session)
    grp_repo = GroupRepository(session)

    # Create an employee
    emp = emp_repo.create(name="Müller", firstname="Hans", hrsweek=38.5)

    # Create a group and add the employee
    grp = grp_repo.create(name="Frühschicht", shortname="FS")
    grp_repo.add_member(grp.id, emp.id)

    # Query
    members = grp_repo.get_members(grp.id)
    results = emp_repo.search("müll")
```

## Running Tests

```bash
cd backend
python -m pytest tests/test_orm.py -v
```
