"""
DBF → SQLAlchemy sync utility.

Reads data from DBF files (the legacy data source) and upserts into
the SQLAlchemy-managed database. This enables a gradual migration:
DBF remains the source of truth while the ORM layer is built out.

Usage:
    from sp5lib.orm import get_engine, init_db
    from sp5lib.orm.sync import sync_employees, sync_groups, sync_all

    engine = get_engine("sqlite:///sp5.db")
    init_db(engine)
    stats = sync_all(engine, "/path/to/Daten")
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from .base import get_session
from .models import Employee, Group, GroupAssignment

_log = logging.getLogger("sp5api.orm.sync")


def _read_dbf(daten_path: str, table_name: str) -> list[dict[str, Any]]:
    """Read a DBF table, returning list of dicts."""
    import os

    from sp5lib.dbf_reader import read_dbf

    path = os.path.join(daten_path, f"5{table_name}.DBF")
    try:
        return read_dbf(path)
    except Exception as exc:
        _log.warning("Could not read %s: %s", path, exc)
        return []


def sync_employees(session: Session, daten_path: str) -> int:
    """Sync employees from 5EMPL.DBF into the ORM employees table.

    Uses upsert semantics: existing records (by ID) are updated,
    new records are inserted. Returns the number of synced rows.
    """
    rows = _read_dbf(daten_path, "EMPL")
    count = 0

    for r in rows:
        emp_id = r.get("ID")
        if not emp_id:
            continue

        emp = session.get(Employee, emp_id)
        if emp is None:
            emp = Employee(id=emp_id)
            session.add(emp)

        emp.position = r.get("POSITION", 0) or 0
        emp.number = str(r.get("NUMBER") or "").strip()
        emp.name = str(r.get("NAME") or "").strip()
        emp.firstname = str(r.get("FIRSTNAME") or "").strip()
        emp.shortname = str(r.get("SHORTNAME") or "").strip()
        emp.sex = r.get("SEX", 0)
        emp.hrsday = float(r.get("HRSDAY", 0) or 0)
        emp.hrsweek = float(r.get("HRSWEEK", 0) or 0)
        emp.hrsmonth = float(r.get("HRSMONTH", 0) or 0)
        emp.workdays = str(r.get("WORKDAYS") or "").strip()
        emp.salutation = str(r.get("SALUTATION") or "").strip()
        emp.street = str(r.get("STREET") or "").strip()
        emp.zip = str(r.get("ZIP") or "").strip()
        emp.town = str(r.get("TOWN") or "").strip()
        emp.phone = str(r.get("PHONE") or "").strip()
        emp.email = str(r.get("EMAIL") or "").strip()
        emp.function = str(r.get("FUNCTION") or "").strip()
        emp.birthday = str(r.get("BIRTHDAY") or "").strip() or None
        emp.empstart = str(r.get("EMPSTART") or "").strip() or None
        emp.empend = str(r.get("EMPEND") or "").strip() or None
        emp.hide = bool(r.get("HIDE"))
        emp.note1 = str(r.get("NOTE1") or "").strip()
        emp.note2 = str(r.get("NOTE2") or "").strip()
        emp.note3 = str(r.get("NOTE3") or "").strip()
        emp.note4 = str(r.get("NOTE4") or "").strip()
        count += 1

    session.flush()
    return count


def sync_groups(session: Session, daten_path: str) -> int:
    """Sync groups from 5GROUP.DBF into the ORM groups table."""
    rows = _read_dbf(daten_path, "GROUP")
    count = 0

    for r in rows:
        group_id = r.get("ID")
        if not group_id:
            continue

        group = session.get(Group, group_id)
        if group is None:
            group = Group(id=group_id)
            session.add(group)

        group.name = str(r.get("NAME") or "").strip()
        group.shortname = str(r.get("SHORTNAME") or "").strip()
        group.super_id = r.get("SUPERID") or None
        group.position = r.get("POSITION", 0) or 0
        group.hide = bool(r.get("HIDE"))
        count += 1

    session.flush()
    return count


def sync_group_assignments(session: Session, daten_path: str) -> int:
    """Sync group assignments from 5GRASG.DBF."""
    rows = _read_dbf(daten_path, "GRASG")

    # Clear existing assignments and re-insert (simple full-sync approach)
    session.query(GroupAssignment).delete()
    session.flush()

    count = 0
    for r in rows:
        ga_id = r.get("ID")
        emp_id = r.get("EMPLOYEEID")
        group_id = r.get("GROUPID")
        if not ga_id or not emp_id or not group_id:
            continue

        ga = GroupAssignment(id=ga_id, employee_id=emp_id, group_id=group_id)
        session.add(ga)
        count += 1

    session.flush()
    return count


def sync_all(engine, daten_path: str) -> dict[str, int]:
    """Sync all supported tables from DBF into the ORM database.

    Returns dict of table_name → row_count.
    """
    session = get_session(engine)
    try:
        stats = {}
        stats["employees"] = sync_employees(session, daten_path)
        stats["groups"] = sync_groups(session, daten_path)
        stats["group_assignments"] = sync_group_assignments(session, daten_path)
        session.commit()
        _log.info("ORM sync complete: %s", stats)
        return stats
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
