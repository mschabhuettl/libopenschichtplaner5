"""
SQLAlchemy ORM layer for OpenSchichtplaner5.

This package provides a database-agnostic ORM abstraction that can target
SQLite (development/testing) or PostgreSQL (production) without changing
application code. It coexists with the existing DBF-based data layer and
is intended as a migration path, not a replacement.

Usage:
    from sp5lib.orm import get_engine, get_session, init_db
    from sp5lib.orm.models import Employee, Group

    engine = get_engine("sqlite:///sp5.db")
    init_db(engine)

    with get_session(engine) as session:
        employees = session.query(Employee).filter_by(hide=False).all()
"""

from .base import Base, get_engine, get_session, init_db

__all__ = ["Base", "get_engine", "get_session", "init_db"]
