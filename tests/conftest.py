"""Gemeinsame Fixtures: ORM-Spiegel wahlweise auf SQLite oder echtem PostgreSQL.

Standardmäßig läuft der ORM-Spiegel als SQLite-Datei unter tmp_path
(Stellvertreter). Ist DATABASE_URL gesetzt (SQLAlchemy-Form, z. B.
postgresql://postgres:test@localhost:5432/postgres), laufen dieselben Tests
gegen echtes PostgreSQL — vor jedem Test wird das Schema abgeräumt, weil die
PG-Datenbank, anders als tmp_path, über Tests hinweg bestehen bleibt.
"""

import os

import pytest
from sqlalchemy import create_engine

_PG_URL = os.environ.get("DATABASE_URL", "")


@pytest.fixture
def orm_url(tmp_path):
    """Datenbank-URL für den ORM-Spiegel (SQLite-Datei oder DATABASE_URL)."""
    if not _PG_URL:
        return f"sqlite:///{tmp_path}/orm.sqlite"
    from sp5lib.orm import models, models_pg  # noqa: F401 — registriert alle Tabellen
    from sp5lib.orm.base import Base

    engine = create_engine(_PG_URL)
    Base.metadata.drop_all(engine)
    engine.dispose()
    return _PG_URL
