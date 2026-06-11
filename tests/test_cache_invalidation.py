"""Repro: DBF-Cache-Invalidierung nach eigenen Writes (mtime-Tick-Race).

Der globale DBF-Cache in sp5lib.database ist mtime-basiert. Schreibt ein
SP5Database-Schreibpfad und liest unmittelbar danach im selben
Dateisystem-Zeittick (mtime unverändert), lieferte der Cache veraltete
Daten, wenn die Methode nicht explizit _invalidate_cache() aufrief — das
taten nur ~15 von ~55 Schreibmethoden. Seit der zentralen Invalidierung
(append/update/delete/pack-Wrapper) ist jeder Schreibpfad abgedeckt.

Der eingefrorene os.path.getmtime simuliert den gemeinsamen Zeittick.
"""

from test_database_calculations import EMP_WEEK, URLAUB, make_db

import sp5lib.database as dbmod


def _freeze_mtime(monkeypatch):
    monkeypatch.setattr(dbmod.os.path, "getmtime", lambda _p: 42.0)


def test_update_employee_visible_despite_same_mtime(tmp_path, monkeypatch):
    db = make_db(tmp_path, {"5EMPL": [EMP_WEEK]})
    _freeze_mtime(monkeypatch)
    assert db.get_employee(1)["NAME"] == "Muster"  # Cache vorbelegen
    db.update_employee(1, {"NAME": "Neu"})
    assert db.get_employee(1)["NAME"] == "Neu"


def test_add_absence_visible_despite_same_mtime(tmp_path, monkeypatch):
    db = make_db(tmp_path, {"5EMPL": [EMP_WEEK], "5LEAVT": [URLAUB], "5ABSEN": []})
    _freeze_mtime(monkeypatch)
    assert db._read("ABSEN") == []  # Cache vorbelegen
    db.add_absence(1, "2014-12-01", 1)
    assert len(db._read("ABSEN")) == 1
