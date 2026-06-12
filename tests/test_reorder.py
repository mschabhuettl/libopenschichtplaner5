"""A9: programmweite manuelle Stammdaten-Sortierung (POSITION, Spec 5.1 Nr. 4).

reorder(entity, ordered_ids) vergibt POSITION 1..N in der gegebenen Reihenfolge;
get_groups/get_shifts/… sortieren danach.
"""

import pytest
from test_database_calculations import SPECS, make_db

SPECS.setdefault("5GROUP", [
    ("ID", "N", 11), ("NAME", "C", 40), ("SHORTNAME", "C", 12),
    ("POSITION", "N", 11), ("HIDE", "N", 1),
])


def _db(tmp_path):
    return make_db(tmp_path, {"5GROUP": [
        {"ID": 10, "NAME": "Alpha", "SHORTNAME": "A", "POSITION": 1, "HIDE": 0},
        {"ID": 11, "NAME": "Beta", "SHORTNAME": "B", "POSITION": 2, "HIDE": 0},
        {"ID": 12, "NAME": "Gamma", "SHORTNAME": "G", "POSITION": 3, "HIDE": 0},
    ]})


def test_reorder_assigns_positions(tmp_path):
    db = _db(tmp_path)
    # neue Reihenfolge: Gamma, Alpha, Beta
    n = db.reorder("groups", [12, 10, 11])
    assert n == 3
    order = [g["ID"] for g in db.get_groups()]
    assert order == [12, 10, 11]
    # POSITION 1..N vergeben
    pos = {g["ID"]: g["POSITION"] for g in db.get_groups()}
    assert pos == {12: 1, 10: 2, 11: 3}


def test_reorder_unknown_entity_raises(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.reorder("frobnicate", [1, 2])


def test_reorder_partial_list(tmp_path):
    db = _db(tmp_path)
    # nur zwei IDs angegeben — diese bekommen POSITION 1,2
    db.reorder("groups", [11, 10])
    pos = {g["ID"]: g["POSITION"] for g in db.get_groups()}
    assert pos[11] == 1 and pos[10] == 2
