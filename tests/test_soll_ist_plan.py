"""A1: Soll-/Istplan (Spec 4.12, D-58). 5MASHI/5SPSHI.TYPE kodiert
0=Istplan (Normaleintrag), 1=Sollplan (Zielvorgabe) — Dekompilat-belegt
(SP5V/SP5R Renderer, Konstruktor-Default 0). get_schedule(plan=…) filtert,
jeder Dienst trägt schedule_type; add_schedule_entry schreibt TYPE.
"""

import pytest

from test_database_calculations import SPECS, make_db

# 5MASHI inкl. TYPE (das Golden-Schema hat TYPE; die Default-Test-Spec nicht)
SPECS["5MASHI_T"] = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("TYPE", "N", 5),
    ("RESERVED", "C", 20),
]


def _db(tmp_path, mashi):
    # make_db erwartet SPECS[table]; temporär 5MASHI auf das TYPE-Schema setzen
    saved = SPECS.get("5MASHI")
    SPECS["5MASHI"] = SPECS["5MASHI_T"]
    try:
        db = make_db(tmp_path, {"5MASHI": mashi})
    finally:
        if saved is not None:
            SPECS["5MASHI"] = saved
    return db


def test_schedule_type_exposed_and_filtered(tmp_path):
    mashi = [
        {"ID": 1, "EMPLOYEEID": 5, "DATE": "2099-04-01", "SHIFTID": 1,
         "WORKPLACID": 0, "TYPE": 0, "RESERVED": ""},   # Istplan
        {"ID": 2, "EMPLOYEEID": 5, "DATE": "2099-04-01", "SHIFTID": 2,
         "WORKPLACID": 0, "TYPE": 1, "RESERVED": ""},   # Sollplan
    ]
    db = _db(tmp_path, mashi)

    ist = db.get_schedule(2099, 4, plan="ist")
    soll = db.get_schedule(2099, 4, plan="soll")
    both = db.get_schedule(2099, 4, plan="both")

    assert [e["schedule_type"] for e in ist] == [0]
    assert ist[0]["shift_id"] == 1
    assert [e["schedule_type"] for e in soll] == [1]
    assert soll[0]["shift_id"] == 2
    assert sorted(e["schedule_type"] for e in both) == [0, 1]


def test_default_plan_is_ist(tmp_path):
    mashi = [
        {"ID": 1, "EMPLOYEEID": 5, "DATE": "2099-04-02", "SHIFTID": 1,
         "WORKPLACID": 0, "TYPE": 0, "RESERVED": ""},
        {"ID": 2, "EMPLOYEEID": 5, "DATE": "2099-04-02", "SHIFTID": 2,
         "WORKPLACID": 0, "TYPE": 1, "RESERVED": ""},
    ]
    db = _db(tmp_path, mashi)
    # Vorgabe ohne plan-Argument = Istplan
    default = db.get_schedule(2099, 4)
    assert [e["shift_id"] for e in default] == [1]


def test_add_entry_writes_type_and_allows_soll_ist_coexistence(tmp_path):
    db = _db(tmp_path, [])
    db.add_schedule_entry(5, "2099-04-03", 1)                      # Ist (Default)
    db.add_schedule_entry(5, "2099-04-03", 2, schedule_type=1)     # Soll
    # zweiter Ist-Eintrag am selben Tag = Duplikat
    with pytest.raises(ValueError):
        db.add_schedule_entry(5, "2099-04-03", 3)
    both = db.get_schedule(2099, 4, plan="both")
    by_type = {e["schedule_type"]: e["shift_id"] for e in both}
    assert by_type == {0: 1, 1: 2}
