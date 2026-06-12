"""A5: Arbeitsplatz-Zuordnung im Dienstplan (Spec 6.4).

add_schedule_entry(workplace_id) + set_schedule_workplace setzen 5MASHI.WORKPLACID;
get_schedule reichert workplace_name aus 5WOPL an.
"""

from test_database_calculations import SPECS, make_db

SPECS["5WOPL"] = [("ID", "N", 11), ("NAME", "C", 40), ("HIDE", "N", 1)]
SPECS["5MASHI_W"] = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("TYPE", "N", 5),
    ("RESERVED", "C", 20),
]


def _db(tmp_path):
    saved = SPECS.get("5MASHI")
    SPECS["5MASHI"] = SPECS["5MASHI_W"]
    try:
        db = make_db(tmp_path, {
            "5MASHI": [],
            "5WOPL": [
                {"ID": 1, "NAME": "Empfang", "HIDE": 0},
                {"ID": 2, "NAME": "Lager", "HIDE": 0},
            ],
        })
    finally:
        if saved is not None:
            SPECS["5MASHI"] = saved
    return db


def test_add_entry_with_workplace_and_enrichment(tmp_path):
    db = _db(tmp_path)
    db.add_schedule_entry(7, "2099-06-02", 1, workplace_id=2)
    sched = db.get_schedule(2099, 6)
    e = next(x for x in sched if x["employee_id"] == 7)
    assert e["workplace_id"] == 2
    assert e["workplace_name"] == "Lager"


def test_set_workplace_on_existing(tmp_path):
    db = _db(tmp_path)
    db.add_schedule_entry(7, "2099-06-03", 1)  # ohne Arbeitsplatz
    assert db.set_schedule_workplace(7, "2099-06-03", 1) == 1
    e = next(x for x in db.get_schedule(2099, 6) if x["employee_id"] == 7)
    assert e["workplace_id"] == 1 and e["workplace_name"] == "Empfang"
    # 0 entfernt die Zuordnung
    assert db.set_schedule_workplace(7, "2099-06-03", 0) == 1
    e = next(x for x in db.get_schedule(2099, 6) if x["employee_id"] == 7)
    assert not e["workplace_id"]
    assert e["workplace_name"] == ""


def test_set_workplace_no_entry_returns_zero(tmp_path):
    db = _db(tmp_path)
    assert db.set_schedule_workplace(7, "2099-06-09", 1) == 0
