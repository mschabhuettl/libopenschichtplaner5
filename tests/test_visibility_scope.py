"""A3: Differenzierte Sichtbarkeit (Spec 9.5.3, 5GRACC/5EMACC).

get_user_visible_employee_ids: None = unbeschränkt; sonst Mitglieder der per
5GRACC zugänglichen Gruppen (inkl. Untergruppen, SUPERID) + 5EMACC-Mitarbeiter.
"""

from test_database_calculations import SPECS, make_db

SPECS["5GROUP_H"] = [
    ("ID", "N", 11), ("NAME", "C", 40), ("SHORTNAME", "C", 12),
    ("POSITION", "N", 11), ("SUPERID", "N", 11), ("HIDE", "N", 1),
]
SPECS["5GRACC"] = [
    ("ID", "N", 11), ("USERID", "N", 11), ("GROUPID", "N", 11),
    ("RIGHTS", "N", 11), ("RESERVED", "C", 20),
]
SPECS["5EMACC"] = [
    ("ID", "N", 11), ("USERID", "N", 11), ("EMPLOYEEID", "N", 11),
    ("RIGHTS", "N", 11), ("RESERVED", "C", 20),
]


def _db(tmp_path, gracc=None, emacc=None):
    saved = SPECS.get("5GROUP")
    SPECS["5GROUP"] = SPECS["5GROUP_H"]
    try:
        db = make_db(tmp_path, {
            # Gruppe 1 (Eltern) mit Untergruppe 2
            "5GROUP": [
                {"ID": 1, "NAME": "Haus", "SHORTNAME": "H", "POSITION": 1,
                 "SUPERID": 0, "HIDE": 0},
                {"ID": 2, "NAME": "Station", "SHORTNAME": "S", "POSITION": 2,
                 "SUPERID": 1, "HIDE": 0},
                {"ID": 3, "NAME": "Extern", "SHORTNAME": "E", "POSITION": 3,
                 "SUPERID": 0, "HIDE": 0},
            ],
            "5GRASG": [
                {"ID": 1, "EMPLOYEEID": 10, "GROUPID": 1},
                {"ID": 2, "EMPLOYEEID": 11, "GROUPID": 2},   # Untergruppe
                {"ID": 3, "EMPLOYEEID": 12, "GROUPID": 3},   # andere Gruppe
            ],
            "5GRACC": gracc or [],
            "5EMACC": emacc or [],
        })
    finally:
        if saved is not None:
            SPECS["5GROUP"] = saved
    return db


def test_no_access_records_means_unrestricted(tmp_path):
    db = _db(tmp_path)
    assert db.get_user_visible_employee_ids(99) is None
    assert db.get_user_visible_group_ids(99) is None


def test_group_access_includes_subgroup_members(tmp_path):
    # Benutzer 5 hat Zugriff auf Gruppe 1 (inkl. Untergruppe 2)
    db = _db(tmp_path, gracc=[{"ID": 1, "USERID": 5, "GROUPID": 1, "RIGHTS": 0,
                               "RESERVED": ""}])
    vis = db.get_user_visible_employee_ids(5)
    assert vis == {10, 11}          # MA 12 (Gruppe 3) bleibt verborgen
    assert db.get_user_visible_group_ids(5) == {1, 2}


def test_employee_access_extends_scope(tmp_path):
    db = _db(
        tmp_path,
        gracc=[{"ID": 1, "USERID": 5, "GROUPID": 2, "RIGHTS": 0, "RESERVED": ""}],
        emacc=[{"ID": 1, "USERID": 5, "EMPLOYEEID": 12, "RIGHTS": 1,
                "RESERVED": ""}],
    )
    vis = db.get_user_visible_employee_ids(5)
    assert vis == {11, 12}          # Untergruppe-2-Mitglied + explizit MA 12
