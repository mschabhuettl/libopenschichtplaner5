"""SHOWABS-Sichtbarkeit / Anonymisierung (Spec 9.5.2 Nr. 2.1, 9.2 Nr. 3, D-67).

apply_absence_visibility transformiert beliebig verschachtelte Plan-Strukturen:
mode 0 = unverändert, 1 = anonymisiert (5USETT-ANOA*), 2 = Abwesenheiten entfernt.
"""

from test_database_calculations import SPECS, make_db

SPECS["5USETT"] = [
    ("ID", "N", 11), ("LOGIN", "N", 1), ("SPSHCAT", "N", 5),
    ("OVERTCAT", "N", 5), ("ANOANAME", "C", 200), ("ANOASHORT", "C", 200),
    ("ANOACRTXT", "N", 11), ("ANOACRBAR", "N", 11), ("ANOACRBK", "N", 11),
    ("ANOABOLD", "N", 1), ("BACKUPFR", "N", 1),
]


def _db(tmp_path):
    usett = [{
        "ID": 0, "LOGIN": 0, "SPSHCAT": 0, "OVERTCAT": 0,
        "ANOANAME": "Abwesend", "ANOASHORT": "X", "ANOACRTXT": 0,
        "ANOACRBAR": 16711680, "ANOACRBK": 16777215, "ANOABOLD": 0,
        "BACKUPFR": 0,
    }]
    return make_db(tmp_path, {"5USETT": usett})


def _entries():
    return [
        {"employee_id": 1, "kind": "shift", "display_name": "T",
         "leave_name": "", "color_bk": "#fff", "color_text": "#000"},
        {"employee_id": 2, "kind": "absence", "display_name": "U",
         "leave_name": "Urlaub", "leave_type_id": 7,
         "color_bk": "#abc", "color_text": "#111"},
    ]


def test_mode0_unchanged(tmp_path):
    db = _db(tmp_path)
    entries = _entries()
    out = db.apply_absence_visibility(entries, 0)
    assert out is entries  # identisch zurück, kein Aufwand


def test_mode2_hides_absences(tmp_path):
    db = _db(tmp_path)
    out = db.apply_absence_visibility(_entries(), 2)
    kinds = [e["kind"] for e in out]
    assert kinds == ["shift"]  # Abwesenheit entfernt, Dienst bleibt


def test_mode1_anonymises_absence(tmp_path):
    db = _db(tmp_path)
    out = db.apply_absence_visibility(_entries(), 1)
    shift, absence = out
    # Dienst unangetastet
    assert shift["display_name"] == "T" and shift["leave_name"] == ""
    # Abwesenheit auf ANOA* ersetzt, echte Art entfernt
    assert absence["display_name"] == "X"
    assert absence["leave_name"] == "Abwesend"
    assert absence["leave_type_id"] is None
    assert absence["anonymized"] is True
    assert absence["color_bk"] == "#FFFFFF"  # ANOACRBK 16777215 → weiß


def test_nested_week_structure(tmp_path):
    db = _db(tmp_path)
    week = {
        "week_start": "2099-01-05",
        "employees": [{"ID": 2, "NAME": "X"}],  # darf NICHT angefasst werden
        "days": [
            {"date": "2099-01-05", "entries": [
                {"employee_id": 2, "kind": "absence", "display_name": "U",
                 "leave_name": "Urlaub", "leave_type_id": 7},
            ]},
        ],
    }
    anon = db.apply_absence_visibility(week, 1)
    assert anon["employees"] == [{"ID": 2, "NAME": "X"}]
    assert anon["days"][0]["entries"][0]["display_name"] == "X"
    hidden = db.apply_absence_visibility(week, 2)
    assert hidden["days"][0]["entries"] == []


def test_showabs_threevalue_in_user_dict(tmp_path):
    """_build_user_dict trägt SHOWABS (mode!=2) + SHOWABS_MODE roh."""
    SPECS["5USER"] = [
        ("ID", "N", 11), ("POSITION", "N", 11), ("NAME", "C", 40),
        ("DESCRIP", "C", 40), ("ADMIN", "N", 1), ("RIGHTS", "N", 11),
        ("HIDE", "N", 1), ("WDUTIES", "N", 1), ("WABSENCES", "N", 1),
        ("WOVERTIMES", "N", 1), ("WNOTES", "N", 1), ("WDEVIATION", "N", 1),
        ("WCYCLEASS", "N", 1), ("WSWAPONLY", "N", 1), ("WPAST", "N", 1),
        ("ADDEMPL", "N", 1), ("SHOWABS", "N", 5), ("SHOWNOTES", "N", 1),
        ("SHOWSTATS", "N", 1), ("BACKUP", "N", 1),
    ]
    users = [
        {"ID": 1, "NAME": "voll", "ADMIN": 0, "SHOWABS": 0},
        {"ID": 2, "NAME": "anon", "ADMIN": 0, "SHOWABS": 1},
        {"ID": 3, "NAME": "keine", "ADMIN": 0, "SHOWABS": 2},
        {"ID": 4, "NAME": "chef", "ADMIN": 1, "SHOWABS": 2},
    ]
    db = make_db(tmp_path, {"5USER": users})
    built = {u["ID"]: db._build_user_dict(u) for u in users}
    assert built[1]["SHOWABS"] is True and built[1]["SHOWABS_MODE"] == 0
    assert built[2]["SHOWABS"] is True and built[2]["SHOWABS_MODE"] == 1
    assert built[3]["SHOWABS"] is False and built[3]["SHOWABS_MODE"] == 2
    # Admin sieht immer vollständig, unabhängig vom Feldwert
    assert built[4]["SHOWABS"] is True and built[4]["SHOWABS_MODE"] == 0
    # Permissions: showabs = darf sehen (mode != 2)
    assert db.get_user_permissions(3)["showabs"] is False
    assert db.get_user_permissions(2)["showabs"] is True
