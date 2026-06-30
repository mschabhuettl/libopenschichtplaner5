"""P3-1 (Punkt 3): Eine lange Schicht (z. B. 12 h) ist KEIN Konflikt.

Früher meldete `get_schedule_conflicts` jeden Dienst über 10 h als „long_shift"-
Warnung. Ein starrer Stunden-Schwellwert ist falsch — 12-Stunden-Schichten sind in
vielen Betrieben normal, und das Original kennt gar keine solche Prüfung. Der
long_shift-Konflikt wurde entfernt; ein echter Tageskonflikt (Dienst + Abwesenheit)
bleibt erhalten.
"""

from test_database_calculations import EMP_WEEK, SPECS, make_db

# 5MASHI inkl. TYPE-Spalte (Ist=0), analog test_conflict_soll_ist.py
_MASHI_T = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("TYPE", "N", 5),
    ("RESERVED", "C", 20),
]


def _db_with_12h_shift(tmp_path):
    # Schichtart „Langdienst" mit 12 h Dauer (DURATION0=12).
    long_shift = {"ID": 1, "NAME": "Langdienst", "SHORTNAME": "L", "POSITION": 1,
                  "HIDE": 0, "NOEXTRA": 0}
    for i in range(7):
        long_shift[f"STARTEND{i}"] = "06:00-18:00"
        long_shift[f"DURATION{i}"] = 12.0
    mashi = [{
        "ID": 1, "EMPLOYEEID": 1, "DATE": "2099-04-01", "SHIFTID": 1,
        "WORKPLACID": 0, "TYPE": 0, "RESERVED": "",
    }]
    saved = SPECS.get("5MASHI")
    SPECS["5MASHI"] = _MASHI_T
    try:
        db = make_db(tmp_path, {
            "5EMPL": [EMP_WEEK],
            "5SHIFT": [long_shift],
            "5MASHI": mashi,
        })
    finally:
        SPECS["5MASHI"] = saved
    return db


def test_twelve_hour_shift_is_not_a_long_shift_conflict(tmp_path):
    db = _db_with_12h_shift(tmp_path)
    conflicts = db.get_schedule_conflicts(2099, 4)
    assert [c for c in conflicts if c["type"] == "long_shift"] == []
    # Auch generell: ein einzelner 12-h-Dienst ohne Abwesenheit erzeugt keinen Konflikt.
    assert [c for c in conflicts if c["employee_id"] == 1] == []
