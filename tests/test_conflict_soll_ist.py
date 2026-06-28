"""P-KONFLIKT: Konflikterkennung läuft nur auf der Ist-Ebene.

Ein Sollplan-Dienst (5MASHI.TYPE=1, Spec 4.12/D-58) ist eine geplante Zielvorgabe.
Trifft er am selben Tag mit einer Ist-Abwesenheit (Krankenstand) zusammen, ist das
die normale Soll-/Ist-Zwei-Ebenen-Ansicht und KEIN Konflikt — das Original-
Schichtplaner5 kennt zwischen den Ebenen gar keine Konfliktprüfung.

Beide Richtungen abgesichert (Revert→rot):
- Soll-Schicht (TYPE=1) + Ist-Abwesenheit  → KEIN shift_and_absence-Konflikt
- Ist-Schicht  (TYPE=0) + Ist-Abwesenheit  → weiterhin GENAU EIN Konflikt
"""

from test_database_calculations import SPECS, EMP_WEEK, make_db

# 5MASHI inkl. TYPE-Spalte (Golden-Schema), analog test_soll_ist_plan.py
_MASHI_T = [
    ("ID", "N", 11), ("EMPLOYEEID", "N", 11), ("DATE", "D", 8),
    ("SHIFTID", "N", 11), ("WORKPLACID", "N", 11), ("TYPE", "N", 5),
    ("RESERVED", "C", 20),
]

_KRANK = {
    "ID": 1, "NAME": "Krank", "SHORTNAME": "K", "POSITION": 1, "HIDE": 0,
    "CHARGETYP": 0, "CHARGEHRS": 0.0, "DEDUCTACT": 0, "DEDUCTOVT": 0,
    "ENTITLED": 0, "STDENTIT": 0.0, "CARRYFWD": 0, "COUNTALL": 0,
}


def _db(tmp_path, mashi_type):
    """DB mit MA 1: Dienst (gegebener TYPE) + Krankenstand am 2099-04-01."""
    mashi = [{
        "ID": 1, "EMPLOYEEID": 1, "DATE": "2099-04-01", "SHIFTID": 1,
        "WORKPLACID": 0, "TYPE": mashi_type, "RESERVED": "",
    }]
    absen = [{
        "ID": 1, "EMPLOYEEID": 1, "DATE": "2099-04-01", "LEAVETYPID": 1,
        "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0,
    }]
    saved = SPECS.get("5MASHI")
    SPECS["5MASHI"] = _MASHI_T
    try:
        db = make_db(tmp_path, {
            "5EMPL": [EMP_WEEK],
            "5LEAVT": [_KRANK],
            "5MASHI": mashi,
            "5ABSEN": absen,
        })
    finally:
        SPECS["5MASHI"] = saved
    return db


def _shift_absence_conflicts(db):
    return [
        c for c in db.get_schedule_conflicts(2099, 4)
        if c["type"] == "shift_and_absence"
        and c["employee_id"] == 1 and c["date"] == "2099-04-01"
    ]


def test_soll_shift_plus_ist_absence_is_not_a_conflict(tmp_path):
    # Sollplan-Dienst (TYPE=1) + Krankenstand: normale Soll-/Ist-Abweichung.
    db = _db(tmp_path, mashi_type=1)
    assert _shift_absence_conflicts(db) == []


def test_ist_shift_plus_absence_still_conflicts(tmp_path):
    # Gegenrichtung: echter Ist-Dienst (TYPE=0) + Abwesenheit bleibt ein Konflikt.
    db = _db(tmp_path, mashi_type=0)
    assert len(_shift_absence_conflicts(db)) == 1
