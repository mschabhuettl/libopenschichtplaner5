"""P-VOLLERFASSUNG Lücke #4: Schichtmodell-Zuordnung über einen Zeitraum
(5CYASS.END). Das Original ordnet Schichtmodelle „über beliebige Zeiträume" zu
(Handbuch Willkommen.03 / MitarbeiterErfassen.31); osp5 schrieb bisher nur START
und ließ END leer (offen). Die Expansion (expand_cycle_assignments) berücksichtigt
END längst — nur der Schreibpfad konnte es nicht setzen.
"""

from datetime import date

from test_database_calculations import SPECS, make_db

from sp5lib import calculations as calc

# 5CYASS-Schema des Test-Scaffolds um RESERVED ergänzen (real C,20; assign_cycle
# schreibt das Feld). Additiv, daher für andere Tests unkritisch.
if "RESERVED" not in {f[0] for f in SPECS["5CYASS"]}:
    SPECS["5CYASS"] = SPECS["5CYASS"] + [("RESERVED", "C", 20)]


def _read_cyass(db):
    return db._read("CYASS")


def test_assign_cycle_writes_end():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        database = make_db(Path(tmp), {"5CYASS": []})
        # mit End-Datum → befristet
        res = database.assign_cycle(10, 1, "2026-06-01", end_date="2026-09-30")
        assert res["end"] == "2026-09-30"
        rec = next(r for r in _read_cyass(database) if int(r["ID"]) == res["id"])
        assert str(rec["END"]) == "2026-09-30"
        # ohne End-Datum → offen (leeres D-Feld)
        res2 = database.assign_cycle(11, 1, "2026-06-01")
        assert res2["end"] == ""
        rec2 = next(r for r in _read_cyass(database) if int(r["ID"]) == res2["id"])
        assert not rec2["END"]


def test_befristete_assignment_expansion_stops_at_end():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        database = make_db(
            Path(tmp),
            {
                # Tagesmodell SIZE=1: jede Position 0 → Schicht 5, also täglich Dienst
                "5CYCLE": [{"ID": 1, "NAME": "Täglich", "SIZE": 1, "UNIT": 0, "HIDE": 0}],
                "5CYENT": [{"ID": 1, "CYCLEEID": 1, "INDEX": 0, "SHIFTID": 5, "WORKPLACID": 0}],
                "5CYASS": [],
            },
        )
        database.assign_cycle(10, 1, "2026-06-01", end_date="2026-06-03")

        duties = calc.expand_cycle_assignments(
            _read_cyass(database),
            cycles=database._read("CYCLE"),
            cycle_entries=database._read("CYENT"),
            von=date(2026, 6, 1),
            bis=date(2026, 6, 30),
        )
        days = sorted(d["DATE"] for d in duties)
        # Nur 1.-3. Juni, NICHT bis Monatsende
        assert days == ["2026-06-01", "2026-06-02", "2026-06-03"]
