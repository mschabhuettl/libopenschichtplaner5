"""V-3 Teiltags-Abwesenheiten: add_absence/update_absence mit INTERVAL/START/END
(Spec 3.5.2/D-54) inkl. Validierung und Ist-Wirkung über die Berechnung."""

import pytest
from test_database_calculations import EMP_WEEK, URLAUB, make_db


def _absence_db(tmp_path):
    return make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5LEAVT": [URLAUB],
        "5ABSEN": [],
    })


def test_add_absence_with_interval(tmp_path):
    """INTERVAL/START/END werden geschrieben und in der Liste geliefert."""
    db = _absence_db(tmp_path)
    rec = db.add_absence(1, "2014-06-02", 1, interval=1)
    assert (rec["INTERVAL"], rec["START"], rec["END"]) == (1, 0, 0)
    rec = db.add_absence(1, "2014-06-03", 1, interval=3, start=480, end=720)
    assert (rec["INTERVAL"], rec["START"], rec["END"]) == (3, 480, 720)
    # Tageswechsel (Spec 3.5.2 Nr. 3): END < START ist zulässig
    rec = db.add_absence(1, "2014-06-04", 1, interval=3, start=1320, end=120)
    assert (rec["START"], rec["END"]) == (1320, 120)

    rows = {r["date"]: r for r in db.get_absences_list(year=2014)}
    assert rows["2014-06-02"]["interval"] == 1
    assert rows["2014-06-03"]["start_time"] == 480
    assert rows["2014-06-03"]["end_time"] == 720


def test_add_absence_interval_validation(tmp_path):
    db = _absence_db(tmp_path)
    with pytest.raises(ValueError):
        db.add_absence(1, "2014-06-02", 1, interval=4)
    with pytest.raises(ValueError):  # START == END ungültig
        db.add_absence(1, "2014-06-02", 1, interval=3, start=480, end=480)
    with pytest.raises(ValueError):  # außerhalb 0..1440
        db.add_absence(1, "2014-06-02", 1, interval=3, start=-1, end=480)
    # INTERVAL != 3: START/END werden ignoriert (auf 0 normiert)
    rec = db.add_absence(1, "2014-06-02", 1, interval=2, start=99, end=480)
    assert (rec["START"], rec["END"]) == (0, 0)


def test_update_absence_interval(tmp_path):
    db = _absence_db(tmp_path)
    db.add_absence(1, "2014-06-02", 1)
    rec = db.update_absence(1, "2014-06-02", interval=3, start=600, end=840)
    assert (rec["INTERVAL"], rec["START"], rec["END"]) == (3, 600, 840)
    rows = db.get_absences_list(year=2014)
    assert (rows[0]["interval"], rows[0]["start_time"], rows[0]["end_time"]) == (3, 600, 840)
    # zurück auf ganztägig: START/END werden genullt
    rec = db.update_absence(1, "2014-06-02", interval=0)
    assert (rec["INTERVAL"], rec["START"], rec["END"]) == (0, 0, 0)
    with pytest.raises(ValueError):
        db.update_absence(1, "2014-12-24", interval=1)  # kein Satz vorhanden


def test_partial_absence_feeds_calculation(tmp_path):
    """Ist-Wirkung (Spec 3.5.2 Nr. 3): Halbtag = 0,5·HRSDAY, stundenweise = Minuten/60."""
    db = make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5LEAVT": [dict(URLAUB, CHARGETYP=1)],
        "5ABSEN": [],
    })
    db.add_absence(1, "2014-12-01", 1, interval=1)  # Mo, Vormittag → 3,85 h
    db.add_absence(1, "2014-12-02", 1, interval=3, start=480, end=720)  # 4 h
    stats = db.get_statistics(2014, 12)
    assert stats[0]["actual_hours"] == pytest.approx(7.7 * 0.5 + 4.0)
