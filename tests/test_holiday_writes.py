"""V-12 Halbe Feiertage: create_holiday/update_holiday mit INTERVAL 0/1/2
(Spec 3.2.1 Nr. 3, UNSICHER) und Option "auch in den Folgejahren"
(repeat_years, Spec 3.2.1 Nr. 4 / R5.16-5)."""

import pytest
from test_database_calculations import make_db


def test_create_holiday_interval_and_repeat_years(tmp_path):
    db = make_db(tmp_path, {"5HOLID": []})
    rec = db.create_holiday({"DATE": "2014-12-24", "NAME": "Heiligabend", "INTERVAL": 2})
    assert rec["INTERVAL"] == 2
    with pytest.raises(ValueError):
        db.create_holiday({"DATE": "2014-12-31", "NAME": "X", "INTERVAL": 3})

    # "auch in den Folgejahren" (Spec 3.2.1 Nr. 4): +9 Jahre, gleicher Termin
    rec = db.create_holiday(
        {"DATE": "2015-01-01", "NAME": "Neujahr", "INTERVAL": 0}, repeat_years=9
    )
    assert len(rec["repeated_ids"]) == 9
    holidays = db.get_holidays()
    neujahr = sorted(h["DATE"] for h in holidays if h["NAME"] == "Neujahr")
    assert neujahr == [f"{y}-01-01" for y in range(2015, 2025)]


def test_update_holiday_interval_validation(tmp_path):
    db = make_db(tmp_path, {"5HOLID": []})
    rec = db.create_holiday({"DATE": "2014-12-24", "NAME": "Heiligabend", "INTERVAL": 0})
    upd = db.update_holiday(rec["id"], {"INTERVAL": 1})
    assert upd["INTERVAL"] == 1
    with pytest.raises(ValueError):
        db.update_holiday(rec["id"], {"INTERVAL": 5})
