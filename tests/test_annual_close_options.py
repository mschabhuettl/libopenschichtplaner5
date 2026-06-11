"""V-17 Jahresabschluss: Option "Urlaubsansprüche bleiben im Folgejahr gleich"
(keep_entitlements, R6.8-4) und artspezifisches CARRYFWD (R6.8-5)."""

import pytest
from test_database_calculations import EMP_WEEK, SONDERURLAUB, URLAUB, make_db


def _close_db(tmp_path):
    """Urlaub (CARRYFWD=1) 5 Tage genommen, Sonderurlaub (CARRYFWD=0) 1 Tag."""
    absen = [
        {"ID": i, "EMPLOYEEID": 1, "DATE": f"2014-06-{i + 1:02d}", "LEAVETYPID": 1,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0}
        for i in range(1, 6)
    ] + [
        {"ID": 9, "EMPLOYEEID": 1, "DATE": "2014-07-01", "LEAVETYPID": 14,
         "TYPE": 0, "INTERVAL": 0, "START": 0, "END": 0}
    ]
    leaen = [
        {"ID": 1, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 1,
         "ENTITLEMNT": 32.0, "REST": 2.0, "INDAYS": 1},
        {"ID": 2, "EMPLOYEEID": 1, "YEAR": 2014, "LEAVETYPID": 14,
         "ENTITLEMNT": 2.0, "REST": 0.0, "INDAYS": 1},
    ]
    return make_db(tmp_path, {
        "5EMPL": [EMP_WEEK],
        "5LEAVT": [URLAUB, SONDERURLAUB],
        "5LEAEN": leaen,
        "5ABSEN": absen,
    })


def test_annual_close_keep_entitlements(tmp_path):
    """R6.8-4: ENTITLEMNT wird kopiert; auch Arten ohne CARRYFWD fortgeschrieben
    (mit REST=0); Verfall wird weiterhin als forfeited ausgewiesen."""
    db = _close_db(tmp_path)
    preview = db.get_annual_close_preview(2014, keep_entitlements=True)
    # Urlaub: 32+2−5 = 29 Übertrag; Sonderurlaub: Rest 1 verfällt (CARRYFWD=0)
    assert preview["total_carry_forward"] == pytest.approx(29.0)
    assert preview["total_forfeited"] == pytest.approx(1.0)

    result = db.run_annual_close(2014, keep_entitlements=True)
    assert result["total_carry_forward"] == pytest.approx(29.0)
    assert result["total_forfeited"] == pytest.approx(1.0)
    rows = {r["leave_type_id"]: r for r in db.get_leave_entitlements(year=2015, employee_id=1)}
    # ENTITLEMNT 2014 kopiert (32 statt STDENTIT 30); Sonderurlaub mit REST=0
    assert rows[1]["entitlement"] == pytest.approx(32.0)
    assert rows[1]["carry_forward"] == pytest.approx(29.0)
    assert rows[14]["entitlement"] == pytest.approx(2.0)
    assert rows[14]["carry_forward"] == pytest.approx(0.0)


def test_annual_close_without_option_respects_carryfwd(tmp_path):
    """Ohne Option: nur CARRYFWD-Arten, ENTITLEMNT aus STDENTIT (R6.8-5)."""
    db = _close_db(tmp_path)
    result = db.run_annual_close(2014)
    assert result["total_carry_forward"] == pytest.approx(29.0)
    assert result["total_forfeited"] == pytest.approx(1.0)
    rows = {r["leave_type_id"]: r for r in db.get_leave_entitlements(year=2015, employee_id=1)}
    assert set(rows) == {1}  # Sonderurlaub (CARRYFWD=0) nicht fortgeschrieben
    assert rows[1]["entitlement"] == pytest.approx(30.0)  # STDENTIT
    assert rows[1]["carry_forward"] == pytest.approx(29.0)
