"""P-VOLLERFASSUNG Lücke #10 (lib-Seite): create_shift/update_shift und
create_leave_type/update_leave_type schreiben das Fettschrift-Flag (5SHIFT.BOLD
/ 5LEAVT.BOLD). Das DBF-Feld existiert im Original; die lib setzte es bisher nie.
"""

from test_database_calculations import SPECS, make_db

# Farb-/BOLD-Felder an die Test-Schemata anhängen (im realen DBF vorhanden;
# additiv, daher für andere Tests unkritisch).
for _t in ("5SHIFT", "5LEAVT"):
    _have = {f[0] for f in SPECS[_t]}
    SPECS[_t] = SPECS[_t] + [
        f for f in (("COLORTEXT", "N", 11), ("COLORBAR", "N", 11),
                    ("COLORBK", "N", 11), ("BOLD", "N", 1))
        if f[0] not in _have
    ]


def test_create_and_update_shift_bold(tmp_path):
    db = make_db(tmp_path, {"5SHIFT": []})
    sid = db.create_shift({"NAME": "Fett", "SHORTNAME": "FT", "BOLD": True,
                           "DURATION0": 8})["ID"]
    s = next(x for x in db.get_shifts(include_hidden=True) if x["ID"] == sid)
    assert s["BOLD"] == 1
    db.update_shift(sid, {"BOLD": False})
    s2 = next(x for x in db.get_shifts(include_hidden=True) if x["ID"] == sid)
    assert s2["BOLD"] == 0


def test_create_and_update_leave_type_bold(tmp_path):
    db = make_db(tmp_path, {"5LEAVT": []})
    lid = db.create_leave_type({"NAME": "FettAbw", "SHORTNAME": "FA",
                                "BOLD": False})["ID"]
    lt = next(x for x in db.get_leave_types(include_hidden=True) if x["ID"] == lid)
    assert lt["BOLD"] == 0
    db.update_leave_type(lid, {"BOLD": True})
    lt2 = next(x for x in db.get_leave_types(include_hidden=True) if x["ID"] == lid)
    assert lt2["BOLD"] == 1
