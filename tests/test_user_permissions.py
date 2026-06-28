"""G-1 (lib-Seite): get_user_permissions liefert die granularen 5USER-Flags
(Spec 9.6) als permissions-Dict; Admin ⇒ alles True."""

from test_database_calculations import SPECS, make_db

SPECS["5USER"] = [
    ("ID", "N", 11), ("POSITION", "N", 11), ("NAME", "C", 40),
    ("DESCRIP", "C", 40), ("ADMIN", "N", 1), ("RIGHTS", "N", 11),
    ("HIDE", "N", 1), ("WDUTIES", "N", 1), ("WABSENCES", "N", 1),
    ("WOVERTIMES", "N", 1), ("WNOTES", "N", 1), ("WDEVIATION", "N", 1),
    ("WCYCLEASS", "N", 1), ("WSWAPONLY", "N", 1), ("WPAST", "N", 1),
    ("ADDEMPL", "N", 1), ("SHOWABS", "N", 5), ("SHOWNOTES", "N", 1),
    ("SHOWSTATS", "N", 1), ("BACKUP", "N", 1),
]


def test_get_user_permissions(tmp_path):
    users = [
        {"ID": 1, "POSITION": 1, "NAME": "Admin", "DESCRIP": "", "ADMIN": 1,
         "RIGHTS": 0, "HIDE": 0, "WDUTIES": 0, "WABSENCES": 0, "WOVERTIMES": 0,
         "WNOTES": 0, "WDEVIATION": 0, "WCYCLEASS": 0, "WSWAPONLY": 0,
         "WPAST": 0, "ADDEMPL": 0, "SHOWABS": 0, "SHOWNOTES": 0,
         "SHOWSTATS": 0, "BACKUP": 0},
        {"ID": 2, "POSITION": 2, "NAME": "Planer", "DESCRIP": "", "ADMIN": 0,
         "RIGHTS": 1, "HIDE": 0, "WDUTIES": 1, "WABSENCES": 1, "WOVERTIMES": 0,
         "WNOTES": 1, "WDEVIATION": 0, "WCYCLEASS": 1, "WSWAPONLY": 0,
         "WPAST": 0, "ADDEMPL": 1, "SHOWABS": 1, "SHOWNOTES": 1,
         "SHOWSTATS": 1, "BACKUP": 0},
    ]
    db = make_db(tmp_path, {"5USER": users})
    # Admin ⇒ alles True (auch wenn die Flags im Satz 0 sind)
    perms = db.get_user_permissions(1)
    assert perms is not None and all(perms.values())
    assert set(perms) == {
        "wduties", "wabsences", "wovertimes", "wnotes", "wdeviation",
        "wcycleass", "wswaponly", "wpast", "addempl", "showabs",
        "shownotes", "showstats", "backup",
    }
    # Nicht-Admin: Flags aus dem 5USER-Satz
    perms = db.get_user_permissions(2)
    assert perms["wduties"] is True
    assert perms["wovertimes"] is False
    assert perms["wpast"] is False
    assert perms["addempl"] is True
    # Unbekannte ID
    assert db.get_user_permissions(999) is None


def test_get_user_identity_matches_login_shape(tmp_path):
    """P-B: get_user_identity(id) liefert exakt das Login-Shape (_build_user_dict)
    des Ziel-Users, ohne Passwort/Digest — für die Admin-Impersonation."""
    users = [
        {"ID": 1, "POSITION": 1, "NAME": "Admin", "DESCRIP": "", "ADMIN": 1,
         "RIGHTS": 0, "HIDE": 0, "WDUTIES": 0, "WABSENCES": 0, "WOVERTIMES": 0,
         "WNOTES": 0, "WDEVIATION": 0, "WCYCLEASS": 0, "WSWAPONLY": 0,
         "WPAST": 0, "ADDEMPL": 0, "SHOWABS": 0, "SHOWNOTES": 0,
         "SHOWSTATS": 0, "BACKUP": 0},
        {"ID": 2, "POSITION": 2, "NAME": "Leser", "DESCRIP": "", "ADMIN": 0,
         "RIGHTS": 0, "HIDE": 0, "WDUTIES": 0, "WABSENCES": 0, "WOVERTIMES": 0,
         "WNOTES": 0, "WDEVIATION": 0, "WCYCLEASS": 0, "WSWAPONLY": 0,
         "WPAST": 0, "ADDEMPL": 0, "SHOWABS": 1, "SHOWNOTES": 0,
         "SHOWSTATS": 0, "BACKUP": 0},
        {"ID": 3, "POSITION": 3, "NAME": "Versteckt", "DESCRIP": "", "ADMIN": 0,
         "RIGHTS": 0, "HIDE": 1, "WDUTIES": 0, "WABSENCES": 0, "WOVERTIMES": 0,
         "WNOTES": 0, "WDEVIATION": 0, "WCYCLEASS": 0, "WSWAPONLY": 0,
         "WPAST": 0, "ADDEMPL": 0, "SHOWABS": 0, "SHOWNOTES": 0,
         "SHOWSTATS": 0, "BACKUP": 0},
    ]
    db = make_db(tmp_path, {"5USER": users})
    # Nicht-Admin-Ziel: identische Felder wie _build_user_dict desselben Satzes
    raw = next(x for x in db._read("USER") if x.get("ID") == 2)
    assert db.get_user_identity(2) == db._build_user_dict(raw)
    ident = db.get_user_identity(2)
    assert ident["ID"] == 2 and ident["role"] == "Leser"
    # SHOWABS=1 (anonymisiert) → Modus 1, aber sichtbar
    assert ident["SHOWABS_MODE"] == 1 and ident["SHOWABS"] is True
    # Nicht-Admin trägt keine Admin-Rechte
    assert ident["WDUTIES"] is False and ident["ACCADMWND"] is False
    # Admin-Ziel: alle Flags True (wie beim Admin-Login)
    assert db.get_user_identity(1)["role"] == "Admin"
    assert db.get_user_identity(1)["WDUTIES"] is True
    # Versteckte (HIDE) und unbekannte ID ⇒ None
    assert db.get_user_identity(3) is None
    assert db.get_user_identity(999) is None


def test_verify_user_password_dict_contains_all_flags(tmp_path):
    """Das Session-Dict (_build_user_dict) enthält alle granularen Flags."""
    db = make_db(tmp_path, {"5USER": []})
    db.create_user({"NAME": "p1", "PASSWORD": "geheim123", "role": "Planer"})
    user = db.verify_user_password("p1", "geheim123")
    for flag in ("WDUTIES", "WABSENCES", "WOVERTIMES", "WNOTES", "WDEVIATION",
                 "WCYCLEASS", "WSWAPONLY", "WPAST", "ADDEMPL", "SHOWABS",
                 "SHOWNOTES", "SHOWSTATS", "BACKUP"):
        assert flag in user, flag
    # Planer-Defaults aus create_user: Schreibflags 1, ADDEMPL 0 (Opt-in)
    assert user["WDUTIES"] is True
    assert user["ADDEMPL"] is False
