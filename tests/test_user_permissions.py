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
        # RIGHTS=0 = voller Schreibmodus → die gespeicherten W*-Flags gelten 1:1.
        {"ID": 2, "POSITION": 2, "NAME": "Planer", "DESCRIP": "", "ADMIN": 0,
         "RIGHTS": 0, "HIDE": 0, "WDUTIES": 1, "WABSENCES": 1, "WOVERTIMES": 0,
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
    # Nicht-Admin (RIGHTS=0): Flags aus dem 5USER-Satz
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
        # RIGHTS=1 = Nur-Leserechte → Rolle „Leser", Schreibflags gesperrt.
        {"ID": 2, "POSITION": 2, "NAME": "Leser", "DESCRIP": "", "ADMIN": 0,
         "RIGHTS": 1, "HIDE": 0, "WDUTIES": 0, "WABSENCES": 0, "WOVERTIMES": 0,
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


def test_create_user_honours_explicit_permission_overrides(tmp_path):
    """Lücke #1: einzelne 5USER-Schreibrechte überschreiben die Rollen-Defaults."""
    db = make_db(tmp_path, {"5USER": []})
    rec = db.create_user({
        "NAME": "feinplaner", "PASSWORD": "geheim123", "role": "Planer",
        "WPAST": False, "WSWAPONLY": True, "ADDEMPL": True,
    })
    perms = db.get_user_permissions(rec["ID"])
    assert perms["wduties"] is True       # Rollen-Default bleibt
    assert perms["wpast"] is False        # explizit aus
    assert perms["wswaponly"] is True     # explizit an
    assert perms["addempl"] is True       # explizit an


def test_update_user_honours_explicit_permission_overrides(tmp_path):
    db = make_db(tmp_path, {"5USER": []})
    uid = db.create_user(
        {"NAME": "u1", "PASSWORD": "geheim123", "role": "Planer"}
    )["ID"]
    # ohne Rolle: einzelne Flags ändern
    db.update_user(uid, {"WABSENCES": False, "ADDEMPL": True})
    perms = db.get_user_permissions(uid)
    assert perms["wabsences"] is False
    assert perms["addempl"] is True
    assert perms["wduties"] is True       # unberührt
    # Rolle Leser (RIGHTS=1 = Nur-Leserechte): der Read-only-Modus setzt sich
    # durch — ein Leser hat keine Schreibrechte, auch ein explizites WNOTES=True
    # wird gesperrt (faithful zum Original: RIGHTS=1 verweigert jeden Schreibzugriff).
    db.update_user(uid, {"role": "Leser", "WNOTES": True})
    perms = db.get_user_permissions(uid)
    assert perms["wnotes"] is False
    assert perms["wduties"] is False      # Leser: read-only


def _user(uid, name, *, admin=0, rights=0, wflags=1):
    """5USER-Satz mit allen W*-Flags = wflags (Default voll schreibend)."""
    rec = {"ID": uid, "POSITION": uid, "NAME": name, "DESCRIP": "", "ADMIN": admin,
           "RIGHTS": rights, "HIDE": 0, "SHOWABS": 0, "SHOWNOTES": 1, "SHOWSTATS": 1}
    for f in ("WDUTIES", "WABSENCES", "WOVERTIMES", "WNOTES", "WDEVIATION",
              "WCYCLEASS", "WSWAPONLY", "WPAST", "ADDEMPL", "BACKUP"):
        rec[f] = wflags
    return rec


def test_p0_2_role_from_rights_mode(tmp_path):
    """P0-2: 5USER.RIGHTS ist ein MODUS, kein Boolean. RIGHTS=0 (voll) und =2
    (differenziert) sind „Planer", RIGHTS=1/3 (Nur-Lesen) sind „Leser", ADMIN=1
    ist „Admin". Früher wurde RIGHTS=0/2 fälschlich als „Leser" angezeigt."""
    users = [
        _user(1, "Admin", admin=1, rights=0),
        _user(2, "Vollplaner", rights=0),     # volle Lese-/Schreibrechte
        _user(3, "NurLesen", rights=1),        # Nur-Leserechte
        _user(4, "Differenziert", rights=2),   # differenzierte Rechte
        _user(5, "NurLesen3", rights=3),       # Variante read-only
    ]
    db = make_db(tmp_path, {"5USER": users})
    roles = {r["ID"]: db._build_user_dict(r)["role"] for r in db._read("USER")}
    assert roles == {1: "Admin", 2: "Planer", 3: "Leser", 4: "Planer", 5: "Leser"}


def test_p0_2_readonly_mode_blocks_writes_despite_stale_flags(tmp_path):
    """P0-2 Enforcement: ein RIGHTS=1/3-User (Nur-Lesen) bekommt KEINE Schreibrechte,
    selbst wenn die gespeicherten W*-Flags (Altbestand echter DBs) noch 1 sind —
    das Original gated über RIGHTS, nicht über die Einzelflags. Verhindert, dass
    osp5 einem read-only-Konto fälschlich Schreiben erlaubt."""
    users = [_user(7, "AltLeser", rights=1, wflags=1)]  # read-only, aber W*=1
    db = make_db(tmp_path, {"5USER": users})
    ud = db._build_user_dict(next(r for r in db._read("USER") if r["ID"] == 7))
    assert ud["role"] == "Leser"
    for f in ("WDUTIES", "WABSENCES", "WOVERTIMES", "WNOTES", "WDEVIATION",
              "WCYCLEASS", "WSWAPONLY", "WPAST", "ADDEMPL", "BACKUP"):
        assert ud[f] is False, f
    perms = db.get_user_permissions(7)
    assert perms is not None
    for key in ("wduties", "wabsences", "wpast", "addempl", "backup"):
        assert perms[key] is False, key
    # Anzeige-/Sichtbarkeitsflags bleiben (Leser darf weiterhin sehen)
    assert perms["shownotes"] is True and perms["showstats"] is True


def test_p0_2_full_mode_keeps_stored_write_flags(tmp_path):
    """RIGHTS=0 (voll schreibend) lässt die gespeicherten W*-Flags durch (Planer),
    RIGHTS=2 (differenziert) ebenso — nur der Read-only-Modus sperrt pauschal."""
    users = [_user(8, "Voll", rights=0, wflags=1), _user(9, "Diff", rights=2, wflags=1)]
    db = make_db(tmp_path, {"5USER": users})
    for uid in (8, 9):
        ud = db._build_user_dict(next(r for r in db._read("USER") if r["ID"] == uid))
        assert ud["role"] == "Planer"
        assert ud["WDUTIES"] is True and ud["WABSENCES"] is True


def test_p0_2_create_user_writes_correct_rights_mode(tmp_path):
    """create_user/update_user schreiben den korrekten RIGHTS-Modus: Planer→0,
    Leser→1; und die zurückgelesene Rolle stimmt damit überein (kein Selbst-Widerspruch)."""
    db = make_db(tmp_path, {"5USER": []})
    planer = db.create_user({"NAME": "pl", "PASSWORD": "geheim123", "role": "Planer"})
    leser = db.create_user({"NAME": "le", "PASSWORD": "geheim123", "role": "Leser"})
    pr = next(r for r in db._read("USER") if r["ID"] == planer["ID"])
    lr = next(r for r in db._read("USER") if r["ID"] == leser["ID"])
    assert int(pr["RIGHTS"]) == 0 and db._build_user_dict(pr)["role"] == "Planer"
    assert int(lr["RIGHTS"]) == 1 and db._build_user_dict(lr)["role"] == "Leser"
