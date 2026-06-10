"""Golden-Regression-Suite gegen die originale Schichtplaner-5-Beispieldatenbank.

Aktivierung über die Env-Var ``SP5_GOLDEN_DB`` (Pfad zum ``Daten``-Verzeichnis
mit den 30 ``5XXXX.DBF``/``5XXXX-L.DBF``-Paaren der Original-Beispiel-DB).
Ohne die Variable wird das gesamte Modul übersprungen. Die DB selbst bleibt
lokal (Referenzmaterial, nie committen) und liegt außerhalb des Repos.

Beispiel:

    SP5_GOLDEN_DB=/pfad/zu/sp5/Daten .venv/bin/python -m pytest tests/test_golden_sample_db.py -v

Datenschutz: Es werden ausschließlich unkritische Stammdaten geprüft (IDs,
Zahlen, Masken, Farben sowie Namen von Schichten, Gruppen, Urlaubsarten,
Feiertagen und Arbeitsplätzen). Keine Personennamen aus 5EMPL/5USER.
"""

import hashlib
import os
from pathlib import Path

import pytest

from sp5lib.dbf_reader import read_dbf

_GOLDEN_DB = os.environ.get("SP5_GOLDEN_DB")
if not _GOLDEN_DB:
    pytest.skip(
        "SP5_GOLDEN_DB nicht gesetzt – Golden-Tests benötigen den Pfad zum "
        "Daten-Verzeichnis der originalen SP5-Beispieldatenbank "
        "(lokales Referenzmaterial, nicht im Repo)",
        allow_module_level=True,
    )

DB = Path(_GOLDEN_DB)


def load(table: str) -> list[dict]:
    return read_dbf(str(DB / f"{table}.DBF"))


def by_id(records: list[dict]) -> dict[int, dict]:
    return {r["ID"]: r for r in records}


def as_bytes(value) -> bytes:
    """Binäre C-Felder (DIGEST/CREATIME/UUID) liefert die Lib als bytes."""
    if isinstance(value, bytes):
        return value
    return value.encode("latin-1")  # Fallback für ältere Lib-Stände


# Exakte Recordzahlen aller 30 Haupttabellen der Beispiel-DB.
EXPECTED_COUNTS = {
    "5ABSEN": 0,
    "5BOOK": 0,
    "5BUILD": 1,
    "5CYASS": 0,
    "5CYCLE": 1,
    "5CYENT": 15,
    "5CYEXC": 0,
    "5DADEM": 0,
    "5EMACC": 0,
    "5EMPL": 30,
    "5GRACC": 3,
    "5GRASG": 36,
    "5GROUP": 10,
    "5HOBAN": 0,
    "5HOLID": 96,
    "5LEAEN": 0,
    "5LEAVT": 8,
    "5MASHI": 0,
    "5NOTE": 0,
    "5OVER": 0,
    "5PERIO": 0,
    "5RESTR": 0,
    "5SHDEM": 0,
    "5SHIFT": 5,
    "5SPDEM": 0,
    "5SPSHI": 0,
    "5USER": 6,
    "5USETT": 1,
    "5WOPL": 4,
    "5XCHAR": 4,
}


# ─── Datenbank-Layout ─────────────────────────────────────────────────────────


def test_golden_db_directory_complete():
    assert DB.is_dir(), f"SP5_GOLDEN_DB zeigt auf kein Verzeichnis: {DB}"
    for table in EXPECTED_COUNTS:
        assert (DB / f"{table}.DBF").is_file(), f"{table}.DBF fehlt"
        assert (DB / f"{table}-L.DBF").is_file(), f"{table}-L.DBF fehlt"


@pytest.mark.parametrize("table,count", sorted(EXPECTED_COUNTS.items()))
def test_table_readable_with_exact_record_count(table, count):
    records = load(table)
    assert len(records) == count


def test_empty_tables_return_empty_list():
    # Leere Haupttabellen (Bewegungs-/Bedarfsdaten) liefern [] statt zu crashen.
    for table, count in EXPECTED_COUNTS.items():
        if count == 0:
            assert load(table) == []
    # Alle 30 Änderungsjournal-Begleittabellen (…-L) sind in der Beispiel-DB leer.
    for table in EXPECTED_COUNTS:
        assert read_dbf(str(DB / f"{table}-L.DBF")) == []


# ─── 5SHIFT — Schichtarten (vollständig) ──────────────────────────────────────

# (ID, NAME, SHORTNAME, POSITION, STARTEND0, DURATION0, COLORTEXT, COLORBAR,
#  COLORBK als Windows-COLORREF-Int 0x00BBGGRR, BOLD, HIDE)
EXPECTED_SHIFTS = [
    (1, "Frühschicht", "F", 1, "06:00-14:00", 8.0, 16777215, 16777215, 16744448, 0, 0),
    (2, "Spätschicht", "S", 2, "14:00-22:00", 8.0, 0, 0, 33023, 0, 0),
    (3, "Nachtschicht", "N", 3, "22:00-06:00", 8.0, 0, 0, 16711935, 0, 0),
    (35, "Bereitschaftsdienst", "B", 5, "12:00-00:00", 4.0, 16777215, 16744448, 255, 1, 1),
    (36, "Tagschicht", "T", 4, "08:00-16:00", 8.0, 16711935, 4194368, 16777215, 1, 0),
]


def test_shift_catalog_complete():
    shifts = by_id(load("5SHIFT"))
    assert sorted(shifts) == [1, 2, 3, 35, 36]
    for sid, name, short, pos, startend0, dur0, ctext, cbar, cbk, bold, hide in EXPECTED_SHIFTS:
        s = shifts[sid]
        assert s["NAME"] == name
        assert s["SHORTNAME"] == short
        assert s["POSITION"] == pos
        assert s["STARTEND0"] == startend0
        assert s["DURATION0"] == dur0
        # Farben sind Windows-COLORREF als Integer (R=v&0xFF, G, B aufsteigend).
        assert s["COLORTEXT"] == ctext
        assert s["COLORBAR"] == cbar
        assert s["COLORBK"] == cbk
        assert s["BOLD"] == bold
        assert s["HIDE"] == hide
        assert s["NOEXTRA"] == 0


def test_shift_startend_slots():
    shifts = by_id(load("5SHIFT"))
    # Frühschicht: Slots Mo–So (0–6) befüllt, Zusatzslot 7 leer.
    f = shifts[1]
    for i in range(7):
        assert f[f"STARTEND{i}"] == "06:00-14:00"
        assert f[f"DURATION{i}"] == 8.0
    assert f["STARTEND7"] == ""
    # Bereitschaftsdienst: alle 8 Slots inkl. Zusatzslot befüllt.
    b = shifts[35]
    for i in range(8):
        assert b[f"STARTEND{i}"] == "12:00-00:00"
    # Tagschicht: nur Mo–Fr (0–4) befüllt.
    t = shifts[36]
    for i in range(5):
        assert t[f"STARTEND{i}"] == "08:00-16:00"
    for i in range(5, 8):
        assert t[f"STARTEND{i}"] == ""
        assert t[f"DURATION{i}"] == 0.0


def test_utf16_umlaut_decoding():
    # Anzeigetexte sind UTF-16LE: Umlaut muss korrekt ankommen.
    shifts = by_id(load("5SHIFT"))
    assert shifts[1]["NAME"] == "Frühschicht"
    assert "ü" in shifts[1]["NAME"]
    assert shifts[2]["NAME"] == "Spätschicht"


# ─── 5LEAVT — Urlaubs-/Abwesenheitsarten ─────────────────────────────────────

# (ID, NAME, SHORTNAME, COLORBAR, CHARGETYP, CHARGEHRS, ENTITLED, STDENTIT, CARRYFWD)
EXPECTED_LEAVE_TYPES = [
    (1, "Urlaub", "Ur", 16711680, 1, 0.0, 1, 30.0, 1),
    (3, "Krankheit", "Kr", 255, 1, 0.0, 0, 0.0, 0),
    (13, "Unbezahlter Urlaub", "UU", 4227200, 0, 0.0, 0, 0.0, 0),
    (14, "Sonderurlaub", "Su", 8388736, 1, 0.0, 1, 2.0, 0),
    (15, "Fortbildung", "Fb", 8388736, 2, 8.25, 0, 0.0, 0),
    (16, "Hochzeit", "Hz", 16711935, 0, 0.0, 0, 0.0, 0),
    (17, "Arztbesuch", "Ab", 33023, 0, 0.0, 0, 0.0, 0),
    (18, "Dienstreise", "DR", 128, 2, 6.0, 0, 30.0, 0),
]


def test_leave_type_catalog():
    leavt = by_id(load("5LEAVT"))
    assert sorted(leavt) == [1, 3, 13, 14, 15, 16, 17, 18]
    for lid, name, short, cbar, chargetyp, chargehrs, entitled, stdentit, carryfwd in EXPECTED_LEAVE_TYPES:
        lt = leavt[lid]
        assert lt["NAME"] == name
        assert lt["SHORTNAME"] == short
        assert lt["COLORBAR"] == cbar  # Urlaub blau (16711680), Krankheit rot (255)
        assert lt["CHARGETYP"] == chargetyp  # 0=keine, 1=ganztägig, 2=feste Stunden
        assert lt["CHARGEHRS"] == chargehrs
        assert lt["ENTITLED"] == entitled
        assert lt["STDENTIT"] == stdentit  # Standardanspruch, z.B. 30 Tage Urlaub
        assert lt["CARRYFWD"] == carryfwd  # Resttage übertragen nur bei Urlaub
        # 8er-Gültigkeitsmaske (Mo–So + Feiertagsslot), hier überall voll.
        assert lt["VALIDDAYS"] == "1 1 1 1 1 1 1 1"


# ─── 5HOLID — Feiertage ──────────────────────────────────────────────────────


def test_holiday_calendar():
    holidays = load("5HOLID")
    assert len(holidays) == 96
    # Neujahr 2013 als ISO-Datum.
    neujahr = [h for h in holidays if h["NAME"] == "Neujahr" and h["DATE"] == "2013-01-01"]
    assert len(neujahr) == 1
    assert neujahr[0]["INTERVAL"] == 0
    # 8 bundesweite Feiertage × 12 Jahre (2013–2024).
    years = sorted({int(h["DATE"][:4]) for h in holidays})
    assert years == list(range(2013, 2025))
    names = sorted({h["NAME"] for h in holidays})
    assert names == [
        "1. Weihnachtsfeiertag",
        "2. Weihnachtsfeiertag",
        "Christi Himmelfahrt",
        "Karfreitag",
        "Neujahr",
        "Ostermontag",
        "Pfingstmontag",
        "Tag der dt. Einheit",
    ]
    for year in years:
        assert sum(1 for h in holidays if h["DATE"].startswith(str(year))) == 8
    # Feste Daten stichprobenartig (Weihnachten ist jahrfix).
    assert any(h["DATE"] == "2013-12-25" and h["NAME"] == "1. Weihnachtsfeiertag" for h in holidays)
    assert any(h["DATE"] == "2024-10-03" and h["NAME"] == "Tag der dt. Einheit" for h in holidays)


# ─── 5XCHAR — Zuschlagsarten ─────────────────────────────────────────────────


def test_extra_charge_definitions():
    xchar = by_id(load("5XCHAR"))
    assert sorted(xchar) == [1, 2, 3, 4]
    # Sonntagstunden: nur So in der 7er-Maske, feiertagsunabhängig (HOLRULE=2).
    so = xchar[1]
    assert so["NAME"] == "Sonntagstunden"
    assert so["VALIDDAYS"] == "0 0 0 0 0 0 1"
    assert so["HOLRULE"] == 2
    # Samstagsstunden: 13:00–20:00 als Minuten ab Mitternacht (780–1200), nur Sa.
    sa = xchar[2]
    assert sa["NAME"] == "Samstagsstunden"
    assert sa["START"] == 780  # 13 * 60
    assert sa["END"] == 1200  # 20 * 60
    assert sa["VALIDDAYS"] == "0 0 0 0 0 1 0"
    # Nachtstunden: 20:00–06:00 (1200–360), alle Wochentage, HOLRULE=0.
    na = xchar[3]
    assert na["NAME"] == "Nachtstunden"
    assert na["START"] == 1200  # 20 * 60
    assert na["END"] == 360  # 6 * 60
    assert na["VALIDDAYS"] == "1 1 1 1 1 1 1"
    assert na["HOLRULE"] == 0
    # Feiertagsstunden: nur an Feiertagen (HOLRULE=1).
    ft = xchar[4]
    assert ft["NAME"] == "Feiertagsstunden"
    assert ft["HOLRULE"] == 1
    assert ft["VALIDDAYS"] == "1 1 1 1 1 1 1"
    # DATE-Feld trägt im Original den Literaltext 'invalid' (regelbasiert,
    # kein fixes Datum) – die Lib liefert dafür None.
    for x in xchar.values():
        assert x["DATE"] is None


# ─── 5GROUP — Hierarchie über SUPERID ────────────────────────────────────────

EXPECTED_GROUP_TREE = {
    # ID: (NAME, SUPERID)
    1: ("Alle Mitarbeiter", 0),
    2: ("Team A", 61),
    51: ("Team C", 61),
    53: ("Verwaltung", 1),
    54: ("Team B", 61),
    55: ("Schichtleitung", 61),
    61: ("Produktion", 1),
    63: ("Vertrieb", 53),
    64: ("Einkauf", 53),
    65: ("Personalwesen", 53),
}


def test_group_hierarchy():
    groups = by_id(load("5GROUP"))
    assert sorted(groups) == sorted(EXPECTED_GROUP_TREE)
    for gid, (name, superid) in EXPECTED_GROUP_TREE.items():
        assert groups[gid]["NAME"] == name
        assert groups[gid]["SUPERID"] == superid
    # Genau eine Wurzel (SUPERID=0), alle anderen SUPERIDs existieren als Gruppe.
    roots = [g for g in groups.values() if g["SUPERID"] == 0]
    assert len(roots) == 1 and roots[0]["ID"] == 1
    for g in groups.values():
        if g["SUPERID"] != 0:
            assert g["SUPERID"] in groups
    # Default-Plan-Hintergrund hellgrau, Tagesbedarfsmaske leer (8er-Maske).
    for g in groups.values():
        assert g["CBKSCHED"] == 14737632
        assert g["DAILYDEM"] == "0 0 0 0 0 0 0 0"


# ─── 5WOPL — Arbeitsplätze ───────────────────────────────────────────────────


def test_workplace_catalog():
    wopl = by_id(load("5WOPL"))
    assert {wid: (w["NAME"], w["SHORTNAME"]) for wid, w in wopl.items()} == {
        1: ("Maschine 1", "M1"),
        3: ("Maschine 2", "M2"),
        4: ("Heimarbeit", "HA"),
        5: ("Wareneingang", "WE"),
    }


# ─── 5CYCLE / 5CYENT — Schichtmodell ─────────────────────────────────────────


def test_cycle_model_structure():
    cycles = load("5CYCLE")
    assert len(cycles) == 1
    c = cycles[0]
    assert c["ID"] == 8
    assert c["NAME"] == "3W-Wechselschicht"
    assert c["SIZE"] == 3  # 3 ...
    assert c["UNIT"] == 1  # ... Wochen

    entries = load("5CYENT")
    assert len(entries) == 15
    assert all(e["CYCLEEID"] == 8 for e in entries)
    assert all(e["WORKPLACID"] == 0 for e in entries)  # 0 = kein Arbeitsplatz
    # F-S-N-Muster: Mo–Fr je Woche, freie Wochenenden (Index 5/6, 12/13, 19/20).
    index_to_shift = {e["INDEX"]: e["SHIFTID"] for e in entries}
    expected = {}
    expected.update({i: 1 for i in range(0, 5)})  # Woche 1: Frühschicht
    expected.update({i: 2 for i in range(7, 12)})  # Woche 2: Spätschicht
    expected.update({i: 3 for i in range(14, 19)})  # Woche 3: Nachtschicht
    assert index_to_shift == expected


# ─── 5EMPL — Mitarbeiter (nur strukturell, keine Personennamen) ──────────────


def test_employee_table_structural():
    employees = load("5EMPL")
    assert len(employees) == 30
    # IDs laufen lückenlos von 40–69.
    assert sorted(e["ID"] for e in employees) == list(range(40, 70))
    # Erster Mitarbeiter (POSITION=1, ID=40): Sollstunden und Arbeitstagsmaske.
    first = min(employees, key=lambda e: e["POSITION"])
    assert first["ID"] == 40
    assert first["POSITION"] == 1
    assert first["HRSDAY"] == 7.7
    assert first["HRSWEEK"] == 38.5
    assert first["HRSMONTH"] == 154.0
    # Arbeitstagsmaske: Mo–Fr arbeiten, Sa/So/Feiertagsslot frei – bei allen 30.
    for e in employees:
        assert e["WORKDAYS"] == "1 1 1 1 1 0 0 0"
    # Anzeigetext-Felder kommen als String an (UTF-16-Decode liefert str).
    assert isinstance(first["NAME"], str) and isinstance(first["FIRSTNAME"], str)


# ─── 5USER — Benutzerkonten (nur strukturell) ────────────────────────────────


def test_user_table_structural():
    users = by_id(load("5USER"))
    # Eigener Nummernkreis 251–256.
    assert sorted(users) == [251, 252, 253, 254, 255, 256]
    # Genau ein Administrator: ID=251.
    assert [uid for uid, u in users.items() if u["ADMIN"] == 1] == [251]
    assert users[252]["RIGHTS"] == 1
    assert users[253]["RIGHTS"] == 2 and users[254]["RIGHTS"] == 2 and users[255]["RIGHTS"] == 2
    # 20er-Masken für Kategorien-/Report-Rechte.
    for u in users.values():
        assert u["CATEGORY"] == "1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
        assert u["REPORT"] == "1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1"


def test_user_digest_admin_is_md5_of_empty_string():
    # DIGEST ist ungesalzenes MD5 (16 Byte binär). Beim Admin (ID=251) ist kein
    # Passwort gesetzt → MD5("") – reiner Formatbeleg, kein Personenbezug.
    users = by_id(load("5USER"))
    digest = as_bytes(users[251]["DIGEST"])
    assert digest == hashlib.md5(b"").digest()
    assert digest.hex() == "d41d8cd98f00b204e9800998ecf8427e"
    assert len(digest) == 16


def test_user_digest_always_16_bytes():
    # Lücke L-1 (parity-lib-decode.md) behoben: Binärfelder kommen verlustfrei
    # als 16 Bytes an (BINARY_C_FIELDS in dbf_reader).
    users = by_id(load("5USER"))
    for uid, u in users.items():
        assert len(as_bytes(u["DIGEST"])) == 16, f"DIGEST von USER {uid} verstümmelt"


# ─── 5USETT — Programmeinstellungen (Singleton) ──────────────────────────────


def test_settings_singleton():
    settings = load("5USETT")
    assert len(settings) == 1
    s = settings[0]
    assert s["ID"] == 0
    assert s["LOGIN"] == 0  # kein Login-Zwang
    assert s["ANOANAME"] == "Abwesend"
    assert s["ANOASHORT"] == "X"
    assert s["ANOACRBAR"] == 16711680  # Balken rot (COLORREF)
    assert s["ANOACRBK"] == 16777215  # Hintergrund weiß
    assert s["CHANGELOG"] == 0  # passt zu den durchgehend leeren -L-Tabellen


# ─── 5BUILD — DB-Identität ───────────────────────────────────────────────────


def test_build_record():
    builds = load("5BUILD")
    assert len(builds) == 1
    b = builds[0]
    assert b["ID"] == 1
    assert b["BUILD"] == 1
    # UUID: 16-Byte-GUID, binär in einem C-Feld.
    uuid = as_bytes(b["UUID"])
    assert len(uuid) == 16
    assert uuid.hex() == "a8304c55b9631f448c6b0ced9a6aabf9"
    # CREATIME: Windows-FILETIME, 8 Byte binär (2013-03-01 13:28:43 UTC).
    creatime = as_bytes(b["CREATIME"])
    assert len(creatime) == 8
    assert creatime.hex() == "4009a3b18016ce01"
