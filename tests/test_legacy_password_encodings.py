"""Regression: Original-SP5-Passwörter mit nicht-ASCII-/WideString-Kodierung.

Schichtplaner5 (Delphi/Windows) speichert das Passwort als ungesalzenes 16-Byte-
MD5 in 5USER.DIGEST. Welche Byte-Kodierung des Passworts gehasht wurde, hängt von
der SP5-Version/dem Code-Pfad ab, der es gesetzt hat: Windows-ANSI (CP1252 — was
deutsche Installationen nutzen) oder Delphi-WideString (UTF-16-LE). Reine ASCII-
Passwörter sind unter UTF-8/CP1252 identisch und gingen schon immer; Umlaut-
Passwörter (CP1252) und WideString-Konten (UTF-16-LE) wurden von der reinen
UTF-8-Prüfung NIE akzeptiert — der echte Grund, warum sich Original-Accounts nicht
anmelden konnten. `verify_user_password` probiert die Kodierungen jetzt durch.

Synthetisch (kein Golden-DB nötig), läuft in CI.
"""

import hashlib
import struct
from datetime import date

import pytest

from sp5lib.database import SP5Database
from sp5lib.dbf_writer import append_record, get_table_fields

# 5USER-Minimalschema: ID, NAME (C/200, UTF-16 in SP5), DIGEST (C/16, binär), HIDE (L)
_SPEC = [("ID", "N", 11, 0), ("NAME", "C", 200, 0), ("DIGEST", "C", 16, 0), ("HIDE", "L", 1, 0)]


def _field_descriptor(name, ftype, length, dec=0):
    name_bytes = name.upper().encode("ascii")[:11].ljust(11, b"\x00")
    return name_bytes + ftype.encode("ascii") + b"\x00" * 4 + bytes([length, dec]) + b"\x00" * 14


def _make_5user(tmp_path):
    record_size = 1 + sum(f[2] for f in _SPEC)
    header_size = 32 + 32 * len(_SPEC) + 1
    hdr = bytearray(32)
    hdr[0] = 0x03
    today = date.today()
    hdr[1], hdr[2], hdr[3] = today.year % 100, today.month, today.day
    struct.pack_into("<I", hdr, 4, 0)
    struct.pack_into("<H", hdr, 8, header_size)
    struct.pack_into("<H", hdr, 10, record_size)
    body = b"".join(_field_descriptor(*f) for f in _SPEC)
    path = tmp_path / "5USER.DBF"
    path.write_bytes(bytes(hdr) + body + b"\x0d" + b"\x1a")
    return str(path)


def _seed(tmp_path):
    """A 5USER table with one account per original digest encoding."""
    path = _make_5user(tmp_path)
    f = get_table_fields(path)
    append_record(path, f, {"ID": 1, "NAME": "WideUser", "DIGEST": hashlib.md5("geheim".encode("utf-16-le")).digest(), "HIDE": False})
    append_record(path, f, {"ID": 2, "NAME": "UmlautUser", "DIGEST": hashlib.md5("Müller".encode("cp1252")).digest(), "HIDE": False})
    append_record(path, f, {"ID": 3, "NAME": "AsciiUser", "DIGEST": hashlib.md5(b"Test1234").digest(), "HIDE": False})
    return SP5Database(str(tmp_path))


@pytest.mark.parametrize(
    "name,password",
    [
        ("WideUser", "geheim"),   # Delphi WideString (UTF-16-LE) — vorher abgelehnt
        ("UmlautUser", "Müller"), # deutsches ANSI (CP1252) — vorher abgelehnt
        ("AsciiUser", "Test1234"),# ASCII (UTF-8) — ging schon immer
    ],
)
def test_legacy_md5_login_accepts_all_original_encodings(tmp_path, name, password):
    db = _seed(tmp_path)
    user = db.verify_user_password(name, password)
    assert user is not None, f"{name} konnte sich mit korrektem Passwort nicht anmelden"
    assert user["NAME"] == name


@pytest.mark.parametrize(
    "name,password",
    [
        ("WideUser", "falsch"),
        ("UmlautUser", "Muller"),   # ohne Umlaut = falsches Passwort
        ("AsciiUser", "test1234"),  # falsche Groß-/Kleinschreibung
    ],
)
def test_legacy_md5_login_still_rejects_wrong_password(tmp_path, name, password):
    db = _seed(tmp_path)
    assert db.verify_user_password(name, password) is None


def test_login_diagnostics_flags_password_states(tmp_path):
    """Privacy-safe Diagnose unterscheidet die Digest-Zustände, die ein Operator
    aus den Portainer-Logs lesen muss (ohne Passwort/Digest zu leaken)."""
    path = _make_5user(tmp_path)
    f = get_table_fields(path)
    append_record(path, f, {"ID": 1, "NAME": "NoPwAdmin", "DIGEST": hashlib.md5(b"").digest(), "HIDE": False})
    append_record(path, f, {"ID": 2, "NAME": "Disabled", "DIGEST": b"\x00" * 16, "HIDE": False})
    append_record(path, f, {"ID": 3, "NAME": "Normal", "DIGEST": hashlib.md5("geheim".encode("utf-16-le")).digest(), "HIDE": False})
    db = SP5Database(str(tmp_path))

    empty = db.login_diagnostics("NoPwAdmin")
    assert empty["digest_is_empty_md5"] is True and empty["digest_all_zero"] is False

    disabled = db.login_diagnostics("Disabled")
    assert disabled["digest_all_zero"] is True and disabled["digest_is_empty_md5"] is False

    normal = db.login_diagnostics("Normal")
    assert normal["digest_is_md5_shape"] is True
    assert normal["digest_all_zero"] is False and normal["digest_is_empty_md5"] is False
    assert normal["encodings_tried"] == ["utf-8", "cp1252", "utf-16-le"]

    assert db.login_diagnostics("Ghost") == {"user_found": False}
