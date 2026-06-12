"""Differenztest der Berechnungsschicht gegen das laufende Original.

Aktivierung über ``SP5_GOLDEN_DB`` (Pfad zum ``Daten``-Verzeichnis der
Original-Beispieldatenbank, lokales Referenzmaterial, nie committen).

Die erwarteten Werte wurden am **laufenden** Original Schichtplaner5 abgelesen:
Das Programm zeigt in seiner Personaltabelle für den Auswertungszeitraum
1.1.–31.12.2026 für jeden der 30 Mitarbeiter der Beispieldatenbank
Sollstunden = 2009,70 h. Dieser Test stellt sicher, dass
``sp5lib.calculations.get_nominal_hours`` exakt dieselben Werte liefert —
eine Live-Validierung der Berechnungsparität über die statische Spec-
Ableitung hinaus (vgl. tests/test_calculations.py mit dem normativen
157,85-h-Handbuchfall).

Reproduktion der Orakelwerte (lokal, nicht in CI): das Original unter wine +
Xvfb starten, die Beispiel-DB laden, Personaltabelle ablesen — siehe das lokale
Harness-Skript im Referenzmaterial (``_sp5-reference/wine-harness/``).

Datenschutz: geprüft werden ausschließlich aggregierte Stundenwerte und
Mitarbeiter-IDs, keine Personennamen.
"""

import os
from datetime import date
from pathlib import Path

import pytest

import sp5lib.calculations as calc
from sp5lib.dbf_reader import read_dbf

_GOLDEN_DB = os.environ.get("SP5_GOLDEN_DB")
if not _GOLDEN_DB:
    pytest.skip(
        "SP5_GOLDEN_DB nicht gesetzt – der Orakeltest vergleicht gegen die "
        "Werte des Original-Programms auf der Beispiel-DB (lokales "
        "Referenzmaterial, nicht im Repo)",
        allow_module_level=True,
    )

DB = Path(_GOLDEN_DB)

# Am laufenden Original abgelesen (Personaltabelle, Zeitraum 2026):
ORACLE_NOMINAL_2026 = 2009.70


def _employees():
    return read_dbf(str(DB / "5EMPL.DBF"))


def _holidays():
    return calc.holiday_calendar(read_dbf(str(DB / "5HOLID.DBF")))


def test_nominal_hours_match_original_for_all_employees():
    """Alle 30 MA: lib-Sollstunden 2026 == Anzeige des Originals (2009,70 h)."""
    holidays = _holidays()
    employees = _employees()
    assert len(employees) == 30
    for emp in employees:
        ctx = calc.EmployeeContext.from_record(emp)
        soll = calc.get_nominal_hours(
            ctx, date(2026, 1, 1), date(2026, 12, 31), holidays=holidays
        )
        assert soll == pytest.approx(ORACLE_NOMINAL_2026), (
            f"Mitarbeiter-ID {emp.get('ID')}: lib {soll:.2f} h != "
            f"Original {ORACLE_NOMINAL_2026:.2f} h"
        )


def test_nominal_hours_oracle_is_not_trivially_zero():
    """Schutz gegen eine versehentlich leere Berechnung (Sollstunden > 0)."""
    holidays = _holidays()
    ctx = calc.EmployeeContext.from_record(_employees()[0])
    soll = calc.get_nominal_hours(
        ctx, date(2026, 1, 1), date(2026, 12, 31), holidays=holidays
    )
    assert soll > 1000.0
