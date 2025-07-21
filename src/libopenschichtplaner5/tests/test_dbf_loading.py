#!/usr/bin/env python3
"""
Praktisches Beispiel: Schichtplan-Analyse für einen Mitarbeiter
Zeigt die Verwendung der korrigierten Models und Loader.
"""

from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Tuple

from corrected_loaders import load_all_tables
from corrected_models import Employee, Shift, EmployeeShift, Absence, LeaveType


class SchichtplanAnalyzer:
    """Analysiert Schichtpläne und generiert Reports."""
    
    def __init__(self, dbf_dir: Path):
        print("Lade Daten...")
        self.data = load_all_tables(dbf_dir)
        
        # Erstelle Lookup-Dictionaries für schnellen Zugriff
        self.employees_by_id = {e.id: e for e in self.data.get('5EMPL', [])***REMOVED***
        self.shifts_by_id = {s.id: s for s in self.data.get('5SHIFT', [])***REMOVED***
        self.leave_types_by_id = {lt.id: lt for lt in self.data.get('5LEAVT', [])***REMOVED***
    
    def analyze_employee_schedule(self, employee_id: int, start_date: date, end_date: date):
        """Analysiert den Schichtplan eines Mitarbeiters."""
        employee = self.employees_by_id.get(employee_id)
        if not employee:
            print(f"Mitarbeiter {employee_id***REMOVED*** nicht gefunden!")
            return
        
        print(f"\n{'='*60***REMOVED***")
        print(f"SCHICHTPLAN-ANALYSE")
        print(f"Mitarbeiter: {employee.name***REMOVED***, {employee.firstname***REMOVED*** (Nr. {employee.number***REMOVED***)")
        print(f"Zeitraum: {start_date***REMOVED*** bis {end_date***REMOVED***")
        print(f"{'='*60***REMOVED***\n")
        
        # Hole alle Schichten im Zeitraum (aus 5MASHI)
        employee_shifts = [
            es for es in self.data.get('5MASHI', [])
            if es.employee_id == employee_id 
            and start_date <= es.date <= end_date
        ]
        
        # Hole alle Abwesenheiten im Zeitraum
        absences = [
            a for a in self.data.get('5ABSEN', [])
            if a.employee_id == employee_id
            and start_date <= a.date <= end_date
        ]
        
        # Sortiere nach Datum
        employee_shifts.sort(key=lambda x: x.date)
        absences.sort(key=lambda x: x.date)
        
        # Erstelle Kalender-Ansicht
        self._print_calendar(employee_shifts, absences, start_date, end_date)
        
        # Statistiken
        self._print_statistics(employee_shifts, absences)
        
        # Zuschläge berechnen
        self._calculate_surcharges(employee_shifts)
    
    def _print_calendar(self, shifts: List[EmployeeShift], absences: List[Absence], 
                       start_date: date, end_date: date):
        """Druckt eine Kalender-Ansicht."""
        print("KALENDER-ANSICHT:")
        print("-" * 60)
        
        # Erstelle Lookup für schnellen Zugriff
        shifts_by_date = {s.date: s for s in shifts***REMOVED***
        absences_by_date = {a.date: a for a in absences***REMOVED***
        
        current_date = start_date
        while current_date <= end_date:
            weekday = current_date.strftime("%a")
            date_str = current_date.strftime("%d.%m.%Y")
            
            # Prüfe ob Schicht oder Abwesenheit
            if current_date in shifts_by_date:
                shift = shifts_by_date[current_date]
                shift_info = self.shifts_by_id.get(shift.shift_id)
                if shift_info:
                    time = shift_info.get_weekday_time(current_date.weekday())
                    print(f"{weekday***REMOVED*** {date_str***REMOVED***: {shift_info.shortname:<3***REMOVED*** "
                          f"{shift_info.name:<20***REMOVED*** {time***REMOVED***")
                else:
                    print(f"{weekday***REMOVED*** {date_str***REMOVED***: Schicht ID {shift.shift_id***REMOVED***")
            elif current_date in absences_by_date:
                absence = absences_by_date[current_date]
                leave_type = self.leave_types_by_id.get(absence.leave_type_id)
                if leave_type:
                    print(f"{weekday***REMOVED*** {date_str***REMOVED***: {leave_type.shortname:<3***REMOVED*** "
                          f"{leave_type.name***REMOVED***")
                else:
                    print(f"{weekday***REMOVED*** {date_str***REMOVED***: Abwesenheit")
            else:
                print(f"{weekday***REMOVED*** {date_str***REMOVED***: --- frei ---")
            
            current_date += timedelta(days=1)
    
    def _print_statistics(self, shifts: List[EmployeeShift], absences: List[Absence]):
        """Druckt Statistiken."""
        print(f"\n{'='*60***REMOVED***")
        print("STATISTIKEN:")
        print("-" * 60)
        
        # Schicht-Statistiken
        shift_counter = Counter(s.shift_id for s in shifts)
        print("\nSchichtverteilung:")
        for shift_id, count in shift_counter.most_common():
            shift_info = self.shifts_by_id.get(shift_id)
            if shift_info:
                print(f"  {shift_info.name:<20***REMOVED*** {count:>3***REMOVED***x")
        
        # Abwesenheits-Statistiken
        if absences:
            absence_counter = Counter(a.leave_type_id for a in absences)
            print("\nAbwesenheiten:")
            for leave_type_id, count in absence_counter.most_common():
                leave_type = self.leave_types_by_id.get(leave_type_id)
                if leave_type:
                    print(f"  {leave_type.name:<20***REMOVED*** {count:>3***REMOVED*** Tage")
        
        # Arbeitsstunden
        total_hours = 0
        for shift in shifts:
            shift_info = self.shifts_by_id.get(shift.shift_id)
            if shift_info:
                weekday = shift.date.weekday()
                hours = shift_info.get_weekday_duration(weekday)
                total_hours += hours
        
        print(f"\nGesamtarbeitsstunden: {total_hours:.1f***REMOVED***h")
    
    def _calculate_surcharges(self, shifts: List[EmployeeShift]):
        """Berechnet Zuschläge basierend auf 5XCHAR."""
        if '5XCHAR' not in self.data:
            return
        
        print(f"\n{'='*60***REMOVED***")
        print("ZUSCHLÄGE:")
        print("-" * 60)
        
        xchar_rules = self.data['5XCHAR']
        surcharges = defaultdict(float)
        
        for shift in shifts:
            shift_info = self.shifts_by_id.get(shift.shift_id)
            if not shift_info:
                continue
            
            weekday = shift.date.weekday()
            hours = shift_info.get_weekday_duration(weekday)
            
            # Prüfe Zuschlagsregeln
            for rule in xchar_rules:
                # Sonntagszuschlag
                if rule.name == "Sonntagstunden" and weekday == 6:
                    surcharges["Sonntag"] += hours
                
                # Samstagszuschlag (ab 13:00 Uhr laut Regel)
                elif rule.name == "Samstagsstunden" and weekday == 5:
                    # Vereinfachte Annahme: Spätschicht = Zuschlag
                    if shift_info.shortname == "S":
                        surcharges["Samstag"] += hours
                
                # Nachtzuschlag
                elif rule.name == "Nachtstunden":
                    if shift_info.shortname == "N":
                        surcharges["Nacht"] += hours
        
        if surcharges:
            for type_name, hours in surcharges.items():
                print(f"  {type_name:<20***REMOVED*** {hours:>6.1f***REMOVED***h")
        else:
            print("  Keine Zuschläge in diesem Zeitraum")
    
    def generate_team_overview(self, group_id: int = None):
        """Generiert eine Team-Übersicht."""
        print(f"\n{'='*60***REMOVED***")
        print("TEAM-ÜBERSICHT")
        print(f"{'='*60***REMOVED***\n")
        
        # Zeige alle aktiven Mitarbeiter
        active_employees = [
            emp for emp in self.data.get('5EMPL', [])
            if not emp.empend  # Kein Austrittsdatum = aktiv
        ]
        
        print(f"Aktive Mitarbeiter: {len(active_employees)***REMOVED***")
        print("-" * 60)
        
        # Sortiere nach Name
        active_employees.sort(key=lambda e: (e.name, e.firstname))
        
        for emp in active_employees:
            # Geschlecht-Icon
            gender = "♀" if emp.sex == 1 else "♂"
            
            # Funktion oder Position
            role = emp.function or f"Position {emp.position***REMOVED***"
            
            print(f"{gender***REMOVED*** {emp.name:<20***REMOVED*** {emp.firstname:<15***REMOVED*** "
                  f"Nr.{emp.number:<10***REMOVED*** {role***REMOVED***")
            
            # Kontaktdaten wenn vorhanden
            if emp.email:
                print(f"   📧 {emp.email***REMOVED***")
            if emp.phone:
                print(f"   📞 {emp.phone***REMOVED***")


# Hauptprogramm
if __name__ == "__main__":
    import sys
    
    # Pfad zu DBF-Dateien
    if len(sys.argv) > 1:
        dbf_dir = Path(sys.argv[1])
    else:
        dbf_dir = Path("./dbf_files")
    
    if not dbf_dir.exists():
        print(f"Fehler: Verzeichnis {dbf_dir***REMOVED*** nicht gefunden!")
        sys.exit(1)
    
    # Erstelle Analyzer
    analyzer = SchichtplanAnalyzer(dbf_dir)
    
    # Beispiel 1: Analysiere Schichtplan für Mitarbeiter 47 (Karsten Bartel)
    # für den aktuellen Monat
    today = date.today()
    start_of_month = today.replace(day=1)
    end_of_month = (start_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    analyzer.analyze_employee_schedule(47, start_of_month, end_of_month)
    
    # Beispiel 2: Team-Übersicht
    analyzer.generate_team_overview()
