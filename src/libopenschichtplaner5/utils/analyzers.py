#!/usr/bin/env python3
"""
Umfassender DBF-Analyzer für Schichtplaner5
Analysiert alle DBF-Dateien und prüft Datenqualität, Beziehungen und Encoding.
"""

from pathlib import Path
from typing import Dict, List, Any, Set, Tuple
from collections import defaultdict, Counter
from datetime import datetime, date
import json


class SchichtplanerAnalyzer:
    """Analysiert Schichtplaner5 DBF-Dateien umfassend."""
    
    def __init__(self, dbf_dir: Path):
        self.dbf_dir = Path(dbf_dir)
        self.tables = {***REMOVED***
        self.statistics = defaultdict(dict)
        self.issues = defaultdict(list)
        self.relationships = defaultdict(list)
        
    def analyze_all(self):
        """Führt eine vollständige Analyse aller DBF-Dateien durch."""
        print(f"\n{'='*80***REMOVED***")
        print("SCHICHTPLANER5 DBF ANALYSE")
        print(f"{'='*80***REMOVED***\n")
        
        # 1. Lade alle Tabellen
        self._load_all_tables()
        
        # 2. Analysiere Datenqualität
        self._analyze_data_quality()
        
        # 3. Prüfe Beziehungen
        self._check_relationships()
        
        # 4. Encoding-Analyse
        self._analyze_encoding_issues()
        
        # 5. Erstelle Report
        self._generate_report()
    
    def _load_all_tables(self):
        """Lädt alle DBF-Dateien."""
        print("1. LADE TABELLEN")
        print("-" * 40)
        
        dbf_files = list(self.dbf_dir.glob("*.txt"))  # Da als .txt umbenannt
        
        for dbf_file in sorted(dbf_files):
            table_name = dbf_file.stem
            print(f"  Lade {table_name***REMOVED***...", end="")
            
            try:
                from improved_dbf_reader import SchichtplanerDBFReader
                reader = SchichtplanerDBFReader(dbf_file)
                self.tables[table_name] = list(reader.records())
                self.statistics[table_name]['record_count'] = len(self.tables[table_name])
                print(f" OK ({len(self.tables[table_name])***REMOVED*** Records)")
            except Exception as e:
                print(f" FEHLER: {e***REMOVED***")
                self.issues[table_name].append(f"Ladefehler: {e***REMOVED***")
    
    def _analyze_data_quality(self):
        """Analysiert die Datenqualität."""
        print("\n2. DATENQUALITÄTS-ANALYSE")
        print("-" * 40)
        
        # Mitarbeiter-Analyse
        if '5EMPL' in self.tables:
            self._analyze_employees()
        
        # Schicht-Analyse
        if '5SHIFT' in self.tables:
            self._analyze_shifts()
        
        # Abwesenheits-Analyse
        if '5ABSEN' in self.tables and '5LEAVT' in self.tables:
            self._analyze_absences()
    
    def _analyze_employees(self):
        """Analysiert Mitarbeiterdaten."""
        employees = self.tables['5EMPL']
        print(f"\n  Mitarbeiter-Analyse:")
        
        # Geschlechterverteilung
        sex_dist = Counter(emp.get('SEX', 0) for emp in employees)
        print(f"    - Geschlechterverteilung: {dict(sex_dist)***REMOVED***")
        
        # Aktive vs. Inaktive
        active = sum(1 for emp in employees if not emp.get('EMPEND'))
        print(f"    - Aktive Mitarbeiter: {active***REMOVED***/{len(employees)***REMOVED***")
        
        # Fehlende wichtige Daten
        missing_email = sum(1 for emp in employees if not emp.get('EMAIL'))
        missing_phone = sum(1 for emp in employees if not emp.get('PHONE'))
        print(f"    - Ohne E-Mail: {missing_email***REMOVED***")
        print(f"    - Ohne Telefon: {missing_phone***REMOVED***")
        
        # Doppelte Personalnummern
        numbers = [emp.get('NUMBER') for emp in employees if emp.get('NUMBER')]
        duplicates = [num for num, count in Counter(numbers).items() if count > 1]
        if duplicates:
            self.issues['5EMPL'].append(f"Doppelte Personalnummern: {duplicates***REMOVED***")
    
    def _analyze_shifts(self):
        """Analysiert Schichtdefinitionen."""
        shifts = self.tables['5SHIFT']
        print(f"\n  Schicht-Analyse:")
        
        print(f"    - Anzahl Schichttypen: {len(shifts)***REMOVED***")
        
        # Zeige alle Schichten
        for shift in shifts:
            print(f"    - {shift.get('NAME')***REMOVED*** ({shift.get('SHORTNAME')***REMOVED***)")
            
            # Prüfe Konsistenz der Arbeitszeiten
            durations = [shift.get(f'DURATION{i***REMOVED***', 0) for i in range(7)]
            if any(d != durations[0] for d in durations if d > 0):
                self.issues['5SHIFT'].append(
                    f"Inkonsistente Dauer bei {shift.get('NAME')***REMOVED***: {durations***REMOVED***"
                )
    
    def _analyze_absences(self):
        """Analysiert Abwesenheiten."""
        absences = self.tables.get('5ABSEN', [])
        leave_types = self.tables.get('5LEAVT', [])
        
        print(f"\n  Abwesenheits-Analyse:")
        print(f"    - Urlaubstypen: {len(leave_types)***REMOVED***")
        
        for lt in leave_types:
            print(f"      • {lt.get('NAME')***REMOVED*** ({lt.get('SHORTNAME')***REMOVED***)")
    
    def _check_relationships(self):
        """Überprüft Beziehungen zwischen Tabellen."""
        print("\n3. BEZIEHUNGS-PRÜFUNG")
        print("-" * 40)
        
        # Definiere erwartete Beziehungen
        relationships = [
            ('5ABSEN', 'EMPLOYEEID', '5EMPL', 'ID'),
            ('5ABSEN', 'LEAVETYPID', '5LEAVT', 'ID'),
            ('5MASHI', 'EMPLOYEEID', '5EMPL', 'ID'),
            ('5MASHI', 'SHIFTID', '5SHIFT', 'ID'),
            ('5SPSHI', 'EMPLOYEEID', '5EMPL', 'ID'),
            ('5SPSHI', 'SHIFTID', '5SHIFT', 'ID'),
            ('5NOTE', 'EMPLOYEEID', '5EMPL', 'ID'),
            ('5BOOK', 'EMPLOYEEID', '5EMPL', 'ID'),
            ('5OVER', 'EMPLOYEEID', '5EMPL', 'ID'),
        ]
        
        for source_table, source_field, target_table, target_field in relationships:
            if source_table in self.tables and target_table in self.tables:
                self._check_foreign_keys(
                    source_table, source_field, 
                    target_table, target_field
                )
    
    def _check_foreign_keys(self, source_table: str, source_field: str, 
                           target_table: str, target_field: str):
        """Prüft Foreign Key Beziehungen."""
        source_records = self.tables[source_table]
        target_records = self.tables[target_table]
        
        # Sammle alle Target-IDs
        target_ids = set(rec.get(target_field) for rec in target_records)
        
        # Prüfe Source-Referenzen
        orphaned = []
        for rec in source_records:
            ref_id = rec.get(source_field)
            if ref_id and ref_id not in target_ids:
                orphaned.append(ref_id)
        
        if orphaned:
            unique_orphaned = list(set(orphaned))
            self.issues[source_table].append(
                f"Verwaiste Referenzen in {source_field***REMOVED*** -> {target_table***REMOVED***.{target_field***REMOVED***: "
                f"{len(unique_orphaned)***REMOVED*** IDs"
            )
    
    def _analyze_encoding_issues(self):
        """Analysiert Encoding-Probleme."""
        print("\n4. ENCODING-ANALYSE")
        print("-" * 40)
        
        # Sammle alle Texte mit problematischen Zeichen
        problematic_chars = {
            "ь": "ü",
            "д": "ä",
            "ц": "ö",
            "Ь": "Ü",
            "Д": "Ä",
            "Ц": "Ö",
            "Я": "ß"
        ***REMOVED***
        
        found_issues = defaultdict(list)
        
        for table_name, records in self.tables.items():
            for record in records:
                for field, value in record.items():
                    if isinstance(value, str):
                        for bad_char, good_char in problematic_chars.items():
                            if bad_char in value:
                                found_issues[table_name].append({
                                    'field': field,
                                    'value': value,
                                    'issue': f"{bad_char***REMOVED*** -> {good_char***REMOVED***"
                                ***REMOVED***)
        
        if found_issues:
            print("  Gefundene Encoding-Probleme:")
            for table, issues in found_issues.items():
                print(f"    {table***REMOVED***: {len(issues)***REMOVED*** Vorkommen")
    
    def _generate_report(self):
        """Generiert einen Abschlussbericht."""
        print(f"\n{'='*80***REMOVED***")
        print("ZUSAMMENFASSUNG")
        print(f"{'='*80***REMOVED***\n")
        
        # Tabellen-Übersicht
        print("TABELLEN-ÜBERSICHT:")
        total_records = 0
        for table_name in sorted(self.tables.keys()):
            count = self.statistics[table_name]['record_count']
            total_records += count
            print(f"  {table_name:<15***REMOVED*** {count:>6***REMOVED*** Records")
        print(f"  {'GESAMT':<15***REMOVED*** {total_records:>6***REMOVED*** Records")
        
        # Gefundene Probleme
        if any(self.issues.values()):
            print("\n\nGEFUNDENE PROBLEME:")
            for table, issues in self.issues.items():
                if issues:
                    print(f"\n  {table***REMOVED***:")
                    for issue in issues:
                        print(f"    - {issue***REMOVED***")
        else:
            print("\n✅ Keine kritischen Probleme gefunden!")
        
        # Empfehlungen
        print("\n\nEMPFEHLUNGEN:")
        print("  1. Encoding auf CP850 oder CP1252 standardisieren")
        print("  2. Fehlende E-Mail-Adressen ergänzen")
        print("  3. MASHI als 'Mitarbeiterschichten' interpretieren")
        print("  4. Zuschlagsregeln (5XCHAR) für Wochenend-/Nachtzuschläge nutzen")
        
        # Speichere detaillierten Report
        self._save_detailed_report()
    
    def _save_detailed_report(self):
        """Speichert einen detaillierten JSON-Report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'statistics': dict(self.statistics),
            'issues': dict(self.issues),
            'table_summary': {
                table: {
                    'record_count': len(records),
                    'fields': list(records[0].keys()) if records else []
                ***REMOVED***
                for table, records in self.tables.items()
            ***REMOVED***
        ***REMOVED***
        
        report_path = self.dbf_dir / 'analysis_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📄 Detaillierter Report gespeichert: {report_path***REMOVED***")


# Beispiel-Verwendung
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        dbf_dir = Path(sys.argv[1])
    else:
        # Standard-Verzeichnis
        dbf_dir = Path("./dbf_files")
    
    if not dbf_dir.exists():
        print(f"Fehler: Verzeichnis {dbf_dir***REMOVED*** nicht gefunden!")
        sys.exit(1)
    
    analyzer = SchichtplanerAnalyzer(dbf_dir)
    analyzer.analyze_all()
