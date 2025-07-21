# practical_examples.py
"""
Praktische Beispiele für die Arbeit mit echten Schichtplaner5-Daten.
Zeigt typische Use Cases und Lösungen für häufige Aufgaben.
"""

from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, Counter
from pathlib import Path

# Beispiel 1: Schichtplan-Analyse
class SchichtplanAnalyzer:
    """Analysiert Schichtpläne und erkennt Muster."""
    
    def __init__(self, engine):
        self.engine = engine
    
    def analyze_shift_patterns(self, employee_id: int, 
                             start_date: date, 
                             end_date: date) -> Dict[str, Any]:
        """
        Analysiert Schichtmuster eines Mitarbeiters.
        
        Beispiel mit echten Daten:
        - Frühschicht (F): 06:00-14:00
        - Spätschicht (S): 14:00-22:00
        - Nachtschicht (N): 22:00-06:00
        """
        # Hole alle Schichten im Zeitraum
        shifts = (self.engine.query()
                 .select("5SPSHI")
                 .where("employee_id", "=", employee_id)
                 .where_date_range("date", start_date, end_date)
                 .join("5SHIFT")
                 .execute())
        
        # Analysiere Muster
        shift_sequence = []
        shift_counts = Counter()
        weekday_distribution = defaultdict(Counter)
        
        for shift_record in shifts.records:
            if isinstance(shift_record, dict):
                shift_data = shift_record["_relations"].get("5SHIFT", [{***REMOVED***])[0]
                shift_name = shift_data.shortname if hasattr(shift_data, 'shortname') else "?"
                shift_date = shift_record["_entity"].date
            else:
                shift_name = "?"
                shift_date = shift_record.date
            
            shift_sequence.append((shift_date, shift_name))
            shift_counts[shift_name] += 1
            
            if shift_date:
                weekday = shift_date.strftime("%A")
                weekday_distribution[weekday][shift_name] += 1
        
        # Erkenne Rotationsmuster (z.B. F-F-S-S-N-N-frei-frei)
        patterns = self._detect_rotation_patterns(shift_sequence)
        
        # Berechne Statistiken
        total_shifts = len(shifts.records)
        total_days = (end_date - start_date).days + 1
        
        return {
            "employee_id": employee_id,
            "period": {
                "start": str(start_date),
                "end": str(end_date),
                "total_days": total_days
            ***REMOVED***,
            "statistics": {
                "total_shifts": total_shifts,
                "work_percentage": (total_shifts / total_days * 100) if total_days > 0 else 0,
                "shift_distribution": dict(shift_counts),
                "most_common_shift": shift_counts.most_common(1)[0] if shift_counts else None
            ***REMOVED***,
            "patterns": {
                "detected_rotations": patterns,
                "weekday_preferences": dict(weekday_distribution)
            ***REMOVED***,
            "health_indicators": self._calculate_health_indicators(shift_sequence)
        ***REMOVED***
    
    def _detect_rotation_patterns(self, shift_sequence: List[Tuple[date, str]]) -> List[str]:
        """Erkennt typische Rotationsmuster."""
        patterns = []
        
        # Sortiere nach Datum
        shift_sequence.sort(key=lambda x: x[0])
        
        # Suche nach typischen Mustern
        sequence_str = "".join([s[1] for s in shift_sequence])
        
        # 3-Schicht-System Muster
        if "FFSSNN" in sequence_str:
            patterns.append("3-Schicht-Rotation (2-2-2)")
        
        # Wechselschicht
        if "FSFSFS" in sequence_str or "SFSFSF" in sequence_str:
            patterns.append("Wechselschicht (Früh/Spät)")
        
        # Nachtschicht-Block
        if "NNNNN" in sequence_str:
            patterns.append("Nachtschicht-Block")
        
        return patterns
    
    def _calculate_health_indicators(self, shift_sequence: List[Tuple[date, str]]) -> Dict[str, Any]:
        """Berechnet Gesundheitsindikatoren basierend auf Schichtfolgen."""
        indicators = {
            "consecutive_nights": 0,
            "quick_returns": 0,  # Wechsel mit < 11h Pause
            "weekend_work": 0,
            "shift_changes": 0
        ***REMOVED***
        
        # Sortiere nach Datum
        shift_sequence.sort(key=lambda x: x[0])
        
        consecutive_nights = 0
        last_shift = None
        last_date = None
        
        for shift_date, shift_type in shift_sequence:
            # Zähle Wochenendarbeit
            if shift_date.weekday() >= 5:  # Samstag oder Sonntag
                indicators["weekend_work"] += 1
            
            # Zähle aufeinanderfolgende Nachtschichten
            if shift_type == "N":
                consecutive_nights += 1
                indicators["consecutive_nights"] = max(
                    indicators["consecutive_nights"], 
                    consecutive_nights
                )
            else:
                consecutive_nights = 0
            
            # Prüfe auf schnelle Wechsel (z.B. Spät -> Früh)
            if last_shift and last_date:
                days_diff = (shift_date - last_date).days
                if days_diff == 1:  # Aufeinanderfolgende Tage
                    if last_shift == "S" and shift_type == "F":
                        indicators["quick_returns"] += 1
                    if last_shift != shift_type:
                        indicators["shift_changes"] += 1
            
            last_shift = shift_type
            last_date = shift_date
        
        return indicators


# Beispiel 2: Urlaubsplanung
class UrlaubsplanungHelper:
    """Hilft bei der Urlaubsplanung und -verwaltung."""
    
    def __init__(self, engine):
        self.engine = engine
    
    def check_urlaubsanspruch(self, employee_id: int, year: int) -> Dict[str, Any]:
        """
        Prüft Urlaubsanspruch und Verbrauch.
        
        Beispiel mit echten Urlaubstypen:
        - ID 1: Urlaub (30 Tage Standard)
        - ID 14: Sonderurlaub (2 Tage)
        """
        # Hole Urlaubsansprüche
        entitlements = (self.engine.query()
                       .select("5LEAEN")
                       .where("employee_id", "=", employee_id)
                       .where("year", "=", year)
                       .join("5LEAVT")
                       .execute())
        
        # Hole genommene Urlaubstage
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        
        absences = (self.engine.query()
                   .select("5ABSEN")
                   .where("employee_id", "=", employee_id)
                   .where_date_range("date", start_date, end_date)
                   .join("5LEAVT")
                   .execute())
        
        # Berechne Verbrauch pro Urlaubstyp
        anspruch_dict = {***REMOVED***
        verbrauch_dict = defaultdict(float)
        
        # Verarbeite Ansprüche
        for ent_record in entitlements.records:
            if isinstance(ent_record, dict):
                ent = ent_record["_entity"]
                leave_type = ent_record["_relations"].get("5LEAVT", [{***REMOVED***])[0]
                
                anspruch_dict[leave_type.name] = {
                    "id": ent.leave_type_id,
                    "anspruch": ent.entitlement,
                    "rest_vorjahr": ent.rest,
                    "in_tagen": ent.in_days,
                    "gesamt": ent.entitlement + ent.rest
                ***REMOVED***
        
        # Verarbeite Abwesenheiten
        for abs_record in absences.records:
            if isinstance(abs_record, dict):
                absence = abs_record["_entity"]
                leave_type = abs_record["_relations"].get("5LEAVT", [{***REMOVED***])[0]
                
                # Berechne Dauer
                if absence.interval:
                    # Teilweise Abwesenheit
                    duration = 0.5  # Vereinfacht
                else:
                    duration = 1.0
                
                verbrauch_dict[leave_type.name] += duration
        
        # Erstelle Zusammenfassung
        result = {
            "employee_id": employee_id,
            "year": year,
            "urlaubskonten": []
        ***REMOVED***
        
        for typ_name, anspruch in anspruch_dict.items():
            verbraucht = verbrauch_dict.get(typ_name, 0)
            rest = anspruch["gesamt"] - verbraucht
            
            result["urlaubskonten"].append({
                "typ": typ_name,
                "anspruch": anspruch["anspruch"],
                "uebertrag": anspruch["rest_vorjahr"],
                "gesamt": anspruch["gesamt"],
                "verbraucht": verbraucht,
                "rest": rest,
                "prozent_verbraucht": (verbraucht / anspruch["gesamt"] * 100) if anspruch["gesamt"] > 0 else 0
            ***REMOVED***)
        
        return result
    
    def find_urlaubsmoeglichkeiten(self, group_id: int, 
                                  start_date: date,
                                  duration_days: int,
                                  min_besetzung: int = 2) -> List[Dict[str, Any]]:
        """
        Findet mögliche Urlaubszeiträume für eine Gruppe.
        """
        # Hole alle Mitarbeiter der Gruppe
        members = self.engine.get_group_members(group_id)
        member_ids = [m['id'] for m in members]
        
        # Prüfe jeden Tag im Suchzeitraum
        possible_periods = []
        current_date = start_date
        
        while current_date < start_date + timedelta(days=90):  # Suche 3 Monate
            # Prüfe ob Urlaubszeitraum möglich
            period_possible = True
            daily_coverage = []
            
            for day_offset in range(duration_days):
                check_date = current_date + timedelta(days=day_offset)
                
                # Zähle verfügbare Mitarbeiter
                available = self._count_available_employees(member_ids, check_date)
                daily_coverage.append({
                    "date": check_date,
                    "available": available,
                    "sufficient": available >= min_besetzung
                ***REMOVED***)
                
                if available < min_besetzung:
                    period_possible = False
            
            if period_possible:
                possible_periods.append({
                    "start": current_date,
                    "end": current_date + timedelta(days=duration_days-1),
                    "duration": duration_days,
                    "coverage": daily_coverage
                ***REMOVED***)
            
            current_date += timedelta(days=1)
        
        return possible_periods[:10]  # Erste 10 Möglichkeiten
    
    def _count_available_employees(self, employee_ids: List[int], 
                                 check_date: date) -> int:
        """Zählt verfügbare Mitarbeiter an einem Tag."""
        # Hole Abwesenheiten
        absences = (self.engine.query()
                   .select("5ABSEN")
                   .where("employee_id", "in", employee_ids)
                   .where("date", "=", check_date)
                   .execute())
        
        absent_employees = {a.employee_id for a in absences.records***REMOVED***
        available = len(employee_ids) - len(absent_employees)
        
        return available


# Beispiel 3: Überstunden-Verwaltung
class UeberstundenManager:
    """Verwaltet und analysiert Überstunden."""
    
    def __init__(self, engine):
        self.engine = engine
    
    def calculate_monthly_overtime(self, employee_id: int, 
                                 month: int, 
                                 year: int) -> Dict[str, Any]:
        """
        Berechnet Überstunden für einen Monat.
        
        Nutzt:
        - 5BOOK für Überstunden-Buchungen
        - 5OVER für Überstunden-Einträge
        - 5XCHAR für Zuschläge (Sonntag, Nacht, etc.)
        """
        from calendar import monthrange
        
        # Monatsbereich
        _, last_day = monthrange(year, month)
        start_date = date(year, month, 1)
        end_date = date(year, month, last_day)
        
        # Hole Mitarbeiter-Stammdaten
        emp = (self.engine.query()
              .select("5EMPL")
              .where("id", "=", employee_id)
              .execute())
        
        if not emp.records:
            return {"error": "Mitarbeiter nicht gefunden"***REMOVED***
        
        employee = emp.records[0]
        soll_stunden = employee.hrsmonth  # Monatssoll
        
        # Hole alle Schichten im Monat
        shifts = (self.engine.query()
                 .select("5SPSHI")
                 .where("employee_id", "=", employee_id)
                 .where_date_range("date", start_date, end_date)
                 .join("5SHIFT")
                 .execute())
        
        # Berechne Ist-Stunden
        ist_stunden = 0.0
        zuschlaege = defaultdict(float)
        
        for shift_record in shifts.records:
            if isinstance(shift_record, dict):
                shift = shift_record["_entity"]
                shift_info = shift_record["_relations"].get("5SHIFT", [{***REMOVED***])[0]
                
                # Basis-Stunden
                duration = shift_info.duration[shift.date.weekday()] if hasattr(shift_info, 'duration') else 8.0
                ist_stunden += duration
                
                # Prüfe Zuschläge
                weekday = shift.date.weekday()
                
                # Sonntagszuschlag
                if weekday == 6:
                    zuschlaege["Sonntag"] += duration * 0.5  # 50% Zuschlag
                
                # Samstagsszuschlag (ab 12:00)
                elif weekday == 5:
                    if hasattr(shift_info, 'startend'):
                        start_time = shift_info.startend[weekday]
                        if "14:00" in start_time or "22:00" in start_time:
                            zuschlaege["Samstag"] += duration * 0.25  # 25% Zuschlag
                
                # Nachtzuschlag (22:00-06:00)
                if hasattr(shift_info, 'shortname') and shift_info.shortname == "N":
                    zuschlaege["Nacht"] += duration * 0.25  # 25% Zuschlag
        
        # Hole Überstunden-Buchungen
        bookings = (self.engine.query()
                   .select("5BOOK")
                   .where("employee_id", "=", employee_id)
                   .where_date_range("date", start_date, end_date)
                   .execute())
        
        # Verarbeite Buchungen
        buchungen = []
        gesamt_buchung = 0.0
        
        for booking in bookings.records:
            buchungen.append({
                "datum": str(booking.date),
                "typ": self._get_booking_type_name(booking.type),
                "stunden": float(booking.value),
                "notiz": booking.note
            ***REMOVED***)
            gesamt_buchung += float(booking.value)
        
        # Berechne Überstunden
        ueberstunden = ist_stunden - soll_stunden + gesamt_buchung
        
        return {
            "employee_id": employee_id,
            "employee_name": f"{employee.name***REMOVED*** {employee.firstname***REMOVED***",
            "monat": f"{month***REMOVED***/{year***REMOVED***",
            "arbeitszeit": {
                "soll_stunden": soll_stunden,
                "ist_stunden": ist_stunden,
                "differenz": ist_stunden - soll_stunden
            ***REMOVED***,
            "zuschlaege": dict(zuschlaege),
            "zuschlaege_gesamt": sum(zuschlaege.values()),
            "buchungen": buchungen,
            "buchungen_gesamt": gesamt_buchung,
            "ueberstunden_gesamt": ueberstunden,
            "ueberstunden_mit_zuschlag": ueberstunden + sum(zuschlaege.values())
        ***REMOVED***
    
    def _get_booking_type_name(self, type_id: int) -> str:
        """Übersetzt Buchungstyp-ID in Namen."""
        booking_types = {
            1: "Überstunden",
            2: "Minusstunden",
            3: "Urlaubskorrektur",
            4: "Krankheitskorrektur",
            5: "Sonstiges"
        ***REMOVED***
        return booking_types.get(type_id, f"Typ {type_id***REMOVED***")


# Beispiel 4: Praktische Verwendung
def example_usage(dbf_dir: Path):
    """Zeigt die praktische Verwendung der Analyzer."""
    from libopenschichtplaner5.query_engine import QueryEngine
    
    # Initialisiere Engine
    engine = QueryEngine(dbf_dir)
    
    # 1. Schichtmuster analysieren
    print("=== Schichtmuster-Analyse ===")
    analyzer = SchichtplanAnalyzer(engine)
    
    # Analysiere Mitarbeiter 47 (Karsten Bartel - Schichtleiter)
    analysis = analyzer.analyze_shift_patterns(
        employee_id=47,
        start_date=date.today() - timedelta(days=30),
        end_date=date.today()
    )
    
    print(f"Schichtverteilung: {analysis['statistics']['shift_distribution']***REMOVED***")
    print(f"Erkannte Muster: {analysis['patterns']['detected_rotations']***REMOVED***")
    print(f"Gesundheitsindikatoren: {analysis['health_indicators']***REMOVED***")
    
    # 2. Urlaubsplanung
    print("\n=== Urlaubsplanung ===")
    urlaub_helper = UrlaubsplanungHelper(engine)
    
    # Prüfe Urlaubsanspruch
    urlaub_info = urlaub_helper.check_urlaubsanspruch(
        employee_id=47,
        year=2024
    )
    
    for konto in urlaub_info['urlaubskonten']:
        print(f"{konto['typ']***REMOVED***: {konto['rest']:.1f***REMOVED*** Tage verfügbar "
              f"(von {konto['gesamt']:.1f***REMOVED***)")
    
    # 3. Überstunden
    print("\n=== Überstunden-Berechnung ===")
    ueberstunden_mgr = UeberstundenManager(engine)
    
    overtime = ueberstunden_mgr.calculate_monthly_overtime(
        employee_id=47,
        month=datetime.now().month,
        year=datetime.now().year
    )
    
    print(f"Soll: {overtime['arbeitszeit']['soll_stunden']:.1f***REMOVED***h")
    print(f"Ist: {overtime['arbeitszeit']['ist_stunden']:.1f***REMOVED***h")
    print(f"Überstunden: {overtime['ueberstunden_gesamt']:.1f***REMOVED***h")
    print(f"Zuschläge: {overtime['zuschlaege']***REMOVED***")


# Beispiel 5: Performance-Optimierungen
class OptimizedQueryEngine:
    """Optimierte Query-Engine mit Caching und Indizierung."""
    
    def __init__(self, engine):
        self.engine = engine
        self._cache = {***REMOVED***
        self._indices = defaultdict(dict)
        self._build_indices()
    
    def _build_indices(self):
        """Baut Indizes für häufige Lookups."""
        # Employee-Index
        for emp in self.engine.loaded_tables.get("5EMPL", []):
            self._indices["employee_by_id"][emp.id] = emp
            self._indices["employee_by_number"][emp.number] = emp
            self._indices["employee_by_name"][f"{emp.name***REMOVED*** {emp.firstname***REMOVED***".lower()] = emp
        
        # Shift-Index
        for shift in self.engine.loaded_tables.get("5SHIFT", []):
            self._indices["shift_by_id"][shift.id] = shift
            self._indices["shift_by_shortname"][shift.shortname] = shift
        
        print(f"Indizes erstellt: {len(self._indices)***REMOVED*** Typen")
    
    def get_employee_by_number(self, number: str) -> Optional[Any]:
        """Schneller Lookup nach Personalnummer."""
        return self._indices["employee_by_number"].get(number)
    
    def search_employees_fast(self, search_term: str) -> List[Any]:
        """Schnelle Mitarbeitersuche."""
        search_lower = search_term.lower()
        results = []
        
        for name, emp in self._indices["employee_by_name"].items():
            if search_lower in name:
                results.append(emp)
        
        return results
