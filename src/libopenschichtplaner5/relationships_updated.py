# libopenschichtplaner5/src/libopenschichtplaner5/relationships_updated.py
"""
Aktualisierte Relationship-Definitionen basierend auf der Analyse der echten DBF-Dateien.
Erweitert um neue Tabellen und korrigierte Feldnamen.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional, Set
from enum import Enum


class RelationType(Enum):
    """Types of relationships between tables."""
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"
    MANY_TO_MANY = "N:N"


@dataclass
class TableRelationship:
    """Defines a relationship between two tables."""
    source_table: str
    source_field: str
    target_table: str
    target_field: str
    relationship_type: RelationType
    description: str = ""
    cascade_delete: bool = False  # NEU: Für Lösch-Kaskadierung
    
    def __hash__(self):
        return hash((self.source_table, self.source_field, self.target_table, self.target_field))


class UpdatedRelationshipManager:
    """
    Aktualisierter Relationship Manager mit allen Tabellen und korrekten Feldnamen.
    """
    
    def __init__(self):
        self.relationships: Set[TableRelationship] = set()
        self._index: Dict[str, List[TableRelationship]] = {***REMOVED***
        self._define_all_relationships()
        self._build_index()
    
    def _define_all_relationships(self):
        """Definiert alle Beziehungen basierend auf den echten DBF-Strukturen."""
        
        # ========== EMPLOYEE (5EMPL) Relationships ==========
        
        # Employee -> Notes (1:N)
        self.add_relationship("5EMPL", "ID", "5NOTE", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY, 
                            "Mitarbeiter hat viele Notizen")
        
        # Employee -> Absences (1:N)
        self.add_relationship("5EMPL", "ID", "5ABSEN", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat viele Abwesenheiten")
        
        # Employee -> Shift Details (1:N)
        self.add_relationship("5EMPL", "ID", "5SPSHI", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat viele Schichtdetails")
        
        # Employee -> Master Shifts (1:N)
        self.add_relationship("5EMPL", "ID", "5MASHI", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat viele Master-Schichten")
        
        # Employee -> Cycle Assignments (1:N)
        self.add_relationship("5EMPL", "ID", "5CYASS", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat Zyklus-Zuweisungen")
        
        # Employee -> Group Assignments (1:N)
        self.add_relationship("5EMPL", "ID", "5GRASG", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter gehört zu Gruppen")
        
        # Employee -> Leave Entitlements (1:N)
        self.add_relationship("5EMPL", "ID", "5LEAEN", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat Urlaubsansprüche")
        
        # Employee -> Bookings (1:N)
        self.add_relationship("5EMPL", "ID", "5BOOK", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat Buchungen (Überstunden)")
        
        # Employee -> Restrictions (1:N)
        self.add_relationship("5EMPL", "ID", "5RESTR", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat Schicht-Einschränkungen")
        
        # Employee -> Employee Access (1:N)
        self.add_relationship("5EMPL", "ID", "5EMACC", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat Zugriffsrechte")
        
        # Employee -> Overtime (1:N)
        self.add_relationship("5EMPL", "ID", "5OVER", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat Überstunden-Einträge")
        
        # Employee -> Cycle Exceptions (1:N)
        self.add_relationship("5EMPL", "ID", "5CYEXC", "EMPLOYEEID", 
                            RelationType.ONE_TO_MANY,
                            "Mitarbeiter hat Zyklus-Ausnahmen")
        
        # ========== GROUP (5GROUP) Relationships ==========
        
        # Group -> Group Assignments (1:N)
        self.add_relationship("5GROUP", "ID", "5GRASG", "GROUPID", 
                            RelationType.ONE_TO_MANY,
                            "Gruppe hat viele Mitarbeiter-Zuweisungen")
        
        # Group -> Group Access (1:N)
        self.add_relationship("5GROUP", "ID", "5GRACC", "GROUPID", 
                            RelationType.ONE_TO_MANY,
                            "Gruppe hat Zugriffsdefinitionen")
        
        # Group -> Periods (1:N)
        self.add_relationship("5GROUP", "ID", "5PERIO", "GROUPID", 
                            RelationType.ONE_TO_MANY,
                            "Gruppe hat Planungsperioden")
        
        # Group -> Daily Demands (1:N)
        self.add_relationship("5GROUP", "ID", "5DADEM", "GROUPID", 
                            RelationType.ONE_TO_MANY,
                            "Gruppe hat Tagesbedarfe")
        
        # Group -> Shift Demands (1:N)
        self.add_relationship("5GROUP", "ID", "5SHDEM", "GROUPID", 
                            RelationType.ONE_TO_MANY,
                            "Gruppe hat Schichtbedarfe")
        
        # Group -> Shift Plan Demands (1:N)
        self.add_relationship("5GROUP", "ID", "5SPDEM", "GROUPID", 
                            RelationType.ONE_TO_MANY,
                            "Gruppe hat Schichtplan-Bedarfe")
        
        # Group -> Holiday Bans (1:N)
        self.add_relationship("5GROUP", "ID", "5HOBAN", "GROUPID", 
                            RelationType.ONE_TO_MANY,
                            "Gruppe hat Urlaubssperren")
        
        # ========== SHIFT (5SHIFT) Relationships ==========
        
        # Shift -> Shift Details (1:N)
        self.add_relationship("5SHIFT", "ID", "5SPSHI", "SHIFTID", 
                            RelationType.ONE_TO_MANY,
                            "Schicht erscheint in vielen Details")
        
        # Shift -> Master Shifts (1:N)
        self.add_relationship("5SHIFT", "ID", "5MASHI", "SHIFTID", 
                            RelationType.ONE_TO_MANY,
                            "Schicht in Master-Zuweisungen")
        
        # Shift -> Cycle Entitlements (1:N)
        self.add_relationship("5SHIFT", "ID", "5CYENT", "SHIFTID", 
                            RelationType.ONE_TO_MANY,
                            "Schicht in Zyklus-Berechtigungen")
        
        # Shift -> Restrictions (1:N)
        self.add_relationship("5SHIFT", "ID", "5RESTR", "SHIFTID", 
                            RelationType.ONE_TO_MANY,
                            "Schicht hat Einschränkungen")
        
        # Shift -> Shift Demands (1:N)
        self.add_relationship("5SHIFT", "ID", "5SHDEM", "SHIFTID", 
                            RelationType.ONE_TO_MANY,
                            "Schicht in Bedarfsplänen")
        
        # Shift -> Shift Plan Demands (1:N)
        self.add_relationship("5SHIFT", "ID", "5SPDEM", "SHIFTID", 
                            RelationType.ONE_TO_MANY,
                            "Schicht in Schichtplan-Bedarfen")
        
        # ========== LEAVE TYPE (5LEAVT) Relationships ==========
        
        # Leave Type -> Leave Entitlements (1:N)
        self.add_relationship("5LEAVT", "ID", "5LEAEN", "LEAVETYPID", 
                            RelationType.ONE_TO_MANY,
                            "Urlaubstyp in Ansprüchen")
        
        # Leave Type -> Absences (1:N)
        self.add_relationship("5LEAVT", "ID", "5ABSEN", "LEAVETYPID", 
                            RelationType.ONE_TO_MANY,
                            "Urlaubstyp in Abwesenheiten")
        
        # ========== CYCLE (5CYCLE) Relationships ==========
        
        # Cycle -> Cycle Assignments (1:N)
        self.add_relationship("5CYCLE", "ID", "5CYASS", "CYCLEID", 
                            RelationType.ONE_TO_MANY,
                            "Zyklus hat viele Zuweisungen")
        
        # Cycle -> Cycle Entitlements (1:N)
        self.add_relationship("5CYCLE", "ID", "5CYENT", "CYCLEID", 
                            RelationType.ONE_TO_MANY,
                            "Zyklus hat Berechtigungen")
        
        # ========== WORK LOCATION (5WOPL) Relationships ==========
        
        # Work Location -> Shift Details (1:N)
        self.add_relationship("5WOPL", "ID", "5SPSHI", "WORKPLACID", 
                            RelationType.ONE_TO_MANY,
                            "Arbeitsort in Schichtdetails")
        
        # Work Location -> Master Shifts (1:N)
        self.add_relationship("5WOPL", "ID", "5MASHI", "WORKPLACID", 
                            RelationType.ONE_TO_MANY,
                            "Arbeitsort in Master-Schichten")
        
        # Work Location -> Cycle Entitlements (1:N)
        self.add_relationship("5WOPL", "ID", "5CYENT", "WORKPLACID", 
                            RelationType.ONE_TO_MANY,
                            "Arbeitsort in Zyklus-Berechtigungen")
        
        # Work Location -> Shift Demands (1:N)
        self.add_relationship("5WOPL", "ID", "5SHDEM", "WORKPLACID", 
                            RelationType.ONE_TO_MANY,
                            "Arbeitsort in Schichtbedarfen")
        
        # Work Location -> Shift Plan Demands (1:N)
        self.add_relationship("5WOPL", "ID", "5SPDEM", "WORKPLACID", 
                            RelationType.ONE_TO_MANY,
                            "Arbeitsort in Schichtplan-Bedarfen")
        
        # ========== USER (5USER) Relationships ==========
        
        # User -> Employee Access (1:N)
        self.add_relationship("5USER", "ID", "5EMACC", "USERID", 
                            RelationType.ONE_TO_MANY,
                            "Benutzer hat Mitarbeiter-Zugriffsrechte")
        
        # User -> Group Access (1:N)
        self.add_relationship("5USER", "ID", "5GRACC", "USERID", 
                            RelationType.ONE_TO_MANY,
                            "Benutzer hat Gruppen-Zugriffsrechte")
        
        # User -> User Settings (1:1)
        self.add_relationship("5USER", "ID", "5USETT", "ID", 
                            RelationType.ONE_TO_ONE,
                            "Benutzer hat Einstellungen")
        
        # ========== HOLIDAY (5HOLID) Relationships ==========
        
        # Holiday -> Holiday Bans? (Unklar ohne Daten)
        # Möglicherweise keine direkte Beziehung
        
        # ========== Weitere spezielle Beziehungen ==========
        
        # Cycle Assignment -> Cycle Exceptions (1:N)
        self.add_relationship("5CYASS", "ID", "5CYEXC", "CYCLEASSID", 
                            RelationType.ONE_TO_MANY,
                            "Zyklus-Zuweisung hat Ausnahmen",
                            cascade_delete=True)
    
    def add_relationship(self, source_table: str, source_field: str, 
                        target_table: str, target_field: str,
                        relationship_type: RelationType, 
                        description: str = "",
                        cascade_delete: bool = False):
        """Fügt eine Beziehung hinzu."""
        rel = TableRelationship(
            source_table, source_field, target_table, 
            target_field, relationship_type, description,
            cascade_delete
        )
        self.relationships.add(rel)
    
    def _build_index(self):
        """Baut Index für schnelle Lookups."""
        self._index.clear()
        for rel in self.relationships:
            # Index by source table
            if rel.source_table not in self._index:
                self._index[rel.source_table] = []
            self._index[rel.source_table].append(rel)
            
            # Also index by target table for reverse lookups
            reverse_key = f"_reverse_{rel.target_table***REMOVED***"
            if reverse_key not in self._index:
                self._index[reverse_key] = []
            self._index[reverse_key].append(rel)
    
    def get_cascade_deletes(self, table: str) -> List[TableRelationship]:
        """Gibt alle Beziehungen zurück, die bei Löschung kaskadiert werden sollten."""
        cascade_rels = []
        for rel in self.get_relationships_to(table):
            if rel.cascade_delete:
                cascade_rels.append(rel)
        return cascade_rels
    
    def validate_referential_integrity(self, source_data: List[Any], 
                                     target_data: List[Any],
                                     relationship: TableRelationship) -> Dict[str, Any]:
        """
        Validiert die referentielle Integrität zwischen zwei Tabellen.
        
        Returns:
            Dict mit Validierungsergebnissen
        """
        # Sammle alle IDs aus der Zieltabelle
        target_ids = set()
        for record in target_data:
            target_id = getattr(record, relationship.target_field, None)
            if target_id is not None:
                target_ids.add(target_id)
        
        # Prüfe Referenzen in der Quelltabelle
        orphaned_records = []
        valid_references = 0
        null_references = 0
        
        for record in source_data:
            ref_value = getattr(record, relationship.source_field, None)
            
            if ref_value is None:
                null_references += 1
            elif ref_value not in target_ids:
                orphaned_records.append({
                    'record_id': getattr(record, 'id', 'unknown'),
                    'field': relationship.source_field,
                    'invalid_reference': ref_value
                ***REMOVED***)
            else:
                valid_references += 1
        
        return {
            'valid': len(orphaned_records) == 0,
            'total_records': len(source_data),
            'valid_references': valid_references,
            'null_references': null_references,
            'orphaned_records': orphaned_records,
            'orphaned_count': len(orphaned_records)
        ***REMOVED***
    
    def get_join_path(self, from_table: str, to_table: str, 
                     max_depth: int = 3) -> Optional[List[TableRelationship]]:
        """
        Findet den kürzesten Pfad zwischen zwei Tabellen.
        
        Args:
            from_table: Ausgangstabelle
            to_table: Zieltabelle
            max_depth: Maximale Anzahl von Joins
            
        Returns:
            Liste von Relationships die den Pfad bilden, oder None
        """
        if from_table == to_table:
            return []
        
        # Breadth-first search
        from collections import deque
        
        queue = deque([(from_table, [])])
        visited = {from_table***REMOVED***
        
        while queue and len(visited) < 50:  # Schutz vor Endlosschleife
            current_table, path = queue.popleft()
            
            if len(path) >= max_depth:
                continue
            
            # Prüfe alle ausgehenden Beziehungen
            for rel in self.get_relationships_from(current_table):
                if rel.target_table == to_table:
                    return path + [rel]
                
                if rel.target_table not in visited:
                    visited.add(rel.target_table)
                    queue.append((rel.target_table, path + [rel]))
            
            # Prüfe auch eingehende Beziehungen (reverse)
            for rel in self.get_relationships_to(current_table):
                if rel.source_table == to_table:
                    # Erstelle reverse relationship
                    reverse_rel = TableRelationship(
                        current_table, rel.target_field,
                        rel.source_table, rel.source_field,
                        self._reverse_relation_type(rel.relationship_type),
                        f"Reverse: {rel.description***REMOVED***"
                    )
                    return path + [reverse_rel]
                
                if rel.source_table not in visited:
                    visited.add(rel.source_table)
                    reverse_rel = TableRelationship(
                        current_table, rel.target_field,
                        rel.source_table, rel.source_field,
                        self._reverse_relation_type(rel.relationship_type),
                        f"Reverse: {rel.description***REMOVED***"
                    )
                    queue.append((rel.source_table, path + [reverse_rel]))
        
        return None
    
    def _reverse_relation_type(self, rel_type: RelationType) -> RelationType:
        """Kehrt einen Beziehungstyp um."""
        if rel_type == RelationType.ONE_TO_MANY:
            return RelationType.MANY_TO_ONE
        elif rel_type == RelationType.MANY_TO_ONE:
            return RelationType.ONE_TO_MANY
        else:
            return rel_type
    
    def get_relationships_from(self, table: str) -> List[TableRelationship]:
        """Get all relationships where the given table is the source."""
        return self._index.get(table, [])
    
    def get_relationships_to(self, table: str) -> List[TableRelationship]:
        """Get all relationships where the given table is the target."""
        return self._index.get(f"_reverse_{table***REMOVED***", [])
    
    def get_all_related_tables(self, table: str) -> Set[str]:
        """Get all tables that have any relationship with the given table."""
        related = set()
        
        for rel in self.get_relationships_from(table):
            related.add(rel.target_table)
        
        for rel in self.get_relationships_to(table):
            related.add(rel.source_table)
        
        return related


# Globale Instanz
updated_relationship_manager = UpdatedRelationshipManager()


# Hilfsfunktionen für häufige Queries
def get_employee_complete_data(engine, employee_id: int) -> Dict[str, Any]:
    """
    Holt alle Daten eines Mitarbeiters mit allen verknüpften Tabellen.
    """
    result = {
        'employee': None,
        'groups': [],
        'current_shifts': [],
        'absences': [],
        'leave_entitlements': [],
        'notes': [],
        'restrictions': [],
        'overtime': [],
        'cycles': []
    ***REMOVED***
    
    # Basis-Mitarbeiterdaten
    emp_query = engine.query().select("5EMPL").where("id", "=", employee_id).execute()
    if emp_query.records:
        result['employee'] = emp_query.records[0]
    else:
        return result
    
    # Gruppen
    group_assignments = (engine.query()
                        .select("5GRASG")
                        .where("employee_id", "=", employee_id)
                        .join("5GROUP")
                        .execute())
    
    for assignment in group_assignments.records:
        if isinstance(assignment, dict) and "_relations" in assignment:
            groups = assignment["_relations"].get("5GROUP", [])
            result['groups'].extend(groups)
    
    # Aktuelle Schichten (letzte 30 Tage)
    from datetime import date, timedelta
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    
    shifts = (engine.query()
             .select("5SPSHI")
             .where("employee_id", "=", employee_id)
             .where_date_range("date", start_date, end_date)
             .join("5SHIFT")
             .join("5WOPL")
             .execute())
    
    result['current_shifts'] = shifts.to_dict()
    
    # Abwesenheiten (aktuelles Jahr)
    current_year = date.today().year
    absences = (engine.query()
               .select("5ABSEN")
               .where("employee_id", "=", employee_id)
               .where("date", ">=", date(current_year, 1, 1))
               .join("5LEAVT")
               .execute())
    
    result['absences'] = absences.to_dict()
    
    # Urlaubsansprüche
    entitlements = (engine.query()
                   .select("5LEAEN")
                   .where("employee_id", "=", employee_id)
                   .where("year", "=", current_year)
                   .join("5LEAVT")
                   .execute())
    
    result['leave_entitlements'] = entitlements.to_dict()
    
    # Notizen (letzte 10)
    notes = (engine.query()
            .select("5NOTE")
            .where("employee_id", "=", employee_id)
            .order_by("date", ascending=False)
            .limit(10)
            .execute())
    
    result['notes'] = notes.to_dict()
    
    return result
