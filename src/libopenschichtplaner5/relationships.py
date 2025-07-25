# src/libopenschichtplaner5/relationships.py
"""
Flexible relationship management system for cross-referencing between DBF tables.
This module provides a declarative way to define relationships between tables
and automatically resolve references.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional, Set
from pathlib import Path
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
    # Neue Felder für die Mapping zwischen DBF-Feldnamen und Python-Attributen
    source_attribute: Optional[str] = None
    target_attribute: Optional[str] = None

    def __hash__(self):
        return hash((self.source_table, self.source_field, self.target_table, self.target_field))


@dataclass
class RelationshipManager:
    """Manages all relationships between tables and provides resolution methods."""
    relationships: Set[TableRelationship] = field(default_factory=set)
    _index: Dict[str, List[TableRelationship]] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize the relationship definitions."""
        self._define_relationships()
        self._build_index()

    def _define_relationships(self):
        """Define all known relationships between tables."""
        # Employee relationships
        self.add_relationship("5EMPL", "id", "5ABSEN", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has many absences", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5SPSHI", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has many shift details", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5MASHI", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has many shifts", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5CYASS", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has many cycle assignments", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5GRASG", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee belongs to many groups", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5LEAEN", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has many leave entitlements", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5BOOK", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has many bookings", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5RESTR", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has many shift restrictions", "ID", "EMPLOYEEID")
        self.add_relationship("5EMPL", "id", "5EMACC", "employee_id", RelationType.ONE_TO_MANY,
                              "Employee has access rights", "ID", "EMPLOYEEID")

        # Group relationships
        self.add_relationship("5GROUP", "id", "5GRASG", "group_id", RelationType.ONE_TO_MANY,
                              "Group has many employee assignments", "ID", "GROUPID")
        self.add_relationship("5GROUP", "id", "5GRACC", "group_id", RelationType.ONE_TO_MANY,
                              "Group has many access definitions", "ID", "GROUPID")
        self.add_relationship("5GROUP", "id", "5PERIO", "group_id", RelationType.ONE_TO_MANY,
                              "Group has many periods", "ID", "GROUPID")
        self.add_relationship("5GROUP", "id", "5DADEM", "group_id", RelationType.ONE_TO_MANY,
                              "Group has daily demands", "ID", "GROUPID")
        self.add_relationship("5GROUP", "id", "5SHDEM", "group_id", RelationType.ONE_TO_MANY,
                              "Group has shift schedules", "ID", "GROUPID")

        # Shift relationships
        self.add_relationship("5SHIFT", "id", "5SPSHI", "shift_id", RelationType.ONE_TO_MANY,
                              "Shift appears in many shift details", "ID", "SHIFTID")
        self.add_relationship("5SHIFT", "id", "5NOTE", "shift_id", RelationType.ONE_TO_MANY,
                              "Shift can have notes", "ID", "SHIFTID")
        self.add_relationship("5SHIFT", "id", "5CYENT", "shift_id", RelationType.ONE_TO_MANY,
                              "Shift used in cycle entitlements", "ID", "SHIFTID")
        self.add_relationship("5SHIFT", "id", "5RESTR", "shift_id", RelationType.ONE_TO_MANY,
                              "Shift can have restrictions", "ID", "SHIFTID")
        self.add_relationship("5SHIFT", "id", "5SHDEM", "shift_id", RelationType.ONE_TO_MANY,
                              "Shift appears in schedules", "ID", "SHIFTID")
        self.add_relationship("5SHIFT", "id", "5MASHI", "shift_id", RelationType.ONE_TO_MANY,
                              "Shift assigned to employees", "ID", "SHIFTID")

        # Leave type relationships
        self.add_relationship("5LEAVT", "id", "5LEAEN", "leave_type_id", RelationType.ONE_TO_MANY,
                              "Leave type used in entitlements", "ID", "LEAVETYPID")
        self.add_relationship("5LEAVT", "id", "5ABSEN", "leave_type_id", RelationType.ONE_TO_MANY,
                              "Leave type used in absences", "ID", "LEAVETYPID")

        # Cycle relationships
        self.add_relationship("5CYCLE", "id", "5CYASS", "cycle_id", RelationType.ONE_TO_MANY,
                              "Cycle has many assignments", "ID", "CYCLEID")
        self.add_relationship("5CYCLE", "id", "5CYENT", "cycle_id", RelationType.ONE_TO_MANY,
                              "Cycle has entitlements", "ID", "CYCLEID")

        # Work location relationships
        self.add_relationship("5WOPL", "id", "5SPSHI", "workplace_id", RelationType.ONE_TO_MANY,
                              "Work location used in shift details", "ID", "WORKPLACID")
        self.add_relationship("5WOPL", "id", "5MASHI", "workplace_id", RelationType.ONE_TO_MANY,
                              "Work location used in employee shifts", "ID", "WORKPLACID")
        self.add_relationship("5WOPL", "id", "5CYENT", "workplace_id", RelationType.ONE_TO_MANY,
                              "Work location in cycle entitlements", "ID", "WORKPLACID")
        self.add_relationship("5WOPL", "id", "5SHDEM", "workplace_id", RelationType.ONE_TO_MANY,
                              "Work location in shift schedules", "ID", "WORKPLACID")

        # User relationships
        self.add_relationship("5USER", "id", "5EMACC", "user_id", RelationType.ONE_TO_MANY,
                              "User has employee access rights", "ID", "USERID")
        self.add_relationship("5USER", "id", "5GRACC", "user_id", RelationType.ONE_TO_MANY,
                              "User has group access rights", "ID", "USERID")

        # Holiday relationships
        self.add_relationship("5HOLID", "id", "5HOBAN", "holiday_id", RelationType.ONE_TO_MANY,
                              "Holiday has assignments", "ID", "HOLIDAY_ID")

        # Additional complex relationships
        self.add_relationship("5CYASS", "id", "5CYEXC", "cycle_ass_id", RelationType.ONE_TO_MANY,
                              "Cycle assignment has exceptions", "ID", "CYCLEASSID")

        # WICHTIG: Korrigierte MASHI Relationships
        self.add_relationship("5MASHI", "shift_id", "5SHIFT", "id", RelationType.MANY_TO_ONE,
                              "Employee shift references shift definition", "SHIFTID", "ID")
        self.add_relationship("5MASHI", "workplace_id", "5WOPL", "id", RelationType.MANY_TO_ONE,
                              "Employee shift at workplace", "WORKPLACID", "ID")
        self.add_relationship("5MASHI", "employee_id", "5EMPL", "id", RelationType.MANY_TO_ONE,
                              "Employee shift belongs to employee", "EMPLOYEEID", "ID")

    def add_relationship(self, source_table: str, source_field: str,
                         target_table: str, target_field: str,
                         relationship_type: RelationType, description: str = "",
                         source_dbf_field: Optional[str] = None,
                         target_dbf_field: Optional[str] = None):
        """Add a relationship definition."""
        rel = TableRelationship(
            source_table, source_field, target_table,
            target_field, relationship_type, description,
            source_dbf_field, target_dbf_field
        )
        self.relationships.add(rel)

    def _build_index(self):
        """Build index for quick lookup of relationships."""
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

    def get_relationships_from(self, table: str) -> List[TableRelationship]:
        """Get all relationships where the given table is the source."""
        return self._index.get(table, [])

    def get_relationships_to(self, table: str) -> List[TableRelationship]:
        """Get all relationships where the given table is the target."""
        return self._index.get(f"_reverse_{table***REMOVED***", [])

    def get_all_related_tables(self, table: str) -> Set[str]:
        """Get all tables that have any relationship with the given table."""
        related = set()

        # Tables this table points to
        for rel in self.get_relationships_from(table):
            related.add(rel.target_table)

        # Tables that point to this table
        for rel in self.get_relationships_to(table):
            related.add(rel.source_table)

        return related

    def resolve_reference(self, source_data: List[Any], source_table: str,
                          target_table: str, target_data: List[Any]) -> List[Tuple[Any, Any]]:
        """
        Resolve references between two tables.
        Returns list of tuples (source_record, target_record) for matching records.
        """
        # Find the relationship
        relationship = None
        for rel in self.get_relationships_from(source_table):
            if rel.target_table == target_table:
                relationship = rel
                break

        if not relationship:
            # Try reverse relationship
            for rel in self.get_relationships_to(source_table):
                if rel.source_table == target_table:
                    # Swap the relationship direction
                    relationship = TableRelationship(
                        target_table, rel.target_field,
                        source_table, rel.source_field,
                        rel.relationship_type, rel.description,
                        rel.target_attribute, rel.source_attribute
                    )
                    source_data, target_data = target_data, source_data
                    break

        if not relationship:
            raise ValueError(f"No relationship found between {source_table***REMOVED*** and {target_table***REMOVED***")

        # Build index on target data for efficient lookup
        target_index = {***REMOVED***
        for record in target_data:
            # Use Python attribute name for accessing the field
            key_value = getattr(record, relationship.target_field, None)
            if key_value is not None:
                if relationship.relationship_type in [RelationType.ONE_TO_MANY, RelationType.MANY_TO_MANY]:
                    if key_value not in target_index:
                        target_index[key_value] = []
                    target_index[key_value].append(record)
                else:
                    target_index[key_value] = record

        # Resolve references
        matches = []
        for source_record in source_data:
            # Use Python attribute name for accessing the field
            source_value = getattr(source_record, relationship.source_field, None)
            if source_value is not None and source_value in target_index:
                if isinstance(target_index[source_value], list):
                    for target_record in target_index[source_value]:
                        matches.append((source_record, target_record))
                else:
                    matches.append((source_record, target_index[source_value]))

        return matches

    def get_relationship_graph(self) -> Dict[str, Dict[str, str]]:
        """
        Generate a graph representation of all relationships.
        Useful for visualization or documentation.
        """
        graph = {***REMOVED***
        for rel in self.relationships:
            if rel.source_table not in graph:
                graph[rel.source_table] = {***REMOVED***

            key = f"{rel.target_table***REMOVED***.{rel.target_field***REMOVED***"
            graph[rel.source_table][key] = {
                "field": rel.source_field,
                "type": rel.relationship_type.value,
                "description": rel.description,
                "dbf_source_field": rel.source_attribute,
                "dbf_target_field": rel.target_attribute
            ***REMOVED***

        return graph

    def validate_relationships(self, loaded_tables: Dict[str, List[Any]]) -> List[str]:
        """
        Validate that all defined relationships have valid fields in loaded data.
        Returns list of validation errors.
        """
        errors = []

        for rel in self.relationships:
            # Check if tables exist
            if rel.source_table not in loaded_tables:
                errors.append(f"Source table {rel.source_table***REMOVED*** not loaded")
                continue
            if rel.target_table not in loaded_tables:
                errors.append(f"Target table {rel.target_table***REMOVED*** not loaded")
                continue

            # Check if fields exist (sample first record)
            if loaded_tables[rel.source_table]:
                sample = loaded_tables[rel.source_table][0]
                # Check Python attribute name (lowercase)
                if not hasattr(sample, rel.source_field):
                    errors.append(f"Field {rel.source_field***REMOVED*** not found in {rel.source_table***REMOVED***")

            if loaded_tables[rel.target_table]:
                sample = loaded_tables[rel.target_table][0]
                # Check Python attribute name (lowercase)
                if not hasattr(sample, rel.target_field):
                    errors.append(f"Field {rel.target_field***REMOVED*** not found in {rel.target_table***REMOVED***")

        return errors


# Singleton instance
relationship_manager = RelationshipManager()


def get_entity_with_relations(entity: Any, entity_table: str,
                              loaded_tables: Dict[str, List[Any]],
                              max_depth: int = 2) -> Dict[str, Any]:
    """
    Get an entity with all its related data resolved.

    Args:
        entity: The main entity to enrich
        entity_table: Table name of the entity
        loaded_tables: Dictionary of all loaded tables
        max_depth: Maximum depth for recursive resolution

    Returns:
        Dictionary with entity data and resolved relations
    """
    result = {
        "_entity": entity,
        "_table": entity_table,
        "_relations": {***REMOVED***
    ***REMOVED***

    if max_depth <= 0:
        return result

    # Get all relationships from this table
    for rel in relationship_manager.get_relationships_from(entity_table):
        if rel.target_table not in loaded_tables:
            continue

        # Find matching records
        entity_value = getattr(entity, rel.source_field, None)
        if entity_value is None:
            continue

        matches = []
        for target_record in loaded_tables[rel.target_table]:
            if getattr(target_record, rel.target_field, None) == entity_value:
                # Recursively resolve relations for matched records
                if max_depth > 1:
                    enriched = get_entity_with_relations(
                        target_record, rel.target_table,
                        loaded_tables, max_depth - 1
                    )
                    matches.append(enriched)
                else:
                    matches.append(target_record)

        if matches:
            if rel.relationship_type in [RelationType.ONE_TO_ONE, RelationType.MANY_TO_ONE]:
                result["_relations"][rel.target_table] = matches[0]
            else:
                result["_relations"][rel.target_table] = matches

    return result