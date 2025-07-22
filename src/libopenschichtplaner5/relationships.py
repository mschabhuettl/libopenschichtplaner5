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
        self.add_relationship("5EMPL", "ID", "5NOTE", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many notes")
        self.add_relationship("5EMPL", "ID", "5ABSEN", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many absences")
        self.add_relationship("5EMPL", "ID", "5SPSHI", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many shift details")
        self.add_relationship("5EMPL", "ID", "5MASHI", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many shifts")
        self.add_relationship("5EMPL", "ID", "5CYASS", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many cycle assignments")
        self.add_relationship("5EMPL", "ID", "5GRASG", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee belongs to many groups")
        self.add_relationship("5EMPL", "ID", "5LEAEN", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many leave entitlements")
        self.add_relationship("5EMPL", "ID", "5BOOK", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many bookings")
        self.add_relationship("5EMPL", "ID", "5RESTR", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has many shift restrictions")
        self.add_relationship("5EMPL", "ID", "5EMACC", "EMPLOYEEID", RelationType.ONE_TO_MANY,
                              "Employee has access rights")

        # Group relationships
        self.add_relationship("5GROUP", "ID", "5GRASG", "GROUPID", RelationType.ONE_TO_MANY,
                              "Group has many employee assignments")
        self.add_relationship("5GROUP", "ID", "5GRACC", "GROUPID", RelationType.ONE_TO_MANY,
                              "Group has many access definitions")
        self.add_relationship("5GROUP", "ID", "5PERIO", "GROUPID", RelationType.ONE_TO_MANY,
                              "Group has many periods")
        self.add_relationship("5GROUP", "ID", "5DADEM", "GROUPID", RelationType.ONE_TO_MANY,
                              "Group has daily demands")
        self.add_relationship("5GROUP", "ID", "5SHDEM", "GROUPID", RelationType.ONE_TO_MANY,
                              "Group has shift schedules")

        # Shift relationships
        self.add_relationship("5SHIFT", "ID", "5SPSHI", "SHIFTID", RelationType.ONE_TO_MANY,
                              "Shift appears in many shift details")
        self.add_relationship("5SHIFT", "ID", "5NOTE", "SHIFTID", RelationType.ONE_TO_MANY,
                              "Shift can have notes")
        self.add_relationship("5SHIFT", "ID", "5CYENT", "SHIFTID", RelationType.ONE_TO_MANY,
                              "Shift used in cycle entitlements")
        self.add_relationship("5SHIFT", "ID", "5RESTR", "SHIFTID", RelationType.ONE_TO_MANY,
                              "Shift can have restrictions")
        self.add_relationship("5SHIFT", "ID", "5SHDEM", "SHIFTID", RelationType.ONE_TO_MANY,
                              "Shift appears in schedules")
        self.add_relationship("5SHIFT", "ID", "5MASHI", "SHIFTID", RelationType.ONE_TO_MANY,
                              "Shift assigned to employees")

        # Leave type relationships
        self.add_relationship("5LEAVT", "ID", "5LEAEN", "LEAVETYPID", RelationType.ONE_TO_MANY,
                              "Leave type used in entitlements")
        self.add_relationship("5LEAVT", "ID", "5ABSEN", "LEAVETYPID", RelationType.ONE_TO_MANY,
                              "Leave type used in absences")

        # Cycle relationships
        self.add_relationship("5CYCLE", "ID", "5CYASS", "CYCLEID", RelationType.ONE_TO_MANY,
                              "Cycle has many assignments")
        self.add_relationship("5CYCLE", "ID", "5CYENT", "CYCLEID", RelationType.ONE_TO_MANY,
                              "Cycle has entitlements")

        # Work location relationships
        self.add_relationship("5WOPL", "ID", "5SPSHI", "WORKPLACID", RelationType.ONE_TO_MANY,
                              "Work location used in shift details")
        self.add_relationship("5WOPL", "ID", "5MASHI", "WORKPLACID", RelationType.ONE_TO_MANY,
                              "Work location used in employee shifts")
        self.add_relationship("5WOPL", "ID", "5CYENT", "WORKPLACID", RelationType.ONE_TO_MANY,
                              "Work location in cycle entitlements")
        self.add_relationship("5WOPL", "ID", "5SHDEM", "WORKPLACID", RelationType.ONE_TO_MANY,
                              "Work location in shift schedules")

        # User relationships
        self.add_relationship("5USER", "ID", "5EMACC", "USERID", RelationType.ONE_TO_MANY,
                              "User has employee access rights")
        self.add_relationship("5USER", "ID", "5GRACC", "USERID", RelationType.ONE_TO_MANY,
                              "User has group access rights")

        # Holiday relationships
        self.add_relationship("5HOLID", "ID", "5HOBAN", "HOLIDAY_ID", RelationType.ONE_TO_MANY,
                              "Holiday has assignments")

        # Additional complex relationships
        self.add_relationship("5CYASS", "ID", "5CYEXC", "CYCLEASSID", RelationType.ONE_TO_MANY,
                              "Cycle assignment has exceptions")

    def add_relationship(self, source_table: str, source_field: str,
                         target_table: str, target_field: str,
                         relationship_type: RelationType, description: str = ""):
        """Add a relationship definition."""
        rel = TableRelationship(source_table, source_field, target_table,
                                target_field, relationship_type, description)
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
                        rel.relationship_type, rel.description
                    )
                    source_data, target_data = target_data, source_data
                    break

        if not relationship:
            raise ValueError(f"No relationship found between {source_table***REMOVED*** and {target_table***REMOVED***")

        # Build index on target data for efficient lookup
        target_index = {***REMOVED***
        for record in target_data:
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
                "description": rel.description
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
                if not hasattr(sample, rel.source_field):
                    errors.append(f"Field {rel.source_field***REMOVED*** not found in {rel.source_table***REMOVED***")

            if loaded_tables[rel.target_table]:
                sample = loaded_tables[rel.target_table][0]
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