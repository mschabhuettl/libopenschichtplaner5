# libopenschichtplaner5/src/libopenschichtplaner5/relationships.py
"""
Relationship management for Schichtplaner5 tables.
Defines how tables are related to each other and provides utilities for resolving references.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional, Set, Union, Iterator
from enum import Enum
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


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
        return hash((self.source_table, self.source_field, 
                    self.target_table, self.target_field))


@dataclass
class RelationshipManager:
    """Manages all table relationships."""
    relationships: Set[TableRelationship] = field(default_factory=set)
    _index_by_source: Dict[str, List[TableRelationship]] = field(default_factory=lambda: defaultdict(list))
    _index_by_target: Dict[str, List[TableRelationship]] = field(default_factory=lambda: defaultdict(list))
    _data_cache: Dict[Tuple[str, Any, str], Any] = field(default_factory=dict)
    
    def __post_init__(self):
        self._rebuild_indexes()
    
    def add_relationship(self, relationship: TableRelationship):
        """Add a relationship to the manager."""
        self.relationships.add(relationship)
        self._index_by_source[relationship.source_table].append(relationship)
        self._index_by_target[relationship.target_table].append(relationship)
    
    def _rebuild_indexes(self):
        """Rebuild internal indexes."""
        self._index_by_source.clear()
        self._index_by_target.clear()
        
        for rel in self.relationships:
            self._index_by_source[rel.source_table].append(rel)
            self._index_by_target[rel.target_table].append(rel)
    
    def get_relationships_from(self, table: str) -> List[TableRelationship]:
        """Get all relationships where the table is the source."""
        return self._index_by_source.get(table, [])
    
    def get_relationships_to(self, table: str) -> List[TableRelationship]:
        """Get all relationships where the table is the target."""
        return self._index_by_target.get(table, [])
    
    def get_all_related_tables(self, table: str) -> Set[str]:
        """Get all tables related to the given table."""
        related = set()
        
        # Tables this table points to
        for rel in self.get_relationships_from(table):
            related.add(rel.target_table)
        
        # Tables that point to this table
        for rel in self.get_relationships_to(table):
            related.add(rel.source_table)
        
        return related
    
    def resolve_reference(self, source_entities: List[Any], source_table: str,
                         target_table: str, target_entities: List[Any]) -> List[Tuple[Any, Any]]:
        """
        Resolve references between entities.
        Returns list of (source_entity, target_entity) tuples.
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
                        source_table=target_table,
                        source_field=rel.target_field,
                        target_table=source_table,
                        target_field=rel.source_field,
                        relationship_type=self._reverse_relationship_type(rel.relationship_type)
                    )
                    # Swap entities
                    source_entities, target_entities = target_entities, source_entities
                    source_table, target_table = target_table, source_table
                    break
        
        if not relationship:
            logger.warning(f"No relationship found between {source_table} and {target_table}")
            return []
        
        # Build index for fast lookup
        target_index = defaultdict(list)
        for target in target_entities:
            key_value = getattr(target, relationship.target_field, None)
            if key_value is not None:
                target_index[key_value].append(target)
        
        # Resolve references
        matches = []
        for source in source_entities:
            source_value = getattr(source, relationship.source_field, None)
            if source_value is not None and source_value in target_index:
                for target in target_index[source_value]:
                    matches.append((source, target))
        
        return matches
    
    def _reverse_relationship_type(self, rel_type: RelationType) -> RelationType:
        """Get the reverse of a relationship type."""
        if rel_type == RelationType.ONE_TO_MANY:
            return RelationType.MANY_TO_ONE
        elif rel_type == RelationType.MANY_TO_ONE:
            return RelationType.ONE_TO_MANY
        else:
            return rel_type
    
    def get_relationship_graph(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        """Get a graph representation of all relationships."""
        graph = defaultdict(lambda: defaultdict(dict))
        
        for rel in self.relationships:
            graph[rel.source_table][rel.target_table] = {
                "field": f"{rel.source_field} -> {rel.target_field}",
                "type": rel.relationship_type.value,
                "description": rel.description
            }
        
        return dict(graph)
    
    def validate_relationships(self, loaded_tables: Dict[str, List[Any]]) -> List[str]:
        """Validate that all defined relationships are valid."""
        errors = []
        
        for rel in self.relationships:
            # Check if tables exist
            if rel.source_table not in loaded_tables:
                errors.append(f"Source table {rel.source_table} not loaded")
                continue
            if rel.target_table not in loaded_tables:
                errors.append(f"Target table {rel.target_table} not loaded")
                continue
            
            # Check if fields exist (sample first record)
            if loaded_tables[rel.source_table]:
                sample = loaded_tables[rel.source_table][0]
                if not hasattr(sample, rel.source_field):
                    errors.append(f"Field {rel.source_field} not found in {rel.source_table}")
            
            if loaded_tables[rel.target_table]:
                sample = loaded_tables[rel.target_table][0]
                if not hasattr(sample, rel.target_field):
                    errors.append(f"Field {rel.target_field} not found in {rel.target_table}")
        
        return errors


# Global relationship manager instance
relationship_manager = RelationshipManager()


# Define all known relationships
def _define_relationships():
    """Define all standard Schichtplaner5 relationships."""
    
    # Employee relationships
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5ABSEN", "employee_id",
        RelationType.ONE_TO_MANY, "Employee has many absences"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5SPSHI", "employee_id",
        RelationType.ONE_TO_MANY, "Employee has many shift details"
    ))
    
    # Note: 5MASHI is sometimes used instead of 5SPSHI for employee shifts
    # We define both relationships to handle different versions
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5MASHI", "employee_id",
        RelationType.ONE_TO_MANY, "Employee has many shift assignments"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5NOTE", "employee_id",
        RelationType.ONE_TO_MANY, "Employee has many notes"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5GRASG", "employee_id",
        RelationType.ONE_TO_MANY, "Employee belongs to groups"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5LEAEN", "employee_id",
        RelationType.ONE_TO_MANY, "Employee has leave entitlements"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5CYASS", "employee_id",
        RelationType.ONE_TO_MANY, "Employee has cycle assignments"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5EMPL", "id", "5BOOK", "employee_id",
        RelationType.ONE_TO_MANY, "Employee has bookings"
    ))
    
    # Group relationships
    relationship_manager.add_relationship(TableRelationship(
        "5GROUP", "id", "5GRASG", "group_id",
        RelationType.ONE_TO_MANY, "Group has many assignments"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5GROUP", "superid", "5GROUP", "id",
        RelationType.MANY_TO_ONE, "Group has parent group"
    ))
    
    # Shift relationships
    relationship_manager.add_relationship(TableRelationship(
        "5SHIFT", "id", "5SPSHI", "shift_id",
        RelationType.ONE_TO_MANY, "Shift used in shift details"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5SHIFT", "id", "5MASHI", "shift_id",
        RelationType.ONE_TO_MANY, "Shift used in assignments"
    ))
    
    # Workplace relationships
    relationship_manager.add_relationship(TableRelationship(
        "5WOPL", "id", "5SPSHI", "workplace_id",
        RelationType.ONE_TO_MANY, "Workplace used in shift details"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5WOPL", "id", "5MASHI", "workplace_id",
        RelationType.ONE_TO_MANY, "Workplace used in assignments"
    ))
    
    # Leave type relationships
    relationship_manager.add_relationship(TableRelationship(
        "5LEAVT", "id", "5ABSEN", "leave_type_id",
        RelationType.ONE_TO_MANY, "Leave type used in absences"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5LEAVT", "id", "5LEAEN", "leave_type_id",
        RelationType.ONE_TO_MANY, "Leave type used in entitlements"
    ))
    
    # Cycle relationships
    relationship_manager.add_relationship(TableRelationship(
        "5CYCLE", "id", "5CYASS", "cycle_id",
        RelationType.ONE_TO_MANY, "Cycle has assignments"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5CYCLE", "id", "5CYENT", "cycle_id",
        RelationType.ONE_TO_MANY, "Cycle has entitlements"
    ))
    
    # User relationships
    relationship_manager.add_relationship(TableRelationship(
        "5USER", "id", "5EMACC", "user_id",
        RelationType.ONE_TO_MANY, "User has employee access"
    ))
    
    relationship_manager.add_relationship(TableRelationship(
        "5USER", "id", "5GRACC", "user_id",
        RelationType.ONE_TO_MANY, "User has group access"
    ))
    
    # Holiday relationships
    relationship_manager.add_relationship(TableRelationship(
        "5HOLID", "id", "5HOASS", "holiday_id",
        RelationType.ONE_TO_MANY, "Holiday has assignments"
    ))


# Initialize relationships
_define_relationships()


def get_entity_with_relations(entity: Any, table_name: str, 
                            loaded_tables: Dict[str, List[Any]],
                            max_depth: int = 1,
                            current_depth: int = 0) -> Dict[str, Any]:
    """
    Enrich an entity with its related data.
    Returns a dictionary with the entity and its relations.
    """
    if current_depth >= max_depth:
        return {"_entity": entity, "_table": table_name, "_relations": {}}
    
    result = {
        "_entity": entity,
        "_table": table_name,
        "_relations": {}
    }
    
    # Get all relationships from this table
    relationships = relationship_manager.get_relationships_from(table_name)
    
    for rel in relationships:
        if rel.target_table not in loaded_tables:
            continue
        
        # Find related entities
        source_value = getattr(entity, rel.source_field, None)
        if source_value is None:
            continue
        
        related_entities = []
        for target_entity in loaded_tables[rel.target_table]:
            target_value = getattr(target_entity, rel.target_field, None)
            if target_value == source_value:
                if rel.relationship_type == RelationType.ONE_TO_ONE:
                    result["_relations"][rel.target_table] = target_entity
                    break
                else:
                    related_entities.append(target_entity)
        
        if related_entities and rel.relationship_type != RelationType.ONE_TO_ONE:
            result["_relations"][rel.target_table] = related_entities
            
            # Recursive enrichment
            if current_depth < max_depth - 1:
                for i, related in enumerate(related_entities):
                    enriched = get_entity_with_relations(
                        related, rel.target_table, loaded_tables,
                        max_depth, current_depth + 1
                    )
                    if enriched["_relations"]:
                        related_entities[i] = enriched
    
    return result
