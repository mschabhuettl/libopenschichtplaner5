# src/libopenschichtplaner5/relationships_improved.py
"""
Improved relationship management with schema-based definitions and lazy loading.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional, Set, Union, Iterator
from pathlib import Path
from enum import Enum
import json
from collections import defaultdict


class RelationType(Enum):
    """Types of relationships between tables."""
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"
    MANY_TO_MANY = "N:N"


@dataclass
class FieldMapping:
    """Maps DBF field names to Python attribute names."""
    dbf_field: str
    python_attribute: str
    data_type: Optional[str] = None
    is_key: bool = False


@dataclass
class RelationshipSchema:
    """Schema definition for a relationship."""
    source_table: str
    target_table: str
    relationship_type: RelationType
    source_field: FieldMapping
    target_field: FieldMapping
    description: str = ""
    lazy_load: bool = True
    cascade_delete: bool = False
    
    def __hash__(self):
        return hash((self.source_table, self.source_field.python_attribute, 
                    self.target_table, self.target_field.python_attribute))


class RelationshipIndex:
    """Optimized index for fast relationship lookups."""
    
    def __init__(self):
        self._forward_index: Dict[str, Dict[Any, List[Any]]] = defaultdict(lambda: defaultdict(list))
        self._reverse_index: Dict[str, Dict[Any, List[Any]]] = defaultdict(lambda: defaultdict(list))
        self._one_to_one_index: Dict[str, Dict[Any, Any]] = defaultdict(dict)
    
    def build_index(self, table_name: str, records: List[Any], key_field: str, 
                   relationship_type: RelationType):
        """Build index for a table."""
        if relationship_type == RelationType.ONE_TO_ONE:
            index = self._one_to_one_index[table_name]
            for record in records:
                key_value = getattr(record, key_field, None)
                if key_value is not None:
                    index[key_value] = record
        else:
            # One-to-many or many-to-many
            index = self._forward_index[table_name]
            for record in records:
                key_value = getattr(record, key_field, None)
                if key_value is not None:
                    index[key_value].append(record)
    
    def lookup(self, table_name: str, key_value: Any, 
              relationship_type: RelationType) -> Union[Any, List[Any], None]:
        """Fast lookup of related records."""
        if relationship_type == RelationType.ONE_TO_ONE:
            return self._one_to_one_index[table_name].get(key_value)
        else:
            return self._forward_index[table_name].get(key_value, [])


@dataclass 
class RelationshipResolver:
    """Resolves relationships with caching and lazy loading."""
    schemas: Set[RelationshipSchema] = field(default_factory=set)
    _schema_index: Dict[str, List[RelationshipSchema]] = field(default_factory=dict)
    _data_index: RelationshipIndex = field(default_factory=RelationshipIndex)
    _cache: Dict[Tuple, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        self._build_schema_index()
    
    def add_schema(self, schema: RelationshipSchema):
        """Add a relationship schema."""
        self.schemas.add(schema)
        self._build_schema_index()
    
    def _build_schema_index(self):
        """Build index for quick schema lookup."""
        self._schema_index.clear()
        for schema in self.schemas:
            # Index by source table
            if schema.source_table not in self._schema_index:
                self._schema_index[schema.source_table] = []
            self._schema_index[schema.source_table].append(schema)
    
    def build_data_indexes(self, loaded_tables: Dict[str, List[Any]]):
        """Build optimized indexes for all loaded data."""
        for schema in self.schemas:
            if schema.target_table in loaded_tables:
                self._data_index.build_index(
                    schema.target_table,
                    loaded_tables[schema.target_table],
                    schema.target_field.python_attribute,
                    schema.relationship_type
                )
    
    def resolve_relationship(self, source_record: Any, source_table: str, 
                           target_table: str, use_cache: bool = True) -> Any:
        """Resolve a specific relationship for a record."""
        # Find matching schema
        schema = self._find_schema(source_table, target_table)
        if not schema:
            return None
        
        # Check cache
        cache_key = (id(source_record), source_table, target_table)
        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]
        
        # Get source value
        source_value = getattr(source_record, schema.source_field.python_attribute, None)
        if source_value is None:
            return None
        
        # Lookup related records
        result = self._data_index.lookup(
            target_table, 
            source_value, 
            schema.relationship_type
        )
        
        # Cache result
        if use_cache:
            self._cache[cache_key] = result
        
        return result
    
    def resolve_all_relationships(self, source_record: Any, source_table: str,
                                max_depth: int = 1, current_depth: int = 0) -> Dict[str, Any]:
        """Resolve all relationships for a record recursively."""
        if current_depth >= max_depth:
            return {}
        
        relationships = {}
        schemas = self._schema_index.get(source_table, [])
        
        for schema in schemas:
            if schema.lazy_load and current_depth > 0:
                # Skip lazy-loaded relationships in deep recursion
                continue
            
            related = self.resolve_relationship(source_record, source_table, schema.target_table)
            if related is not None:
                relationships[schema.target_table] = related
                
                # Recursive resolution for related records
                if current_depth < max_depth - 1:
                    if isinstance(related, list):
                        for rel_record in related:
                            sub_relations = self.resolve_all_relationships(
                                rel_record, schema.target_table, 
                                max_depth, current_depth + 1
                            )
                            if sub_relations:
                                if not hasattr(rel_record, '_relations'):
                                    rel_record._relations = {}
                                rel_record._relations.update(sub_relations)
                    else:
                        sub_relations = self.resolve_all_relationships(
                            related, schema.target_table,
                            max_depth, current_depth + 1
                        )
                        if sub_relations:
                            if not hasattr(related, '_relations'):
                                related._relations = {}
                            related._relations.update(sub_relations)
        
        return relationships
    
    def _find_schema(self, source_table: str, target_table: str) -> Optional[RelationshipSchema]:
        """Find schema for a specific relationship."""
        schemas = self._schema_index.get(source_table, [])
        for schema in schemas:
            if schema.target_table == target_table:
                return schema
        return None
    
    def get_related_tables(self, table_name: str) -> Set[str]:
        """Get all tables related to a specific table."""
        related = set()
        
        # Tables this table points to
        for schema in self._schema_index.get(table_name, []):
            related.add(schema.target_table)
        
        # Tables that point to this table
        for schemas in self._schema_index.values():
            for schema in schemas:
                if schema.target_table == table_name:
                    related.add(schema.source_table)
        
        return related
    
    def clear_cache(self):
        """Clear the relationship cache."""
        self._cache.clear()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get relationship resolver statistics."""
        return {
            "schemas_count": len(self.schemas),
            "cached_relationships": len(self._cache),
            "indexed_tables": len(self._data_index._forward_index),
            "memory_usage_estimate": len(self._cache) * 64  # Rough estimate
        }


def load_relationships_from_json(json_path: Path) -> RelationshipResolver:
    """Load relationship definitions from JSON file."""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    resolver = RelationshipResolver()
    
    for rel_data in data.get('relationships', []):
        source_field = FieldMapping(
            dbf_field=rel_data['source_field']['dbf_field'],
            python_attribute=rel_data['source_field']['python_attribute'],
            data_type=rel_data['source_field'].get('data_type'),
            is_key=rel_data['source_field'].get('is_key', False)
        )
        
        target_field = FieldMapping(
            dbf_field=rel_data['target_field']['dbf_field'],
            python_attribute=rel_data['target_field']['python_attribute'],
            data_type=rel_data['target_field'].get('data_type'),
            is_key=rel_data['target_field'].get('is_key', False)
        )
        
        schema = RelationshipSchema(
            source_table=rel_data['source_table'],
            target_table=rel_data['target_table'],
            relationship_type=RelationType(rel_data['relationship_type']),
            source_field=source_field,
            target_field=target_field,
            description=rel_data.get('description', ''),
            lazy_load=rel_data.get('lazy_load', True),
            cascade_delete=rel_data.get('cascade_delete', False)
        )
        
        resolver.add_schema(schema)
    
    return resolver


def create_default_resolver() -> RelationshipResolver:
    """Create resolver with default Schichtplaner5 relationships."""
    resolver = RelationshipResolver()
    
    # Employee relationships
    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5ABSEN", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has many absences"
    ))
    
    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5MASHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has many shifts"
    ))
    
    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5SPSHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has many shift details"
    ))
    
    # Shift relationships  
    resolver.add_schema(RelationshipSchema(
        "5SHIFT", "5MASHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("SHIFTID", "shift_id"),
        "Shift used in employee shifts"
    ))
    
    resolver.add_schema(RelationshipSchema(
        "5SHIFT", "5SPSHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("SHIFTID", "shift_id"),
        "Shift used in shift details"
    ))
    
    # Group relationships
    resolver.add_schema(RelationshipSchema(
        "5GROUP", "5GRASG", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("GROUPID", "group_id"),
        "Group has many assignments"
    ))
    
    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5GRASG", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee belongs to groups"
    ))
    
    # Leave type relationships
    resolver.add_schema(RelationshipSchema(
        "5LEAVT", "5ABSEN", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("LEAVETYPID", "leave_type_id"),
        "Leave type used in absences"
    ))
    
    # Workplace relationships
    resolver.add_schema(RelationshipSchema(
        "5WOPL", "5MASHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("WORKPLACID", "workplace_id"),
        "Workplace used in shifts"
    ))
    
    return resolver


# Global instance for backward compatibility
default_resolver = create_default_resolver()