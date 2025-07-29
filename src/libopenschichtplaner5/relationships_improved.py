# src/libopenschichtplaner5/relationships_improved.py
"""
Improved relationship management with caching and performance optimizations.
Replaces the original relationships.py with a more efficient implementation.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any, Optional, Set, Union
from enum import Enum
from collections import defaultdict
from functools import lru_cache
import time

logger = logging.getLogger(__name__)


class RelationType(Enum):
    """Types of relationships between tables."""
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"
    MANY_TO_MANY = "N:N"


@dataclass
class FieldMapping:
    """Maps a DBF field to a model attribute."""
    dbf_field: str
    model_attr: str

    def get_value(self, obj: Any) -> Any:
        """Get the mapped value from an object."""
        return getattr(obj, self.model_attr, None)


@dataclass
class RelationshipSchema:
    """Enhanced relationship definition with field mappings."""
    source_table: str
    target_table: str
    relationship_type: RelationType
    source_field: FieldMapping
    target_field: FieldMapping
    description: str = ""
    cascade_delete: bool = False
    lazy_load: bool = True

    def __hash__(self):
        return hash((self.source_table, self.target_table,
                     self.source_field.model_attr, self.target_field.model_attr))


class RelationshipIndex:
    """Efficient index for relationship lookups."""

    def __init__(self):
        self._indexes: Dict[str, Dict[Any, Union[Any, List[Any]]]] = defaultdict(dict)
        self._stats = defaultdict(int)

    def build_index(self, table_name: str, records: List[Any],
                    field_name: str, rel_type: RelationType):
        """Build an index for fast lookups."""
        start_time = time.time()
        index = {}

        for record in records:
            key = getattr(record, field_name, None)
            if key is not None:
                if rel_type == RelationType.ONE_TO_ONE:
                    index[key] = record
                else:
                    if key not in index:
                        index[key] = []
                    index[key].append(record)

        self._indexes[f"{table_name}:{field_name}"] = index

        # Stats
        elapsed = time.time() - start_time
        self._stats['builds'] += 1
        self._stats['build_time'] += elapsed
        logger.debug(f"Built index for {table_name}.{field_name} in {elapsed:.3f}s")

    def lookup(self, table_name: str, key: Any,
               rel_type: RelationType) -> Union[Any, List[Any], None]:
        """Fast lookup using index."""
        index_key = f"{table_name}:{key}"
        # For simple lookups, just use the field name as key
        for idx_key, index in self._indexes.items():
            if idx_key.startswith(f"{table_name}:"):
                result = index.get(key)
                self._stats['lookups'] += 1
                if result is not None:
                    self._stats['hits'] += 1
                return result

        self._stats['misses'] += 1
        return [] if rel_type != RelationType.ONE_TO_ONE else None

    def get_statistics(self) -> Dict[str, Any]:
        """Get index statistics."""
        return dict(self._stats)


class RelationshipResolver:
    """Optimized relationship resolver with caching."""

    def __init__(self, cache_size: int = 1000):
        self.schemas: Set[RelationshipSchema] = set()
        self._schema_index: Dict[str, List[RelationshipSchema]] = defaultdict(list)
        self._indexes = RelationshipIndex()
        self._cache: Dict[Tuple[Any, str, str], Any] = {}
        self._cache_size = cache_size
        self._stats = defaultdict(int)

    def add_schema(self, schema: RelationshipSchema):
        """Add a relationship schema."""
        self.schemas.add(schema)
        self._schema_index[schema.source_table].append(schema)
        logger.debug(f"Added schema: {schema.source_table} -> {schema.target_table}")

    def build_data_indexes(self, loaded_tables: Dict[str, List[Any]]):
        """Build all indexes for loaded data."""
        logger.info("Building relationship indexes...")
        start_time = time.time()

        for schema in self.schemas:
            if schema.target_table in loaded_tables:
                target_records = loaded_tables[schema.target_table]
                self._indexes.build_index(
                    schema.target_table,
                    target_records,
                    schema.target_field.model_attr,
                    schema.relationship_type
                )

        elapsed = time.time() - start_time
        logger.info(f"Built all indexes in {elapsed:.3f}s")

    @lru_cache(maxsize=128)
    def _get_schemas_for_tables(self, source_table: str,
                                target_table: str) -> List[RelationshipSchema]:
        """Get schemas between two tables (cached)."""
        schemas = []
        for schema in self._schema_index.get(source_table, []):
            if schema.target_table == target_table:
                schemas.append(schema)
        return schemas

    def resolve_relationship(self, source_entity: Any, source_table: str,
                             target_table: str) -> Union[Any, List[Any]]:
        """Resolve a relationship with caching."""
        # Check cache first
        cache_key = (id(source_entity), source_table, target_table)
        if cache_key in self._cache:
            self._stats['cache_hits'] += 1
            return self._cache[cache_key]

        self._stats['cache_misses'] += 1

        # Find matching schema
        schemas = self._get_schemas_for_tables(source_table, target_table)
        if not schemas:
            logger.warning(f"No schema found: {source_table} -> {target_table}")
            return []

        schema = schemas[0]  # Use first matching schema

        # Get source value
        source_value = schema.source_field.get_value(source_entity)
        if source_value is None:
            result = [] if schema.relationship_type != RelationType.ONE_TO_ONE else None
        else:
            # Use index for lookup
            result = self._indexes.lookup(
                target_table,
                source_value,
                schema.relationship_type
            )

        # Cache result
        if len(self._cache) >= self._cache_size:
            # Simple cache eviction - remove first item
            self._cache.pop(next(iter(self._cache)))
        self._cache[cache_key] = result

        return result

    def resolve_all_relationships(self, entity: Any, table_name: str,
                                  loaded_tables: Dict[str, List[Any]],
                                  max_depth: int = 1,
                                  current_depth: int = 0) -> Dict[str, Any]:
        """Resolve all relationships for an entity recursively."""
        if current_depth >= max_depth:
            return {}

        result = {}

        for schema in self._schema_index.get(table_name, []):
            if schema.target_table not in loaded_tables:
                continue

            related = self.resolve_relationship(entity, table_name, schema.target_table)
            if related:
                result[schema.target_table] = related

                # Recursive resolution
                if current_depth < max_depth - 1:
                    if isinstance(related, list):
                        for i, rel_entity in enumerate(related[:5]):  # Limit recursion
                            sub_relations = self.resolve_all_relationships(
                                rel_entity, schema.target_table, loaded_tables,
                                max_depth, current_depth + 1
                            )
                            if sub_relations:
                                if isinstance(result[schema.target_table], list):
                                    result[schema.target_table][i] = {
                                        "_entity": rel_entity,
                                        "_relations": sub_relations
                                    }
                    else:
                        sub_relations = self.resolve_all_relationships(
                            related, schema.target_table, loaded_tables,
                            max_depth, current_depth + 1
                        )
                        if sub_relations:
                            result[schema.target_table] = {
                                "_entity": related,
                                "_relations": sub_relations
                            }

        return result

    def get_statistics(self) -> Dict[str, Any]:
        """Get resolver statistics."""
        stats = dict(self._stats)
        stats['schemas_count'] = len(self.schemas)
        stats['cached_relationships'] = len(self._cache)
        stats['index_stats'] = self._indexes.get_statistics()

        # Memory usage estimate
        import sys
        stats['memory_usage_estimate'] = sys.getsizeof(self._cache) + \
                                         sys.getsizeof(self._indexes._indexes)

        return stats

    def clear_cache(self):
        """Clear the relationship cache."""
        self._cache.clear()
        logger.info("Relationship cache cleared")

    def validate_relationships(self, loaded_tables: Dict[str, List[Any]]) -> List[str]:
        """Validate all defined relationships."""
        errors = []

        for schema in self.schemas:
            # Check tables exist
            if schema.source_table not in loaded_tables:
                errors.append(f"Source table {schema.source_table} not loaded")
                continue
            if schema.target_table not in loaded_tables:
                errors.append(f"Target table {schema.target_table} not loaded")
                continue

            # Check fields exist (sample first record if available)
            if loaded_tables[schema.source_table]:
                sample = loaded_tables[schema.source_table][0]
                if not hasattr(sample, schema.source_field.model_attr):
                    errors.append(
                        f"Field {schema.source_field.model_attr} not found in {schema.source_table}"
                    )

            if loaded_tables[schema.target_table]:
                sample = loaded_tables[schema.target_table][0]
                if not hasattr(sample, schema.target_field.model_attr):
                    errors.append(
                        f"Field {schema.target_field.model_attr} not found in {schema.target_table}"
                    )

        return errors


def create_default_resolver() -> RelationshipResolver:
    """Create resolver with all standard Schichtplaner5 relationships."""
    resolver = RelationshipResolver()

    # Employee relationships
    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5ABSEN", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has many absences"
    ))

    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5SPSHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has many shift details"
    ))

    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5MASHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has many shift assignments"
    ))

    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5NOTE", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has many notes"
    ))

    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5GRASG", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee belongs to groups"
    ))

    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5LEAEN", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has leave entitlements"
    ))

    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5CYASS", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has cycle assignments"
    ))

    resolver.add_schema(RelationshipSchema(
        "5EMPL", "5BOOK", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id"),
        "Employee has bookings"
    ))

    # Group relationships
    resolver.add_schema(RelationshipSchema(
        "5GROUP", "5GRASG", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("GROUPID", "group_id"),
        "Group has many assignments"
    ))

    # Shift relationships
    resolver.add_schema(RelationshipSchema(
        "5SHIFT", "5SPSHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("SHIFTID", "shift_id"),
        "Shift used in shift details"
    ))

    resolver.add_schema(RelationshipSchema(
        "5SHIFT", "5MASHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("SHIFTID", "shift_id"),
        "Shift used in assignments"
    ))

    # Workplace relationships
    resolver.add_schema(RelationshipSchema(
        "5WOPL", "5SPSHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("WORKPLACID", "workplace_id"),
        "Workplace used in shift details"
    ))

    resolver.add_schema(RelationshipSchema(
        "5WOPL", "5MASHI", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("WORKPLACID", "workplace_id"),
        "Workplace used in assignments"
    ))

    # Leave type relationships
    resolver.add_schema(RelationshipSchema(
        "5LEAVT", "5ABSEN", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("LEAVETYPID", "leave_type_id"),
        "Leave type used in absences"
    ))

    resolver.add_schema(RelationshipSchema(
        "5LEAVT", "5LEAEN", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("LEAVETYPID", "leave_type_id"),
        "Leave type used in entitlements"
    ))

    # Cycle relationships
    resolver.add_schema(RelationshipSchema(
        "5CYCLE", "5CYASS", RelationType.ONE_TO_MANY,
        FieldMapping("ID", "id"), FieldMapping("CYCLEID", "cycle_id"),
        "Cycle has assignments"
    ))

    return resolver


# Global improved resolver instance
improved_relationship_manager = create_default_resolver()


# Compatibility layer
class RelationshipManagerCompat:
    """Compatibility wrapper for the original relationship_manager interface."""

    def __init__(self, resolver: RelationshipResolver):
        self.resolver = resolver
        self.relationships = resolver.schemas

    def resolve_reference(self, source_entities: List[Any], source_table: str,
                          target_table: str, target_entities: List[Any]) -> List[Tuple[Any, Any]]:
        """Compatibility method for resolve_reference."""
        # Build index for target entities if not already done
        if target_entities:
            sample = target_entities[0]
            # Find the appropriate field
            schemas = self.resolver._get_schemas_for_tables(source_table, target_table)
            if schemas:
                schema = schemas[0]
                self.resolver._indexes.build_index(
                    target_table, target_entities,
                    schema.target_field.model_attr,
                    schema.relationship_type
                )

        matches = []
        for source in source_entities:
            related = self.resolver.resolve_relationship(source, source_table, target_table)
            if related:
                if isinstance(related, list):
                    for target in related:
                        matches.append((source, target))
                else:
                    matches.append((source, related))

        return matches

    def get_all_related_tables(self, table: str) -> Set[str]:
        """Get all tables related to the given table."""
        related = set()

        # Outgoing relationships
        for schema in self.resolver._schema_index.get(table, []):
            related.add(schema.target_table)

        # Incoming relationships
        for schema in self.resolver.schemas:
            if schema.target_table == table:
                related.add(schema.source_table)

        return related

    def get_relationships_from(self, table: str) -> List[RelationshipSchema]:
        """Get relationships where table is source."""
        return self.resolver._schema_index.get(table, [])

    def get_relationships_to(self, table: str) -> List[RelationshipSchema]:
        """Get relationships where table is target."""
        return [s for s in self.resolver.schemas if s.target_table == table]

    def validate_relationships(self, loaded_tables: Dict[str, List[Any]]) -> List[str]:
        """Validate relationships."""
        return self.resolver.validate_relationships(loaded_tables)

    def get_relationship_graph(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        """Get relationship graph."""
        graph = defaultdict(lambda: defaultdict(dict))

        for schema in self.resolver.schemas:
            graph[schema.source_table][schema.target_table] = {
                "field": f"{schema.source_field.model_attr} -> {schema.target_field.model_attr}",
                "type": schema.relationship_type.value,
                "description": schema.description
            }

        return dict(graph)


# Create compatibility instance
relationship_manager = RelationshipManagerCompat(improved_relationship_manager)


def get_entity_with_relations(entity: Any, table_name: str,
                              loaded_tables: Dict[str, List[Any]],
                              max_depth: int = 1,
                              current_depth: int = 0) -> Dict[str, Any]:
    """Compatibility function using improved resolver."""
    relations = improved_relationship_manager.resolve_all_relationships(
        entity, table_name, loaded_tables, max_depth, current_depth
    )

    return {
        "_entity": entity,
        "_table": table_name,
        "_relations": relations
    }