# tests/test_relationships.py
"""
Unit tests for the relationship management system.
Tests both original and improved relationship implementations.
"""

import pytest
from unittest.mock import Mock
from dataclasses import dataclass
from typing import List

from src.libopenschichtplaner5.relationships import (
    relationship_manager, RelationType, TableRelationship, 
    get_entity_with_relations
)
from src.libopenschichtplaner5.relationships_improved import (
    RelationshipResolver, RelationshipSchema, FieldMapping,
    RelationshipIndex, create_default_resolver
)


@dataclass
class MockEmployee:
    """Mock employee model for testing."""
    id: int
    name: str
    group_id: int = None


@dataclass  
class MockAbsence:
    """Mock absence model for testing."""
    id: int
    employee_id: int
    date: str
    leave_type_id: int


@dataclass
class MockGroup:
    """Mock group model for testing."""
    id: int
    name: str


class TestOriginalRelationships:
    """Test cases for the original relationship system."""
    
    def test_relationship_resolution(self):
        """Test basic relationship resolution."""
        # Create test data
        employees = [MockEmployee(id=1, name="John")]
        absences = [MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1)]
        
        # Test resolution
        matches = relationship_manager.resolve_reference(
            employees, "5EMPL", "5ABSEN", absences
        )
        
        assert len(matches) == 1
        assert matches[0][0].id == 1  # Employee
        assert matches[0][1].id == 1  # Absence
        assert matches[0][1].employee_id == 1
    
    def test_no_matches(self):
        """Test when no relationships match."""
        employees = [MockEmployee(id=1, name="John")]
        absences = [MockAbsence(id=1, employee_id=999, date="2024-01-01", leave_type_id=1)]
        
        matches = relationship_manager.resolve_reference(
            employees, "5EMPL", "5ABSEN", absences
        )
        
        assert len(matches) == 0
    
    def test_one_to_many_relationship(self):
        """Test one-to-many relationships."""
        employees = [MockEmployee(id=1, name="John")]
        absences = [
            MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1),
            MockAbsence(id=2, employee_id=1, date="2024-01-02", leave_type_id=1),
            MockAbsence(id=3, employee_id=2, date="2024-01-01", leave_type_id=1)
        ]
        
        matches = relationship_manager.resolve_reference(
            employees, "5EMPL", "5ABSEN", absences
        )
        
        # Should find 2 matches for employee 1
        assert len(matches) == 2
        assert all(match[1].employee_id == 1 for match in matches)
    
    def test_get_related_tables(self):
        """Test getting all related tables."""
        related = relationship_manager.get_all_related_tables("5EMPL")
        
        # Employee should be related to many tables
        assert "5ABSEN" in related
        assert "5SPSHI" in related
        assert "5MASHI" in related
        assert "5GRASG" in related
    
    def test_relationship_validation(self):
        """Test relationship validation."""
        loaded_tables = {
            "5EMPL": [MockEmployee(id=1, name="John")],
            "5ABSEN": [MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1)]
        }
        
        errors = relationship_manager.validate_relationships(loaded_tables)
        
        # Should have no validation errors for valid data
        assert len(errors) == 0
    
    def test_entity_enrichment(self):
        """Test entity enrichment with relationships."""
        loaded_tables = {
            "5EMPL": [MockEmployee(id=1, name="John")],
            "5ABSEN": [MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1)]
        }
        
        employee = loaded_tables["5EMPL"][0]
        enriched = get_entity_with_relations(
            employee, "5EMPL", loaded_tables, max_depth=1
        )
        
        assert "_entity" in enriched
        assert "_relations" in enriched
        assert enriched["_entity"] == employee


class TestImprovedRelationships:
    """Test cases for the improved relationship system."""
    
    def test_schema_creation(self):
        """Test relationship schema creation."""
        schema = RelationshipSchema(
            source_table="5EMPL",
            target_table="5ABSEN", 
            relationship_type=RelationType.ONE_TO_MANY,
            source_field=FieldMapping("ID", "id"),
            target_field=FieldMapping("EMPLOYEEID", "employee_id"),
            description="Employee absences"
        )
        
        assert schema.source_table == "5EMPL"
        assert schema.target_table == "5ABSEN"
        assert schema.relationship_type == RelationType.ONE_TO_MANY
    
    def test_resolver_schema_registration(self):
        """Test schema registration in resolver."""
        resolver = RelationshipResolver()
        
        schema = RelationshipSchema(
            "5EMPL", "5ABSEN", RelationType.ONE_TO_MANY,
            FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id")
        )
        
        resolver.add_schema(schema)
        assert schema in resolver.schemas
        assert "5EMPL" in resolver._schema_index
    
    def test_relationship_index_building(self):
        """Test building relationship indexes."""
        index = RelationshipIndex()
        
        absences = [
            MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1),
            MockAbsence(id=2, employee_id=1, date="2024-01-02", leave_type_id=1),
            MockAbsence(id=3, employee_id=2, date="2024-01-01", leave_type_id=1)
        ]
        
        index.build_index("5ABSEN", absences, "employee_id", RelationType.ONE_TO_MANY)
        
        # Test lookup
        results = index.lookup("5ABSEN", 1, RelationType.ONE_TO_MANY)
        assert len(results) == 2
        assert all(abs.employee_id == 1 for abs in results)
    
    def test_one_to_one_indexing(self):
        """Test one-to-one relationship indexing."""
        index = RelationshipIndex()
        
        employees = [MockEmployee(id=1, name="John"), MockEmployee(id=2, name="Jane")]
        
        index.build_index("5EMPL", employees, "id", RelationType.ONE_TO_ONE)
        
        result = index.lookup("5EMPL", 1, RelationType.ONE_TO_ONE)
        assert result.id == 1
        assert result.name == "John"
    
    def test_relationship_resolution_with_caching(self):
        """Test relationship resolution with caching."""
        resolver = RelationshipResolver()
        
        schema = RelationshipSchema(
            "5EMPL", "5ABSEN", RelationType.ONE_TO_MANY,
            FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id")
        )
        resolver.add_schema(schema)
        
        # Build indexes
        loaded_tables = {
            "5EMPL": [MockEmployee(id=1, name="John")],
            "5ABSEN": [MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1)]
        }
        resolver.build_data_indexes(loaded_tables)
        
        employee = loaded_tables["5EMPL"][0]
        
        # First resolution (should cache)
        result1 = resolver.resolve_relationship(employee, "5EMPL", "5ABSEN")
        
        # Second resolution (should use cache)
        result2 = resolver.resolve_relationship(employee, "5EMPL", "5ABSEN")
        
        assert result1 == result2
        assert len(result1) == 1
        assert result1[0].employee_id == 1
    
    def test_recursive_relationship_resolution(self):
        """Test recursive resolution of relationships."""
        resolver = RelationshipResolver()
        
        # Add schemas
        emp_to_group = RelationshipSchema(
            "5EMPL", "5GROUP", RelationType.MANY_TO_ONE,
            FieldMapping("GROUPID", "group_id"), FieldMapping("ID", "id")
        )
        emp_to_absence = RelationshipSchema(
            "5EMPL", "5ABSEN", RelationType.ONE_TO_MANY,
            FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id")
        )
        
        resolver.add_schema(emp_to_group)
        resolver.add_schema(emp_to_absence)
        
        # Build test data
        loaded_tables = {
            "5EMPL": [MockEmployee(id=1, name="John", group_id=10)],
            "5GROUP": [MockGroup(id=10, name="IT Department")],
            "5ABSEN": [MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1)]
        }
        resolver.build_data_indexes(loaded_tables)
        
        employee = loaded_tables["5EMPL"][0]
        relations = resolver.resolve_all_relationships(employee, "5EMPL", max_depth=2)
        
        assert "5GROUP" in relations
        assert "5ABSEN" in relations
    
    def test_cache_management(self):
        """Test relationship cache management."""
        resolver = RelationshipResolver()
        
        schema = RelationshipSchema(
            "5EMPL", "5ABSEN", RelationType.ONE_TO_MANY,
            FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id")
        )
        resolver.add_schema(schema)
        
        loaded_tables = {
            "5EMPL": [MockEmployee(id=1, name="John")],
            "5ABSEN": [MockAbsence(id=1, employee_id=1, date="2024-01-01", leave_type_id=1)]
        }
        resolver.build_data_indexes(loaded_tables)
        
        employee = loaded_tables["5EMPL"][0]
        
        # Add to cache
        resolver.resolve_relationship(employee, "5EMPL", "5ABSEN")
        assert len(resolver._cache) > 0
        
        # Clear cache
        resolver.clear_cache()
        assert len(resolver._cache) == 0
    
    def test_default_resolver_creation(self):
        """Test creation of default resolver with standard relationships."""
        resolver = create_default_resolver()
        
        assert len(resolver.schemas) > 0
        
        # Should have employee relationships
        emp_schemas = resolver._schema_index.get("5EMPL", [])
        target_tables = {schema.target_table for schema in emp_schemas}
        
        assert "5ABSEN" in target_tables
        assert "5MASHI" in target_tables
        assert "5SPSHI" in target_tables
    
    def test_resolver_statistics(self):
        """Test resolver statistics generation."""
        resolver = RelationshipResolver()
        
        for i in range(5):
            schema = RelationshipSchema(
                f"TABLE_{i}", f"TARGET_{i}", RelationType.ONE_TO_MANY,
                FieldMapping("ID", "id"), FieldMapping("REF_ID", "ref_id")
            )
            resolver.add_schema(schema)
        
        stats = resolver.get_statistics()
        
        assert stats["schemas_count"] == 5
        assert "cached_relationships" in stats
        assert "memory_usage_estimate" in stats


class TestRelationshipPerformance:
    """Performance tests for relationship resolution."""
    
    def test_large_dataset_performance(self):
        """Test performance with larger datasets."""
        resolver = RelationshipResolver()
        
        schema = RelationshipSchema(
            "5EMPL", "5ABSEN", RelationType.ONE_TO_MANY,
            FieldMapping("ID", "id"), FieldMapping("EMPLOYEEID", "employee_id")
        )
        resolver.add_schema(schema)
        
        # Create larger test datasets
        employees = [MockEmployee(id=i, name=f"Employee_{i}") for i in range(1, 1001)]
        absences = []
        for emp_id in range(1, 1001):
            for abs_id in range(3):  # 3 absences per employee
                absences.append(MockAbsence(
                    id=len(absences) + 1,
                    employee_id=emp_id,
                    date=f"2024-01-{abs_id + 1:02d}",
                    leave_type_id=1
                ))
        
        loaded_tables = {"5EMPL": employees, "5ABSEN": absences}
        
        import time
        start_time = time.time()
        resolver.build_data_indexes(loaded_tables)
        index_time = time.time() - start_time
        
        # Index building should be reasonably fast
        assert index_time < 1.0  # Less than 1 second
        
        # Test resolution performance
        start_time = time.time()
        for employee in employees[:100]:  # Test first 100
            resolver.resolve_relationship(employee, "5EMPL", "5ABSEN")
        resolution_time = time.time() - start_time
        
        # Resolution should be fast with indexing
        assert resolution_time < 0.5  # Less than 0.5 seconds for 100 resolutions


@pytest.mark.parametrize("relationship_type,expected_behavior", [
    (RelationType.ONE_TO_ONE, "single_result"),
    (RelationType.ONE_TO_MANY, "list_result"),
    (RelationType.MANY_TO_ONE, "list_result"),
    (RelationType.MANY_TO_MANY, "list_result"),
])
def test_relationship_types(relationship_type, expected_behavior):
    """Test different relationship types behave correctly."""
    index = RelationshipIndex()
    
    test_data = [MockEmployee(id=1, name="Test"), MockEmployee(id=2, name="Test2")]
    index.build_index("TEST", test_data, "id", relationship_type)
    
    result = index.lookup("TEST", 1, relationship_type)
    
    if expected_behavior == "single_result":
        assert not isinstance(result, list)
        assert result.id == 1
    else:
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].id == 1


if __name__ == "__main__":
    pytest.main([__file__])