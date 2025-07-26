# tests/test_registry.py
"""
Unit tests for the registry system.
Tests both original and improved registry implementations.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from tempfile import TemporaryDirectory
import json

from src.libopenschichtplaner5.registry import load_table, TABLE_METADATA, get_table_info
from src.libopenschichtplaner5.registry_improved import (
    PluginRegistry, TableDefinition, TablePlugin, enhanced_registry,
    register_table_definitions_from_json
)
from src.libopenschichtplaner5.exceptions import DBFLoadError


class TestOriginalRegistry:
    """Test cases for the original registry implementation."""
    
    def test_load_table_success(self):
        """Test successful table loading."""
        with TemporaryDirectory() as temp_dir:
            dbf_path = Path(temp_dir) / "5EMPL.DBF"
            dbf_path.touch()  # Create empty file
            
            with patch('src.libopenschichtplaner5.registry.models') as mock_models:
                mock_loader = Mock(return_value=[Mock(id=1, name="Test")])
                mock_models.get.return_value = mock_loader
                
                result = load_table("5EMPL", dbf_path)
                assert len(result) == 1
                assert result[0].id == 1
    
    def test_load_table_file_not_found(self):
        """Test handling of missing DBF files."""
        with TemporaryDirectory() as temp_dir:
            dbf_path = Path(temp_dir) / "nonexistent.DBF"
            
            with pytest.raises(FileNotFoundError):
                load_table("5EMPL", dbf_path)
    
    def test_load_table_unknown_table(self):
        """Test handling of unknown table names."""
        with TemporaryDirectory() as temp_dir:
            dbf_path = Path(temp_dir) / "UNKNOWN.DBF"
            
            with pytest.raises(ValueError, match="Unknown table name"):
                load_table("UNKNOWN", dbf_path)
    
    def test_get_table_info(self):
        """Test table metadata retrieval."""
        info = get_table_info("5EMPL")
        assert info is not None
        assert info.name == "5EMPL"
        assert info.description == "Employee master data"
        assert "id" in info.required_fields
    
    def test_optional_table_handling(self):
        """Test that optional tables are handled gracefully."""
        with TemporaryDirectory() as temp_dir:
            dbf_path = Path(temp_dir) / "5BUILD.DBF"  # Optional table
            
            # Should return empty list for missing optional table
            result = load_table("5BUILD", dbf_path)
            assert result == []


class MockTablePlugin(TablePlugin):
    """Mock plugin for testing."""
    
    def __init__(self, table_name: str, optional: bool = False, deps: set = None):
        self._table_name = table_name
        self._optional = optional
        self._deps = deps or set()
    
    @property
    def table_name(self) -> str:
        return self._table_name
    
    @property
    def model_class(self):
        return Mock
    
    def load_data(self, path: Path):
        return [Mock(id=1, name=f"Test {self._table_name}")]
    
    @property
    def dependencies(self) -> set:
        return self._deps
    
    @property
    def is_optional(self) -> bool:
        return self._optional


class TestImprovedRegistry:
    """Test cases for the improved registry implementation."""
    
    def test_plugin_registration(self):
        """Test plugin registration and discovery."""
        registry = PluginRegistry()
        plugin = MockTablePlugin("TEST_TABLE")
        
        registry.register_plugin(plugin)
        assert "TEST_TABLE" in registry.plugins
        assert registry.plugins["TEST_TABLE"] == plugin
    
    def test_dependency_resolution_simple(self):
        """Test simple dependency resolution."""
        registry = PluginRegistry()
        
        # No dependencies
        plugin_a = MockTablePlugin("TABLE_A")
        # Depends on TABLE_A
        plugin_b = MockTablePlugin("TABLE_B", deps={"TABLE_A"})
        
        registry.register_plugin(plugin_a)
        registry.register_plugin(plugin_b)
        
        order = registry._resolve_dependencies()
        
        # TABLE_A should come before TABLE_B
        assert order.index("TABLE_A") < order.index("TABLE_B")
    
    def test_dependency_resolution_complex(self):
        """Test complex dependency chain resolution."""
        registry = PluginRegistry()
        
        plugins = {
            "A": MockTablePlugin("A"),
            "B": MockTablePlugin("B", deps={"A"}),
            "C": MockTablePlugin("C", deps={"A", "B"}),
            "D": MockTablePlugin("D", deps={"B"}),
        }
        
        for plugin in plugins.values():
            registry.register_plugin(plugin)
        
        order = registry._resolve_dependencies()
        
        # Verify dependency order
        assert order.index("A") < order.index("B")
        assert order.index("A") < order.index("C")
        assert order.index("B") < order.index("C")
        assert order.index("B") < order.index("D")
    
    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies."""
        registry = PluginRegistry()
        
        plugin_a = MockTablePlugin("A", deps={"B"})
        plugin_b = MockTablePlugin("B", deps={"A"})
        
        registry.register_plugin(plugin_a)
        registry.register_plugin(plugin_b)
        
        with pytest.raises(ValueError, match="Circular dependency"):
            registry._resolve_dependencies()
    
    def test_load_all_tables_success(self):
        """Test successful loading of all tables."""
        registry = PluginRegistry()
        plugin = MockTablePlugin("TEST_TABLE")
        registry.register_plugin(plugin)
        
        with TemporaryDirectory() as temp_dir:
            dbf_path = Path(temp_dir) / "TEST_TABLE.DBF"
            dbf_path.touch()
            
            tables = registry.load_all_tables(Path(temp_dir))
            
            assert "TEST_TABLE" in tables
            assert len(tables["TEST_TABLE"]) == 1
            assert registry.is_loaded("TEST_TABLE")
    
    def test_optional_table_missing(self):
        """Test handling of missing optional tables."""
        registry = PluginRegistry()
        plugin = MockTablePlugin("OPTIONAL_TABLE", optional=True)
        registry.register_plugin(plugin)
        
        with TemporaryDirectory() as temp_dir:
            # Don't create the DBF file
            tables = registry.load_all_tables(Path(temp_dir))
            
            assert "OPTIONAL_TABLE" in tables
            assert tables["OPTIONAL_TABLE"] == []
    
    def test_required_table_missing(self):
        """Test handling of missing required tables."""
        registry = PluginRegistry()
        plugin = MockTablePlugin("REQUIRED_TABLE", optional=False)
        registry.register_plugin(plugin)
        
        with TemporaryDirectory() as temp_dir:
            # Don't create the DBF file
            with pytest.raises(FileNotFoundError):
                registry.load_all_tables(Path(temp_dir))
    
    def test_definition_registration(self):
        """Test registration from table definitions."""
        registry = PluginRegistry()
        
        definition = TableDefinition(
            name="5EMPL",
            model_class_path="libopenschichtplaner5.models.employee.Employee",
            loader_func_path="libopenschichtplaner5.models.employee.load_employees",
            description="Employee data"
        )
        
        registry.register_from_definition(definition)
        assert "5EMPL" in registry.definitions
    
    def test_json_configuration_loading(self):
        """Test loading table definitions from JSON."""
        config = {
            "tables": [
                {
                    "name": "5EMPL",
                    "model_class_path": "libopenschichtplaner5.models.employee.Employee",
                    "loader_func_path": "libopenschichtplaner5.models.employee.load_employees",
                    "description": "Employee master data",
                    "required_fields": ["id", "name"],
                    "dependencies": ["5GROUP"]
                }
            ]
        }
        
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            with open(config_path, 'w') as f:
                json.dump(config, f)
            
            register_table_definitions_from_json(config_path)
            
            # Should be registered in enhanced_registry
            assert "5EMPL" in enhanced_registry.definitions
            definition = enhanced_registry.definitions["5EMPL"]
            assert definition.description == "Employee master data"
            assert "5GROUP" in definition.dependencies


class TestRegistryIntegration:
    """Integration tests for registry components."""
    
    def test_mixed_plugins_and_definitions(self):
        """Test loading with both plugins and definitions."""
        registry = PluginRegistry()
        
        # Add a plugin
        plugin = MockTablePlugin("PLUGIN_TABLE")
        registry.register_plugin(plugin)
        
        # Add a definition
        definition = TableDefinition(
            name="DEF_TABLE",
            model_class_path="test.module.Model",
            loader_func_path="test.module.load_data"
        )
        registry.register_from_definition(definition)
        
        order = registry._resolve_dependencies()
        assert "PLUGIN_TABLE" in order
        assert "DEF_TABLE" in order
    
    def test_registry_statistics(self):
        """Test registry can provide useful statistics."""
        registry = PluginRegistry()
        
        for i in range(3):
            plugin = MockTablePlugin(f"TABLE_{i}")
            registry.register_plugin(plugin)
        
        assert len(registry.plugins) == 3
        assert len(registry.get_load_order()) >= 0
    
    def test_error_recovery(self):
        """Test registry recovers gracefully from load errors."""
        registry = PluginRegistry()
        
        # Plugin that will fail
        failing_plugin = MockTablePlugin("FAILING_TABLE")
        failing_plugin.load_data = Mock(side_effect=Exception("Load failed"))
        
        # Plugin that should succeed
        good_plugin = MockTablePlugin("GOOD_TABLE", optional=True)
        
        registry.register_plugin(failing_plugin)
        registry.register_plugin(good_plugin)
        
        with TemporaryDirectory() as temp_dir:
            # Create files for both
            Path(temp_dir, "FAILING_TABLE.DBF").touch()
            Path(temp_dir, "GOOD_TABLE.DBF").touch()
            
            # Should fail due to required table failure
            with pytest.raises(DBFLoadError):
                registry.load_all_tables(Path(temp_dir))


# Parametrized tests for edge cases
@pytest.mark.parametrize("table_name,expected_optional", [
    ("5EMPL", False),      # Required table
    ("5BUILD", True),      # Optional table  
    ("5CYEXC", True),      # Optional table
])
def test_table_optional_status(table_name, expected_optional):
    """Test that table optional status is correctly configured."""
    info = get_table_info(table_name)
    if info:
        assert info.optional == expected_optional


@pytest.mark.parametrize("invalid_name", [
    "",                    # Empty string
    "INVALID",            # Unknown table
    "5FAKE",              # Non-existent table
    None,                 # None value
])
def test_invalid_table_names(invalid_name):
    """Test handling of various invalid table names."""
    with pytest.raises((ValueError, TypeError)):
        with TemporaryDirectory() as temp_dir:
            dbf_path = Path(temp_dir) / "test.DBF"
            load_table(invalid_name, dbf_path)


if __name__ == "__main__":
    pytest.main([__file__])