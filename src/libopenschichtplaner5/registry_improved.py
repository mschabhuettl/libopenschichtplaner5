# libopenschichtplaner5/src/libopenschichtplaner5/registry_improved.py
"""
Improved registry implementation with plugin architecture and dependency resolution.
"""
from pathlib import Path
from typing import Callable, Dict, List, Union, Any, Optional, Type, Set
import logging
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from importlib import import_module
import json

from .utils.logging_config import get_logger
from .exceptions import DBFLoadError

logger = get_logger()


@dataclass
class TableDefinition:
    """Enhanced table definition with dependencies and metadata."""
    name: str
    model_class_path: str  # e.g., "libopenschichtplaner5.models.employee.Employee"
    loader_func_path: str  # e.g., "libopenschichtplaner5.models.employee.load_employees"
    description: str = ""
    primary_key: str = "id"
    required_fields: List[str] = field(default_factory=list)
    optional: bool = False
    dependencies: Set[str] = field(default_factory=set)  # Other tables this depends on
    version: str = "1.0"
    
    def __post_init__(self):
        if not self.required_fields:
            self.required_fields = [self.primary_key]


class TablePlugin(ABC):
    """Abstract base class for table plugins."""
    
    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the table name."""
        pass
    
    @property
    @abstractmethod
    def model_class(self) -> Type:
        """Return the model class."""
        pass
    
    @abstractmethod
    def load_data(self, path: Path) -> List[Any]:
        """Load data from DBF file."""
        pass
    
    @property
    def dependencies(self) -> Set[str]:
        """Return table dependencies."""
        return set()
    
    @property
    def is_optional(self) -> bool:
        """Whether this table is optional."""
        return False


class PluginRegistry:
    """Registry for table plugins with dependency resolution."""
    
    def __init__(self):
        self.plugins: Dict[str, TablePlugin] = {}
        self.definitions: Dict[str, TableDefinition] = {}
        self.load_order: List[str] = []
        self._loaded: Set[str] = set()
    
    def register_plugin(self, plugin: TablePlugin):
        """Register a table plugin."""
        self.plugins[plugin.table_name] = plugin
        logger.debug(f"Registered plugin for {plugin.table_name}")
    
    def register_from_definition(self, definition: TableDefinition):
        """Register a table from definition."""
        self.definitions[definition.name] = definition
        logger.debug(f"Registered definition for {definition.name}")
    
    def _resolve_dependencies(self) -> List[str]:
        """Resolve loading order based on dependencies."""
        # Simple topological sort
        visited = set()
        temp_mark = set()
        order = []
        
        def visit(table_name: str):
            if table_name in temp_mark:
                raise ValueError(f"Circular dependency detected involving {table_name}")
            if table_name in visited:
                return
                
            temp_mark.add(table_name)
            
            # Get dependencies
            deps = set()
            if table_name in self.plugins:
                deps = self.plugins[table_name].dependencies
            elif table_name in self.definitions:
                deps = self.definitions[table_name].dependencies
            
            for dep in deps:
                if dep in self.plugins or dep in self.definitions:
                    visit(dep)
            
            temp_mark.remove(table_name)
            visited.add(table_name)
            order.append(table_name)
        
        # Visit all tables
        all_tables = set(self.plugins.keys()) | set(self.definitions.keys())
        for table in all_tables:
            if table not in visited:
                visit(table)
        
        return order
    
    def load_all_tables(self, dbf_dir: Path) -> Dict[str, List[Any]]:
        """Load all tables in dependency order."""
        try:
            self.load_order = self._resolve_dependencies()
        except ValueError as e:
            logger.error(f"Dependency resolution failed: {e}")
            raise
        
        loaded_tables = {}
        
        for table_name in self.load_order:
            try:
                if table_name in self.plugins:
                    # Use plugin
                    plugin = self.plugins[table_name]
                    dbf_path = dbf_dir / f"{table_name}.DBF"
                    
                    if not dbf_path.exists():
                        if plugin.is_optional:
                            logger.info(f"Optional table {table_name} not found, skipping")
                            loaded_tables[table_name] = []
                            continue
                        else:
                            raise FileNotFoundError(f"Required table {table_name} not found at {dbf_path}")
                    
                    records = plugin.load_data(dbf_path)
                    loaded_tables[table_name] = records
                    
                elif table_name in self.definitions:
                    # Use definition
                    definition = self.definitions[table_name]
                    loaded_tables[table_name] = self._load_from_definition(definition, dbf_dir)
                
                self._loaded.add(table_name)
                logger.info(f"Loaded {table_name}: {len(loaded_tables[table_name])} records")
                
            except Exception as e:
                is_optional = (
                    (table_name in self.plugins and self.plugins[table_name].is_optional) or
                    (table_name in self.definitions and self.definitions[table_name].optional)
                )
                
                if is_optional:
                    logger.warning(f"Failed to load optional table {table_name}: {e}")
                    loaded_tables[table_name] = []
                else:
                    logger.error(f"Failed to load required table {table_name}: {e}")
                    raise DBFLoadError(f"Failed to load required table {table_name}") from e
        
        return loaded_tables
    
    def _load_from_definition(self, definition: TableDefinition, dbf_dir: Path) -> List[Any]:
        """Load table from definition."""
        dbf_path = dbf_dir / f"{definition.name}.DBF"
        
        if not dbf_path.exists():
            if definition.optional:
                return []
            raise FileNotFoundError(f"Required table {definition.name} not found")
        
        # Dynamically import loader function
        module_path, func_name = definition.loader_func_path.rsplit(".", 1)
        module = import_module(module_path)
        loader_func = getattr(module, func_name)
        
        return loader_func(dbf_path)
    
    def get_load_order(self) -> List[str]:
        """Get the computed load order."""
        return self.load_order.copy()
    
    def is_loaded(self, table_name: str) -> bool:
        """Check if a table has been loaded."""
        return table_name in self._loaded


# Enhanced registry instance
enhanced_registry = PluginRegistry()


def register_table_definitions_from_json(json_path: Path):
    """Register table definitions from JSON configuration."""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for table_data in data.get('tables', []):
        definition = TableDefinition(
            name=table_data['name'],
            model_class_path=table_data['model_class_path'],
            loader_func_path=table_data['loader_func_path'],
            description=table_data.get('description', ''),
            primary_key=table_data.get('primary_key', 'id'),
            required_fields=table_data.get('required_fields', []),
            optional=table_data.get('optional', False),
            dependencies=set(table_data.get('dependencies', [])),
            version=table_data.get('version', '1.0')
        )
        enhanced_registry.register_from_definition(definition)


# Backward compatibility function
def load_table_enhanced(name: str, path: Path) -> List[Any]:
    """Enhanced table loading with better error handling."""
    if name not in enhanced_registry.plugins and name not in enhanced_registry.definitions:
        # Fallback to original registry
        from .registry import load_table as original_load_table
        return original_load_table(name, path)
    
    # Use enhanced registry for single table
    dbf_dir = path.parent
    tables = enhanced_registry.load_all_tables(dbf_dir)
    return tables.get(name, [])