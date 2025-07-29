# src/libopenschichtplaner5/registry_improved.py
"""
Improved registry system with plugin architecture and dependency resolution.
Replaces the original registry.py with a more robust implementation.
"""

import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Type, Set, Tuple, Callable
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import importlib
import json
from collections import defaultdict, OrderedDict

from .exceptions import DBFLoadError, SchichtplanerError
from .db.reader import DBFTable

logger = logging.getLogger(__name__)


@dataclass
class TableDefinition:
    """Enhanced table definition with metadata."""
    name: str
    model_class_path: str
    loader_func_path: str
    description: str = ""
    required_fields: List[str] = field(default_factory=lambda: ["id"])
    optional: bool = False
    dependencies: Set[str] = field(default_factory=set)
    category: str = "general"

    def __post_init__(self):
        # Validate paths
        if not self.model_class_path or not self.loader_func_path:
            raise ValueError(f"Invalid paths for table {self.name}")


class TablePlugin(ABC):
    """Abstract base class for table plugins."""

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the table name this plugin handles."""
        pass

    @property
    @abstractmethod
    def model_class(self) -> Type:
        """Return the model class for this table."""
        pass

    @abstractmethod
    def load_data(self, path: Path) -> List[Any]:
        """Load data from the given path."""
        pass

    @property
    def dependencies(self) -> Set[str]:
        """Return set of table names this table depends on."""
        return set()

    @property
    def is_optional(self) -> bool:
        """Whether this table is optional."""
        return False

    def validate_data(self, data: List[Any]) -> List[str]:
        """Validate loaded data. Return list of errors."""
        return []


class DynamicTablePlugin(TablePlugin):
    """Dynamic plugin created from TableDefinition."""

    def __init__(self, definition: TableDefinition):
        self.definition = definition
        self._model_class = None
        self._loader_func = None
        self._load_imports()

    def _load_imports(self):
        """Dynamically load model class and loader function."""
        try:
            # Load model class
            module_path, class_name = self.definition.model_class_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            self._model_class = getattr(module, class_name)

            # Load loader function
            module_path, func_name = self.definition.loader_func_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            self._loader_func = getattr(module, func_name)

        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to load plugin for {self.definition.name}: {e}")
            raise

    @property
    def table_name(self) -> str:
        return self.definition.name

    @property
    def model_class(self) -> Type:
        return self._model_class

    def load_data(self, path: Path) -> List[Any]:
        """Load data using the configured loader function."""
        if not path.exists():
            if self.is_optional:
                logger.info(f"Optional table {self.table_name} not found at {path}")
                return []
            raise FileNotFoundError(f"Required table {self.table_name} not found at {path}")

        try:
            return self._loader_func(path)
        except Exception as e:
            if self.is_optional:
                logger.warning(f"Failed to load optional table {self.table_name}: {e}")
                return []
            raise DBFLoadError(f"Failed to load {self.table_name}: {e}")

    @property
    def dependencies(self) -> Set[str]:
        return self.definition.dependencies

    @property
    def is_optional(self) -> bool:
        return self.definition.optional


class PluginRegistry:
    """Enhanced registry with plugin support and dependency resolution."""

    def __init__(self):
        self.plugins: Dict[str, TablePlugin] = {}
        self.definitions: Dict[str, TableDefinition] = {}
        self.loaded_tables: Dict[str, List[Any]] = {}
        self.load_errors: Dict[str, str] = {}
        self.metadata: Dict[str, Dict[str, Any]] = {}

    def register_plugin(self, plugin: TablePlugin):
        """Register a table plugin."""
        self.plugins[plugin.table_name] = plugin
        logger.debug(f"Registered plugin for table: {plugin.table_name}")

    def register_from_definition(self, definition: TableDefinition):
        """Create and register a plugin from a table definition."""
        self.definitions[definition.name] = definition
        try:
            plugin = DynamicTablePlugin(definition)
            self.register_plugin(plugin)
        except Exception as e:
            logger.error(f"Failed to create plugin from definition {definition.name}: {e}")
            self.load_errors[definition.name] = str(e)

    def _resolve_dependencies(self) -> List[str]:
        """Resolve table loading order based on dependencies."""
        # Build dependency graph
        graph = defaultdict(set)
        in_degree = defaultdict(int)

        all_tables = set(self.plugins.keys())

        for table_name, plugin in self.plugins.items():
            deps = plugin.dependencies
            graph[table_name] = deps
            for dep in deps:
                if dep in all_tables:
                    in_degree[dep] += 0  # Initialize
                    in_degree[table_name] += 1

        # Topological sort using Kahn's algorithm
        queue = [table for table in all_tables if in_degree[table] == 0]
        result = []

        while queue:
            table = queue.pop(0)
            result.append(table)

            # Check all tables that depend on this one
            for other_table in all_tables:
                if table in graph[other_table]:
                    in_degree[other_table] -= 1
                    if in_degree[other_table] == 0:
                        queue.append(other_table)

        if len(result) != len(all_tables):
            # Circular dependency detected
            remaining = all_tables - set(result)
            raise ValueError(f"Circular dependency detected involving tables: {remaining}")

        return result

    def load_all_tables(self, dbf_dir: Path,
                        extensions: List[str] = None) -> Dict[str, List[Any]]:
        """Load all registered tables in dependency order."""
        if extensions is None:
            extensions = [".DBF", ".dbf", ".txt", ".TXT"]

        self.loaded_tables.clear()
        self.load_errors.clear()

        # Resolve loading order
        try:
            load_order = self._resolve_dependencies()
        except ValueError as e:
            logger.error(f"Dependency resolution failed: {e}")
            raise

        logger.info(f"Loading tables in order: {load_order}")

        # Load tables
        for table_name in load_order:
            plugin = self.plugins[table_name]

            # Try different file extensions
            loaded = False
            for ext in extensions:
                file_path = dbf_dir / f"{table_name}{ext}"
                if file_path.exists():
                    try:
                        logger.info(f"Loading {table_name} from {file_path}")
                        data = plugin.load_data(file_path)
                        self.loaded_tables[table_name] = data

                        # Store metadata
                        self.metadata[table_name] = {
                            "path": str(file_path),
                            "count": len(data),
                            "optional": plugin.is_optional,
                            "model_class": plugin.model_class.__name__ if plugin.model_class else None
                        }

                        # Validate data
                        errors = plugin.validate_data(data)
                        if errors:
                            logger.warning(f"Validation errors for {table_name}: {errors}")

                        loaded = True
                        logger.info(f"Loaded {table_name}: {len(data)} records")
                        break

                    except Exception as e:
                        error_msg = f"Error loading {table_name}: {e}"
                        logger.error(error_msg)
                        self.load_errors[table_name] = str(e)

                        if not plugin.is_optional:
                            raise DBFLoadError(error_msg)

            if not loaded:
                if plugin.is_optional:
                    logger.info(f"Optional table {table_name} not found")
                    self.loaded_tables[table_name] = []
                else:
                    raise FileNotFoundError(f"Required table {table_name} not found in {dbf_dir}")

        logger.info(f"Successfully loaded {len(self.loaded_tables)} tables")
        return self.loaded_tables

    def get_table(self, name: str) -> Optional[List[Any]]:
        """Get loaded table data."""
        return self.loaded_tables.get(name)

    def is_loaded(self, name: str) -> bool:
        """Check if a table is loaded."""
        return name in self.loaded_tables

    def get_statistics(self) -> Dict[str, Any]:
        """Get registry statistics."""
        return {
            "registered_plugins": len(self.plugins),
            "loaded_tables": len(self.loaded_tables),
            "total_records": sum(len(data) for data in self.loaded_tables.values()),
            "load_errors": len(self.load_errors),
            "optional_tables": sum(1 for p in self.plugins.values() if p.is_optional),
            "metadata": self.metadata
        }

    def get_load_order(self) -> List[str]:
        """Get the resolved loading order."""
        try:
            return self._resolve_dependencies()
        except ValueError:
            return list(self.plugins.keys())  # Fallback to unordered


# Create global enhanced registry instance
enhanced_registry = PluginRegistry()


def register_standard_tables():
    """Register all standard Schichtplaner5 tables."""
    standard_tables = [
        TableDefinition(
            name="5EMPL",
            model_class_path="libopenschichtplaner5.models.employee.Employee",
            loader_func_path="libopenschichtplaner5.models.employee.load_employees",
            description="Employee master data",
            required_fields=["id", "name"],
            dependencies=set()
        ),
        TableDefinition(
            name="5GROUP",
            model_class_path="libopenschichtplaner5.models.group.Group",
            loader_func_path="libopenschichtplaner5.models.group.load_groups",
            description="Groups/Departments",
            dependencies=set()
        ),
        TableDefinition(
            name="5SHIFT",
            model_class_path="libopenschichtplaner5.models.shift.Shift",
            loader_func_path="libopenschichtplaner5.models.shift.load_shifts",
            description="Shift definitions",
            dependencies=set()
        ),
        TableDefinition(
            name="5WOPL",
            model_class_path="libopenschichtplaner5.models.work_location.WorkLocation",
            loader_func_path="libopenschichtplaner5.models.work_location.load_work_locations",
            description="Work locations",
            dependencies=set()
        ),
        TableDefinition(
            name="5ABSEN",
            model_class_path="libopenschichtplaner5.models.absence.Absence",
            loader_func_path="libopenschichtplaner5.models.absence.load_absences",
            description="Employee absences",
            dependencies={"5EMPL", "5LEAVT"}
        ),
        TableDefinition(
            name="5LEAVT",
            model_class_path="libopenschichtplaner5.models.leave_type.LeaveType",
            loader_func_path="libopenschichtplaner5.models.leave_type.load_leave_types",
            description="Leave types",
            dependencies=set()
        ),
        TableDefinition(
            name="5SPSHI",
            model_class_path="libopenschichtplaner5.models.shift_detail.ShiftDetail",
            loader_func_path="libopenschichtplaner5.models.shift_detail.load_shift_details",
            description="Shift details",
            dependencies={"5EMPL", "5SHIFT", "5WOPL"}
        ),
        TableDefinition(
            name="5MASHI",
            model_class_path="libopenschichtplaner5.models.employee_shift.EmployeeShift",
            loader_func_path="libopenschichtplaner5.models.employee_shift.load_employee_shifts",
            description="Employee shifts (legacy)",
            dependencies={"5EMPL", "5SHIFT", "5WOPL"},
            optional=True
        ),
        TableDefinition(
            name="5NOTE",
            model_class_path="libopenschichtplaner5.models.note.Note",
            loader_func_path="libopenschichtplaner5.models.note.load_notes",
            description="Notes",
            dependencies={"5EMPL"}
        ),
        TableDefinition(
            name="5GRASG",
            model_class_path="libopenschichtplaner5.models.group_assignment.GroupAssignment",
            loader_func_path="libopenschichtplaner5.models.group_assignment.load_group_assignments",
            description="Group assignments",
            dependencies={"5EMPL", "5GROUP"}
        ),
        TableDefinition(
            name="5LEAEN",
            model_class_path="libopenschichtplaner5.models.leave_entitlement.LeaveEntitlement",
            loader_func_path="libopenschichtplaner5.models.leave_entitlement.load_leave_entitlements",
            description="Leave entitlements",
            dependencies={"5EMPL", "5LEAVT"}
        ),
        TableDefinition(
            name="5CYCLE",
            model_class_path="libopenschichtplaner5.models.cycle.Cycle",
            loader_func_path="libopenschichtplaner5.models.cycle.load_cycles",
            description="Shift cycles",
            dependencies=set()
        ),
        TableDefinition(
            name="5CYASS",
            model_class_path="libopenschichtplaner5.models.cycle_assignment.CycleAssignment",
            loader_func_path="libopenschichtplaner5.models.cycle_assignment.load_cycle_assignments",
            description="Cycle assignments",
            dependencies={"5EMPL", "5CYCLE"}
        ),
        TableDefinition(
            name="5HOLID",
            model_class_path="libopenschichtplaner5.models.holiday.Holiday",
            loader_func_path="libopenschichtplaner5.models.holiday.load_holidays",
            description="Holidays",
            dependencies=set()
        ),
        TableDefinition(
            name="5USER",
            model_class_path="libopenschichtplaner5.models.user.User",
            loader_func_path="libopenschichtplaner5.models.user.load_users",
            description="System users",
            dependencies=set()
        ),
        TableDefinition(
            name="5BOOK",
            model_class_path="libopenschichtplaner5.models.book.Book",
            loader_func_path="libopenschichtplaner5.models.book.load_books",
            description="Bookings/Overtime",
            dependencies={"5EMPL"},
            optional=True
        ),
        TableDefinition(
            name="5BUILD",
            model_class_path="libopenschichtplaner5.models.build.Build",
            loader_func_path="libopenschichtplaner5.models.build.load_builds",
            description="Build information",
            dependencies=set(),
            optional=True
        ),
        TableDefinition(
            name="5XCHAR",
            model_class_path="libopenschichtplaner5.models.xchar.XChar",
            loader_func_path="libopenschichtplaner5.models.xchar.load_xchar",
            description="Extra characteristics/Surcharges",
            dependencies=set(),
            optional=True
        ),
        TableDefinition(
            name="5OVER",
            model_class_path="libopenschichtplaner5.models.overtime.Overtime",
            loader_func_path="libopenschichtplaner5.models.overtime.load_overtime",
            description="Overtime records",
            dependencies={"5EMPL"},
            optional=True
        ),
    ]

    # Register all standard tables
    for table_def in standard_tables:
        try:
            enhanced_registry.register_from_definition(table_def)
        except Exception as e:
            logger.error(f"Failed to register {table_def.name}: {e}")


def register_table_definitions_from_json(config_path: Path):
    """Load table definitions from JSON configuration."""
    with open(config_path, 'r') as f:
        config = json.load(f)

    for table_config in config.get('tables', []):
        definition = TableDefinition(
            name=table_config['name'],
            model_class_path=table_config['model_class_path'],
            loader_func_path=table_config['loader_func_path'],
            description=table_config.get('description', ''),
            required_fields=table_config.get('required_fields', ['id']),
            optional=table_config.get('optional', False),
            dependencies=set(table_config.get('dependencies', [])),
            category=table_config.get('category', 'general')
        )
        enhanced_registry.register_from_definition(definition)


# Initialize standard tables on import
register_standard_tables()


# Compatibility layer with original registry
def load_table(name: str, path: Path) -> List[Any]:
    """Compatibility function for original registry interface."""
    if name not in enhanced_registry.plugins:
        # Try to load it anyway if we have a definition
        if name in enhanced_registry.definitions:
            enhanced_registry.register_from_definition(enhanced_registry.definitions[name])
        else:
            raise ValueError(f"Unknown table: {name}")

    plugin = enhanced_registry.plugins.get(name)
    if plugin:
        return plugin.load_data(path)
    else:
        raise ValueError(f"No plugin registered for table: {name}")


def get_table_info(name: str) -> Optional[TableDefinition]:
    """Get table definition info."""
    return enhanced_registry.definitions.get(name)


# Export these for compatibility
TABLE_NAMES = list(enhanced_registry.plugins.keys())
TABLE_METADATA = enhanced_registry.definitions