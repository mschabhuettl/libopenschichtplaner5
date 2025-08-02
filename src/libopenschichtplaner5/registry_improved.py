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
        """Dynamically load model class and loader function.
        
        This method uses reflection to load the model class and loader function
        from the paths specified in the table definition. This allows for
        dynamic registration of tables without hardcoding imports.
        """
        try:
            # Load model class using dynamic import
            # Split the path into module and class name (e.g., "models.employee.Employee" -> "models.employee", "Employee")
            module_path, class_name = self.definition.model_class_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            self._model_class = getattr(module, class_name)

            # Load loader function using dynamic import
            # Split the path into module and function name (e.g., "models.employee.load_employees" -> "models.employee", "load_employees")
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
        """Resolve table loading order based on dependencies using topological sorting.
        
        This implements Kahn's algorithm for topological sorting to determine the correct
        order to load tables based on their foreign key dependencies. For example,
        if table A depends on table B, then B must be loaded before A.
        
        Returns:
            List[str]: Table names in the correct loading order
            
        Raises:
            ValueError: If circular dependencies are detected
        """
        # Build dependency graph where each table maps to its dependencies
        graph = defaultdict(set)
        in_degree = defaultdict(int)  # Count of incoming dependencies for each table

        all_tables = set(self.plugins.keys())

        # Build the dependency graph and calculate in-degrees
        for table_name, plugin in self.plugins.items():
            deps = plugin.dependencies
            graph[table_name] = deps
            # For each dependency, increment the in-degree of the dependent table
            for dep in deps:
                if dep in all_tables:
                    in_degree[dep] += 0  # Initialize dependency table
                    in_degree[table_name] += 1  # Increment dependent table's in-degree

        # Topological sort using Kahn's algorithm
        # Start with tables that have no dependencies (in-degree = 0)
        queue = [table for table in all_tables if in_degree[table] == 0]
        result = []

        # Process tables in dependency order
        while queue:
            # Get a table with no remaining dependencies
            table = queue.pop(0)
            result.append(table)

            # Update in-degrees for tables that depend on the current table
            for other_table in all_tables:
                if table in graph[other_table]:
                    in_degree[other_table] -= 1
                    # If all dependencies are satisfied, add to queue
                    if in_degree[other_table] == 0:
                        queue.append(other_table)

        # Check for circular dependencies
        if len(result) != len(all_tables):
            # Some tables couldn't be processed - indicates circular dependency
            remaining = all_tables - set(result)
            raise ValueError(f"Circular dependency detected involving tables: {remaining}")

        return result

    def load_all_tables(self, dbf_dir: Path,
                        extensions: List[str] = None) -> Dict[str, List[Any]]:
        """Load all registered tables from DBF files in dependency order.
        
        This method loads all registered tables from the specified directory,
        respecting dependency relationships. Tables are loaded in the order
        determined by the dependency resolution algorithm.
        
        Args:
            dbf_dir: Directory containing the DBF files
            extensions: List of file extensions to try (default: [".DBF", ".dbf", ".txt", ".TXT"])
            
        Returns:
            Dict mapping table names to loaded data records
            
        Raises:
            DBFLoadError: If a required table fails to load
            FileNotFoundError: If a required table file is not found
        """
        # Default file extensions to try when loading tables
        if extensions is None:
            extensions = [".DBF", ".dbf", ".txt", ".TXT"]

        # Clear any previously loaded data
        self.loaded_tables.clear()
        self.load_errors.clear()

        # Determine the correct loading order based on dependencies
        try:
            load_order = self._resolve_dependencies()
        except ValueError as e:
            logger.error(f"Dependency resolution failed: {e}")
            raise

        logger.info(f"Loading tables in order: {load_order}")

        # Load each table in dependency order
        for table_name in load_order:
            plugin = self.plugins[table_name]

            # Try to find the table file with different extensions
            loaded = False
            for ext in extensions:
                file_path = dbf_dir / f"{table_name}{ext}"
                if file_path.exists():
                    try:
                        logger.info(f"Loading {table_name} from {file_path}")
                        # Use the plugin's loader to read the data
                        data = plugin.load_data(file_path)
                        self.loaded_tables[table_name] = data

                        # Store metadata for debugging and monitoring
                        self.metadata[table_name] = {
                            "path": str(file_path),
                            "count": len(data),
                            "optional": plugin.is_optional,
                            "model_class": plugin.model_class.__name__ if plugin.model_class else None
                        }

                        # Run data validation if implemented
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

                        # Only raise exception for required tables
                        if not plugin.is_optional:
                            raise DBFLoadError(error_msg)

            # Handle case where no file was found for this table
            if not loaded:
                if plugin.is_optional:
                    logger.info(f"Optional table {table_name} not found")
                    self.loaded_tables[table_name] = []  # Empty list for missing optional tables
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
        TableDefinition(
            name="5CYENT",
            model_class_path="libopenschichtplaner5.models.cycle_entitlement.CycleEntitlement",
            loader_func_path="libopenschichtplaner5.models.cycle_entitlement.load_cycle_entitlements",
            description="Cycle entitlements - maps cycles to shifts",
            dependencies={"5CYCLE", "5SHIFT"}
        ),
        TableDefinition(
            name="5CYEXC",
            model_class_path="libopenschichtplaner5.models.cycle_exception.CycleException",
            loader_func_path="libopenschichtplaner5.models.cycle_exception.load_cycle_exceptions",
            description="Cycle exceptions - handles scheduling exceptions",
            dependencies={"5CYCLE", "5SHIFT"},
            optional=True  # Currently empty
        ),
        TableDefinition(
            name="5EMACC",
            model_class_path="libopenschichtplaner5.models.employee_access.EmployeeAccess",
            loader_func_path="libopenschichtplaner5.models.employee_access.load_employee_access",
            description="Employee access control - individual permissions",
            dependencies={"5EMPL"}
        ),
        TableDefinition(
            name="5GRACC",
            model_class_path="libopenschichtplaner5.models.group_access.GroupAccess",
            loader_func_path="libopenschichtplaner5.models.group_access.load_group_access",
            description="Group access control - group-level permissions",
            dependencies={"5GROUP"}
        ),
        TableDefinition(
            name="5HOBAN",
            model_class_path="libopenschichtplaner5.models.holiday_assignment.HolidayAssignment",
            loader_func_path="libopenschichtplaner5.models.holiday_assignment.load_holiday_assignments",
            description="Holiday bans - leave restriction periods",
            dependencies={"5GROUP"}
        ),
        TableDefinition(
            name="5PERIO",
            model_class_path="libopenschichtplaner5.models.period.Period",
            loader_func_path="libopenschichtplaner5.models.period.load_periods",
            description="Special periods - holidays, training blocks",
            dependencies={"5GROUP"}
        ),
        TableDefinition(
            name="5RESTR",
            model_class_path="libopenschichtplaner5.models.shift_restriction.ShiftRestriction",
            loader_func_path="libopenschichtplaner5.models.shift_restriction.load_shift_restrictions",
            description="Shift restrictions - employee shift preferences/restrictions",
            dependencies={"5EMPL", "5SHIFT"}
        ),
        TableDefinition(
            name="5SHDEM",
            model_class_path="libopenschichtplaner5.models.shift_demand.ShiftDemand",
            loader_func_path="libopenschichtplaner5.models.shift_demand.load_shift_demands",
            description="Standard shift demands - weekly staffing requirements",
            dependencies={"5GROUP", "5SHIFT", "5WOPL"}
        ),
        TableDefinition(
            name="5SPDEM",
            model_class_path="libopenschichtplaner5.models.shift_plan_demand.ShiftPlanDemand",
            loader_func_path="libopenschichtplaner5.models.shift_plan_demand.load_shift_plan_demands",
            description="Special shift demands - date-specific staffing overrides",
            dependencies={"5GROUP", "5SHIFT", "5WOPL"}
        ),
        TableDefinition(
            name="5USETT",
            model_class_path="libopenschichtplaner5.models.user_setting.UserSetting",
            loader_func_path="libopenschichtplaner5.models.user_setting.load_user_settings",
            description="User settings - global system configuration",
            dependencies=set(),
            optional=True  # Singleton table
        ),
        TableDefinition(
            name="5DADEM",
            model_class_path="libopenschichtplaner5.models.shift_demand.ShiftDemand",  # Reuse existing model
            loader_func_path="libopenschichtplaner5.models.shift_demand.load_day_demands",
            description="Day demands - daily staffing requirements",
            dependencies={"5SHIFT"},
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