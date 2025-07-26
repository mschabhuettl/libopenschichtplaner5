# libopenschichtplaner5/src/libopenschichtplaner5/registry.py
"""
Improved registry for DBF table handlers with validation and metadata.
"""
from pathlib import Path
from typing import Callable, Dict, List, Union, Any, Optional, NamedTuple
import logging
from dataclasses import dataclass

# Import all models - with better error handling
from .utils.logging_config import get_logger

logger = get_logger()

# Import models with graceful error handling
IMPORT_ERRORS = {}


def safe_import(module_name: str, items: List[str]):
    """Safely import items from a module, tracking errors."""
    try:
        module = __import__(f"libopenschichtplaner5.models.{module_name}", fromlist=items)
        return {item: getattr(module, item) for item in items}
    except ImportError as e:
        IMPORT_ERRORS[module_name] = str(e)
        logger.warning(f"Could not import {module_name}: {e}")
        return {}


# Import all models
models = {}
models.update(safe_import("absence", ["Absence", "load_absences"]))
models.update(safe_import("book", ["Book", "load_books"]))
models.update(safe_import("build", ["Build", "load_builds"]))
models.update(safe_import("cycle", ["Cycle", "load_cycles"]))
models.update(safe_import("cycle_assignment", ["CycleAssignment", "load_cycle_assignments"]))
models.update(safe_import("cycle_entitlement", ["CycleEntitlement", "load_cycle_entitlements"]))
models.update(safe_import("cycle_exception", ["CycleException", "load_cycle_exceptions"]))
models.update(safe_import("employee", ["Employee", "load_employees"]))
models.update(safe_import("employee_access", ["EmployeeAccess", "load_employee_access"]))
models.update(safe_import("employee_shift", ["EmployeeShift", "load_employee_shifts"]))
models.update(safe_import("group", ["Group", "load_groups"]))
models.update(safe_import("group_access", ["GroupAccess", "load_group_access"]))
models.update(safe_import("group_assignment", ["GroupAssignment", "load_group_assignments"]))
models.update(safe_import("holiday", ["Holiday", "load_holidays"]))
models.update(safe_import("holiday_assignment", ["HolidayAssignment", "load_holiday_assignments"]))
models.update(safe_import("leave_entitlement", ["LeaveEntitlement", "load_leave_entitlements"]))
models.update(safe_import("leave_type", ["LeaveType", "load_leave_types"]))
models.update(safe_import("note", ["Note", "load_notes"]))
models.update(safe_import("overtime", ["Overtime", "load_overtime"]))
models.update(safe_import("period", ["Period", "load_periods"]))
models.update(safe_import("shift", ["Shift", "load_shifts"]))
models.update(safe_import("shift_demand", ["ShiftDemand", "load_shift_demands"]))
models.update(safe_import("shift_detail", ["ShiftDetail", "load_shift_details"]))
models.update(safe_import("shift_plan_demand", ["ShiftPlanDemand", "load_shift_plan_demands"]))
models.update(safe_import("shift_restriction", ["ShiftRestriction", "load_shift_restrictions"]))
models.update(safe_import("shift_schedule", ["ShiftSchedule", "load_shift_schedules"]))
models.update(safe_import("user", ["User", "load_users"]))
models.update(safe_import("user_setting", ["UserSetting", "load_user_settings"]))
models.update(safe_import("work_location", ["WorkLocation", "load_work_locations"]))
models.update(safe_import("xchar", ["XChar", "load_xchar"]))


# Fallback loader for missing models
def create_fallback_loader(table_name: str):
    """Create a fallback loader that returns empty list."""

    def fallback_loader(path: Path) -> List[Any]:
        logger.warning(f"Using fallback loader for {table_name} - no data will be loaded")
        return []

    return fallback_loader


@dataclass
class TableMetadata:
    """Metadata for a DBF table."""
    name: str
    description: str
    model_class: Optional[type]
    loader_func: Callable[[Path], List[Any]]
    primary_key: str = "id"
    required_fields: List[str] = None
    optional: bool = False

    def __post_init__(self):
        if self.required_fields is None:
            self.required_fields = [self.primary_key]


# Enhanced table registry with metadata
TABLE_METADATA: Dict[str, TableMetadata] = {
    "5ABSEN": TableMetadata(
        name="5ABSEN",
        description="Employee absences",
        model_class=models.get("Absence"),
        loader_func=models.get("load_absences", create_fallback_loader("5ABSEN")),
        required_fields=["id", "employee_id", "date", "leave_type_id"]
    ),
    "5BOOK": TableMetadata(
        name="5BOOK",
        description="Time bookings and overtime",
        model_class=models.get("Book"),
        loader_func=models.get("load_books", create_fallback_loader("5BOOK")),
        required_fields=["id", "employee_id", "date"]
    ),
    "5BUILD": TableMetadata(
        name="5BUILD",
        description="Software build information",
        model_class=models.get("Build"),
        loader_func=models.get("load_builds", create_fallback_loader("5BUILD")),
        optional=True
    ),
    "5CYASS": TableMetadata(
        name="5CYASS",
        description="Cycle assignments to employees",
        model_class=models.get("CycleAssignment"),
        loader_func=models.get("load_cycle_assignments", create_fallback_loader("5CYASS")),
        required_fields=["id", "employee_id", "cycle_id"]
    ),
    "5CYCLE": TableMetadata(
        name="5CYCLE",
        description="Cycle definitions",
        model_class=models.get("Cycle"),
        loader_func=models.get("load_cycles", create_fallback_loader("5CYCLE"))
    ),
    "5CYENT": TableMetadata(
        name="5CYENT",
        description="Cycle entitlements",
        model_class=models.get("CycleEntitlement"),
        loader_func=models.get("load_cycle_entitlements", create_fallback_loader("5CYENT"))
    ),
    "5CYEXC": TableMetadata(
        name="5CYEXC",
        description="Cycle exceptions",
        model_class=models.get("CycleException"),
        loader_func=models.get("load_cycle_exceptions", create_fallback_loader("5CYEXC")),
        optional=True
    ),
    "5DADEM": TableMetadata(
        name="5DADEM",
        description="Daily demand requirements",
        model_class=models.get("ShiftDemand"),
        loader_func=models.get("load_shift_demands", create_fallback_loader("5DADEM")),
        optional=True
    ),
    "5EMACC": TableMetadata(
        name="5EMACC",
        description="Employee access rights",
        model_class=models.get("EmployeeAccess"),
        loader_func=models.get("load_employee_access", create_fallback_loader("5EMACC")),
        optional=True
    ),
    "5EMPL": TableMetadata(
        name="5EMPL",
        description="Employee master data",
        model_class=models.get("Employee"),
        loader_func=models.get("load_employees", create_fallback_loader("5EMPL")),
        required_fields=["id", "name", "firstname"]
    ),
    "5GRACC": TableMetadata(
        name="5GRACC",
        description="Group access definitions",
        model_class=models.get("GroupAccess"),
        loader_func=models.get("load_group_access", create_fallback_loader("5GRACC")),
        optional=True
    ),
    "5GRASG": TableMetadata(
        name="5GRASG",
        description="Group assignments for employees",
        model_class=models.get("GroupAssignment"),
        loader_func=models.get("load_group_assignments", create_fallback_loader("5GRASG")),
        required_fields=["id", "employee_id", "group_id"]
    ),
    "5GROUP": TableMetadata(
        name="5GROUP",
        description="Groups and departments",
        model_class=models.get("Group"),
        loader_func=models.get("load_groups", create_fallback_loader("5GROUP"))
    ),
    "5HOBAN": TableMetadata(
        name="5HOBAN",
        description="Holiday assignments",
        model_class=models.get("HolidayAssignment"),
        loader_func=models.get("load_holiday_assignments", create_fallback_loader("5HOBAN")),
        optional=True
    ),
    "5HOLID": TableMetadata(
        name="5HOLID",
        description="Holiday definitions",
        model_class=models.get("Holiday"),
        loader_func=models.get("load_holidays", create_fallback_loader("5HOLID")),
        optional=True
    ),
    "5LEAEN": TableMetadata(
        name="5LEAEN",
        description="Leave entitlements",
        model_class=models.get("LeaveEntitlement"),
        loader_func=models.get("load_leave_entitlements", create_fallback_loader("5LEAEN")),
        required_fields=["id", "employee_id", "leave_type_id"]
    ),
    "5LEAVT": TableMetadata(
        name="5LEAVT",
        description="Leave types and absence categories",
        model_class=models.get("LeaveType"),
        loader_func=models.get("load_leave_types", create_fallback_loader("5LEAVT"))
    ),
    "5MASHI": TableMetadata(
        name="5MASHI",
        description="Employee shift assignments (Mitarbeiterschichten)",
        model_class=models.get("EmployeeShift"),
        loader_func=models.get("load_employee_shifts", create_fallback_loader("5MASHI")),
        required_fields=["id", "employee_id", "shift_id", "date"]
    ),
    "5NOTE": TableMetadata(
        name="5NOTE",
        description="Notes and comments",
        model_class=models.get("Note"),
        loader_func=models.get("load_notes", create_fallback_loader("5NOTE")),
        required_fields=["id", "employee_id", "date"]
    ),
    "5OVER": TableMetadata(
        name="5OVER",
        description="Overtime records",
        model_class=models.get("Overtime"),
        loader_func=models.get("load_overtime", create_fallback_loader("5OVER")),
        optional=True
    ),
    "5PERIO": TableMetadata(
        name="5PERIO",
        description="Time periods",
        model_class=models.get("Period"),
        loader_func=models.get("load_periods", create_fallback_loader("5PERIO")),
        optional=True
    ),
    "5RESTR": TableMetadata(
        name="5RESTR",
        description="Shift restrictions",
        model_class=models.get("ShiftRestriction"),
        loader_func=models.get("load_shift_restrictions", create_fallback_loader("5RESTR")),
        optional=True
    ),
    "5SHDEM": TableMetadata(
        name="5SHDEM",
        description="Shift demand schedules",
        model_class=models.get("ShiftSchedule"),
        loader_func=models.get("load_shift_schedules", create_fallback_loader("5SHDEM")),
        optional=True
    ),
    "5SHIFT": TableMetadata(
        name="5SHIFT",
        description="Shift definitions and schedules",
        model_class=models.get("Shift"),
        loader_func=models.get("load_shifts", create_fallback_loader("5SHIFT"))
    ),
    "5SPDEM": TableMetadata(
        name="5SPDEM",
        description="Shift plan demands",
        model_class=models.get("ShiftPlanDemand"),
        loader_func=models.get("load_shift_plan_demands", create_fallback_loader("5SPDEM")),
        optional=True
    ),
    "5SPSHI": TableMetadata(
        name="5SPSHI",
        description="Shift plan details (scheduled shifts)",
        model_class=models.get("ShiftDetail"),
        loader_func=models.get("load_shift_details", create_fallback_loader("5SPSHI")),
        required_fields=["id", "employee_id", "shift_id", "date"]
    ),
    "5USER": TableMetadata(
        name="5USER",
        description="System users",
        model_class=models.get("User"),
        loader_func=models.get("load_users", create_fallback_loader("5USER")),
        optional=True
    ),
    "5USETT": TableMetadata(
        name="5USETT",
        description="User settings",
        model_class=models.get("UserSetting"),
        loader_func=models.get("load_user_settings", create_fallback_loader("5USETT")),
        optional=True
    ),
    "5WOPL": TableMetadata(
        name="5WOPL",
        description="Work locations and places",
        model_class=models.get("WorkLocation"),
        loader_func=models.get("load_work_locations", create_fallback_loader("5WOPL"))
    ),
    "5XCHAR": TableMetadata(
        name="5XCHAR",
        description="Surcharge rules and characteristics",
        model_class=models.get("XChar"),
        loader_func=models.get("load_xchar", create_fallback_loader("5XCHAR")),
        optional=True
    ),
}

# Backward compatibility
TABLE_REGISTRY = {name: meta.loader_func for name, meta in TABLE_METADATA.items()}
TABLE_NAMES = list(TABLE_METADATA.keys())


def load_table(name: str, path: Path) -> List[Any]:
    """
    Load a DBF table with enhanced error handling and validation.

    Args:
        name: Table name (e.g. "5EMPL")
        path: Path to the DBF file

    Returns:
        List of loaded model instances

    Raises:
        ValueError: If table name is unknown
        FileNotFoundError: If DBF file doesn't exist
        RuntimeError: If loading fails
    """
    if name not in TABLE_METADATA:
        available = ", ".join(TABLE_NAMES)
        raise ValueError(f"Unknown table name: {name}. Available: {available}")

    metadata = TABLE_METADATA[name]

    if not path.exists():
        if metadata.optional:
            logger.info(f"Optional table {name} not found at {path}, skipping")
            return []
        else:
            raise FileNotFoundError(f"Required DBF file not found: {path}")

    try:
        logger.debug(f"Loading table {name} from {path}")
        records = metadata.loader_func(path)
        logger.info(f"Successfully loaded {len(records)} records from {name}")
        return records
    except Exception as e:
        error_msg = f"Error loading {name} from {path}: {e}"
        if metadata.optional:
            logger.warning(f"{error_msg} (optional table, continuing)")
            return []
        else:
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e


def get_table_info(name: str) -> Optional[TableMetadata]:
    """Get metadata information for a table."""
    return TABLE_METADATA.get(name)


def list_required_tables() -> List[str]:
    """Get list of required (non-optional) tables."""
    return [name for name, meta in TABLE_METADATA.items() if not meta.optional]


def list_optional_tables() -> List[str]:
    """Get list of optional tables."""
    return [name for name, meta in TABLE_METADATA.items() if meta.optional]


def validate_table_availability(dbf_dir: Path) -> Dict[str, bool]:
    """
    Check which tables are available in the given directory.

    Returns:
        Dictionary mapping table names to availability status
    """
    availability = {}
    for name in TABLE_NAMES:
        dbf_path = dbf_dir / f"{name}.DBF"
        availability[name] = dbf_path.exists()

    return availability


def print_import_status():
    """Print status of model imports for debugging."""
    print("Model Import Status:")
    print("-" * 40)

    for table_name, metadata in TABLE_METADATA.items():
        if metadata.model_class:
            status = "✓ OK"
        else:
            status = "✗ MISSING"
        print(f"{table_name:<10} {status}")

    if IMPORT_ERRORS:
        print("\nImport Errors:")
        print("-" * 40)
        for module, error in IMPORT_ERRORS.items():
            print(f"{module}: {error}")


if __name__ == "__main__":
    print_import_status()