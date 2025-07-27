# libopenschichtplaner5/src/libopenschichtplaner5/registry.py
"""
Central registry for all Schichtplaner5 tables.
Manages table metadata and model loading.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional, Callable, Type
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """Metadata for a Schichtplaner5 table."""
    name: str
    description: str
    model_class: Optional[Type] = None
    loader_func: Optional[Callable] = None
    required_fields: List[str] = None
    optional: bool = False
    
    def __post_init__(self):
        if self.required_fields is None:
            self.required_fields = ["id"]


# Central registry of all tables
TABLE_REGISTRY: Dict[str, TableMetadata] = {}


def register_table(name: str, description: str, optional: bool = False):
    """Register a table in the registry."""
    TABLE_REGISTRY[name] = TableMetadata(
        name=name,
        description=description,
        optional=optional
    )


# Register all known tables
register_table("5EMPL", "Employee master data")
register_table("5GROUP", "Groups/Departments")
register_table("5SHIFT", "Shift definitions")
register_table("5WOPL", "Work locations/Workplaces")
register_table("5ABSEN", "Absences")
register_table("5LEAVT", "Leave types")
register_table("5SPSHI", "Employee shift details")
register_table("5MASHI", "Employee shift assignments")  # Legacy name, maps to 5SPSHI
register_table("5MASPD", "Employee shift plan data", optional=True)
register_table("5NOTE", "Notes")
register_table("5GRASG", "Group assignments")
register_table("5LEAEN", "Leave entitlements")
register_table("5CYASS", "Cycle assignments")
register_table("5CYCLE", "Cycles")
register_table("5HOLID", "Holidays")
register_table("5USER", "Users")
register_table("5BOOK", "Bookings/Overtime")
register_table("5HOASS", "Holiday assignments")
register_table("5CYENT", "Cycle entitlements")
register_table("5CYEXC", "Cycle exceptions", optional=True)
register_table("5SPMED", "Shift demands", optional=True)
register_table("5SHRES", "Shift restrictions", optional=True)
register_table("5SCHED", "Shift schedules", optional=True)
register_table("5SPDEM", "Shift plan demands", optional=True)
register_table("5USSET", "User settings", optional=True)
register_table("5PERIO", "Periods", optional=True)
register_table("5GRACC", "Group access", optional=True)
register_table("5EMACC", "Employee access", optional=True)
register_table("5HOBAN", "Holiday bans", optional=True)
register_table("5BUILD", "Build information", optional=True)
register_table("5XCHAR", "Extra characteristics/Surcharges", optional=True)
register_table("5OVER", "Overtime records", optional=True)


# Lazy loading of models
_model_cache: Dict[str, Type] = {}
_loader_cache: Dict[str, Callable] = {}


def _lazy_load_model(table_name: str) -> Optional[Type]:
    """Lazy load model class for a table."""
    if table_name in _model_cache:
        return _model_cache[table_name]
    
    # Model mapping
    model_mapping = {
        "5EMPL": ("employee", "Employee"),
        "5GROUP": ("group", "Group"),
        "5SHIFT": ("shift", "Shift"),
        "5WOPL": ("work_location", "WorkLocation"),
        "5ABSEN": ("absence", "Absence"),
        "5LEAVT": ("leave_type", "LeaveType"),
        "5SPSHI": ("shift_detail", "ShiftDetail"),
        "5MASHI": ("employee_shift", "EmployeeShift"),
        "5NOTE": ("note", "Note"),
        "5GRASG": ("group_assignment", "GroupAssignment"),
        "5LEAEN": ("leave_entitlement", "LeaveEntitlement"),
        "5CYASS": ("cycle_assignment", "CycleAssignment"),
        "5CYCLE": ("cycle", "Cycle"),
        "5HOLID": ("holiday", "Holiday"),
        "5USER": ("user", "User"),
        "5BOOK": ("book", "Book"),
        "5HOASS": ("holiday_assignment", "HolidayAssignment"),
        "5CYENT": ("cycle_entitlement", "CycleEntitlement"),
        "5CYEXC": ("cycle_exception", "CycleException"),
        "5SPMED": ("shift_demand", "ShiftDemand"),
        "5SHRES": ("shift_restriction", "ShiftRestriction"),
        "5SCHED": ("shift_schedule", "ShiftSchedule"),
        "5SPDEM": ("shift_plan_demand", "ShiftPlanDemand"),
        "5USSET": ("user_setting", "UserSetting"),
        "5PERIO": ("period", "Period"),
        "5GRACC": ("group_access", "GroupAccess"),
        "5EMACC": ("employee_access", "EmployeeAccess"),
        "5BUILD": ("build", "Build"),
        "5XCHAR": ("xchar", "XChar"),
        "5OVER": ("overtime", "Overtime"),
    }
    
    if table_name not in model_mapping:
        return None
    
    module_name, class_name = model_mapping[table_name]
    
    try:
        module = __import__(f"libopenschichtplaner5.models.{module_name}", 
                           fromlist=[class_name])
        model_class = getattr(module, class_name)
        _model_cache[table_name] = model_class
        
        # Update registry
        if table_name in TABLE_REGISTRY:
            TABLE_REGISTRY[table_name].model_class = model_class
        
        return model_class
    except (ImportError, AttributeError) as e:
        logger.warning(f"Could not load model for {table_name}: {e}")
        return None


def _lazy_load_loader(table_name: str) -> Optional[Callable]:
    """Lazy load loader function for a table."""
    if table_name in _loader_cache:
        return _loader_cache[table_name]
    
    # Loader mapping
    loader_mapping = {
        "5EMPL": ("employee", "load_employees"),
        "5GROUP": ("group", "load_groups"),
        "5SHIFT": ("shift", "load_shifts"),
        "5WOPL": ("work_location", "load_work_locations"),
        "5ABSEN": ("absence", "load_absences"),
        "5LEAVT": ("leave_type", "load_leave_types"),
        "5SPSHI": ("shift_detail", "load_shift_details"),
        "5MASHI": ("employee_shift", "load_employee_shifts"),
        "5NOTE": ("note", "load_notes"),
        "5GRASG": ("group_assignment", "load_group_assignments"),
        "5LEAEN": ("leave_entitlement", "load_leave_entitlements"),
        "5CYASS": ("cycle_assignment", "load_cycle_assignments"),
        "5CYCLE": ("cycle", "load_cycles"),
        "5HOLID": ("holiday", "load_holidays"),
        "5USER": ("user", "load_users"),
        "5BOOK": ("book", "load_books"),
        "5HOASS": ("holiday_assignment", "load_holiday_assignments"),
        "5CYENT": ("cycle_entitlement", "load_cycle_entitlements"),
        "5CYEXC": ("cycle_exception", "load_cycle_exceptions"),
        "5SPMED": ("shift_demand", "load_shift_demands"),
        "5SHRES": ("shift_restriction", "load_shift_restrictions"),
        "5SCHED": ("shift_schedule", "load_shift_schedules"),
        "5SPDEM": ("shift_plan_demand", "load_shift_plan_demands"),
        "5USSET": ("user_setting", "load_user_settings"),
        "5PERIO": ("period", "load_periods"),
        "5GRACC": ("group_access", "load_group_access"),
        "5EMACC": ("employee_access", "load_employee_access"),
        "5BUILD": ("build", "load_builds"),
        "5XCHAR": ("xchar", "load_xchar"),
        "5OVER": ("overtime", "load_overtime"),
    }
    
    if table_name not in loader_mapping:
        return None
    
    module_name, func_name = loader_mapping[table_name]
    
    try:
        module = __import__(f"libopenschichtplaner5.models.{module_name}", 
                           fromlist=[func_name])
        loader_func = getattr(module, func_name)
        _loader_cache[table_name] = loader_func
        
        # Update registry
        if table_name in TABLE_REGISTRY:
            TABLE_REGISTRY[table_name].loader_func = loader_func
        
        return loader_func
    except (ImportError, AttributeError) as e:
        logger.warning(f"Could not load loader for {table_name}: {e}")
        return None


def load_table(name: str, path: Path) -> List[Any]:
    """Load a table from DBF file."""
    if name not in TABLE_REGISTRY:
        # Handle special cases
        if name == "5MASHI":
            # 5MASHI is sometimes an alias for 5SPSHI
            name = "5SPSHI"
        else:
            raise ValueError(f"Unknown table name: {name}")
    
    metadata = TABLE_REGISTRY[name]
    
    # Check if file exists
    if not path.exists():
        if metadata.optional:
            logger.info(f"Optional table {name} not found at {path}")
            return []
        else:
            raise FileNotFoundError(f"Required table {name} not found at {path}")
    
    # Get loader function
    loader = metadata.loader_func or _lazy_load_loader(name)
    
    if not loader:
        # Fallback: try generic loading with model
        model_class = metadata.model_class or _lazy_load_model(name)
        if model_class and hasattr(model_class, 'from_record'):
            from .db.reader import DBFTable
            table = DBFTable(path)
            return [model_class.from_record(record) for record in table.records()]
        else:
            raise ValueError(f"No loader available for table {name}")
    
    try:
        return loader(path)
    except Exception as e:
        if metadata.optional:
            logger.warning(f"Failed to load optional table {name}: {e}")
            return []
        else:
            raise


def get_table_info(name: str) -> Optional[TableMetadata]:
    """Get metadata for a table."""
    # Handle aliases
    if name == "5MASHI":
        name = "5SPSHI"
    return TABLE_REGISTRY.get(name)


# Convenience lists
TABLE_NAMES = list(TABLE_REGISTRY.keys())
TABLE_METADATA = TABLE_REGISTRY


# For backward compatibility
def get_all_table_names() -> List[str]:
    """Get all registered table names."""
    return TABLE_NAMES


def is_table_optional(name: str) -> bool:
    """Check if a table is optional."""
    metadata = get_table_info(name)
    return metadata.optional if metadata else False
