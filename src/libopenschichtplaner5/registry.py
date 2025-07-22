# libopenschichtplaner5/src/libopenschichtplaner5/registry.py
"""
Registry für DBF-Tabellen-Handler.
"""
from pathlib import Path
from typing import Callable, Dict, List, Union, Any

# Models
from .models.absence import Absence, load_absences
from .models.book import Book, load_books
from .models.build import Build, load_builds
from .models.cycle_assignment import CycleAssignment, load_cycle_assignments
from .models.cycle import Cycle, load_cycles
from .models.cycle_entitlement import CycleEntitlement, load_cycle_entitlements
from .models.cycle_exception import CycleException, load_cycle_exceptions
from .models.shift_demand import ShiftDemand, load_shift_demands
from .models.employee_access import EmployeeAccess, load_employee_access
from .models.employee import Employee, load_employees
from .models.group_access import GroupAccess, load_group_access
from .models.group_assignment import GroupAssignment, load_group_assignments
from .models.group import Group, load_groups
from .models.holiday_assignment import HolidayAssignment, load_holiday_assignments
from .models.holiday import Holiday, load_holidays
from .models.leave_entitlement import LeaveEntitlement, load_leave_entitlements
from .models.leave_type import LeaveType, load_leave_types
from .models.employee_shift import EmployeeShift, load_employee_shifts
from .models.note import Note, load_notes
from .models.period import Period, load_periods
from .models.shift_restriction import ShiftRestriction, load_shift_restrictions
from .models.shift_schedule import ShiftSchedule, load_shift_schedules
from .models.shift import Shift, load_shifts
from .models.shift_plan_demand import ShiftPlanDemand, load_shift_plan_demands
from .models.shift_detail import ShiftDetail, load_shift_details
from .models.user import User, load_users
from .models.user_setting import UserSetting, load_user_settings
from .models.work_location import WorkLocation, load_work_locations

# Neue/Umbenannte Models
try:
    from .models.overtime import Overtime, load_overtime
except ImportError:
    print("Warning: overtime.py not found - creating dummy loader")
    def load_overtime(path):
        return []

try:
    from .models.xchar import XChar, load_xchar
except ImportError:
    # Fallback auf shift_rule wenn xchar noch nicht umbenannt
    try:
        from .models.shift_rule import ShiftRule as XChar, load_shift_rules as load_xchar
    except ImportError:
        print("Warning: xchar.py/shift_rule.py not found - creating dummy loader")
        def load_xchar(path):
            return []

# Type definitions
ModelType = Union[Employee, Shift, Group, User, Any]
LoaderFunction = Callable[[Path], List[ModelType]]

# Central registry
TABLE_REGISTRY: Dict[str, LoaderFunction] = {
    "5ABSEN": load_absences,
    "5BOOK": load_books,
    "5BUILD": load_builds,
    "5CYASS": load_cycle_assignments,
    "5CYCLE": load_cycles,
    "5CYENT": load_cycle_entitlements,
    "5CYEXC": load_cycle_exceptions,
    "5DADEM": load_shift_demands,
    "5EMACC": load_employee_access,
    "5EMPL": load_employees,
    "5GRACC": load_group_access,
    "5GRASG": load_group_assignments,
    "5GROUP": load_groups,
    "5HOBAN": load_holiday_assignments,
    "5HOLID": load_holidays,
    "5LEAEN": load_leave_entitlements,
    "5LEAVT": load_leave_types,
    "5MASHI": load_employee_shifts,  # Mitarbeiterschichten!
    "5NOTE": load_notes,
    "5OVER": load_overtime,
    "5PERIO": load_periods,
    "5RESTR": load_shift_restrictions,
    "5SHDEM": load_shift_schedules,
    "5SHIFT": load_shifts,
    "5SPDEM": load_shift_plan_demands,
    "5SPSHI": load_shift_details,
    "5USER": load_users,
    "5USETT": load_user_settings,
    "5WOPL": load_work_locations,
    "5XCHAR": load_xchar,
***REMOVED***

# List of table names
TABLE_NAMES = list(TABLE_REGISTRY.keys())


def load_table(name: str, path: Path) -> List[ModelType]:
    """
    Lädt eine DBF-Tabelle.

    Args:
        name: Tabellenname (z.B. "5EMPL")
        path: Pfad zur DBF-Datei

    Returns:
        Liste der geladenen Model-Instanzen
    """
    if name not in TABLE_REGISTRY:
        raise ValueError(f"Unknown table name: {name***REMOVED***. Available: {', '.join(TABLE_NAMES)***REMOVED***")

    loader = TABLE_REGISTRY[name]
    try:
        return loader(path)
    except Exception as e:
        print(f"Error loading {name***REMOVED*** from {path***REMOVED***: {e***REMOVED***")
        raise