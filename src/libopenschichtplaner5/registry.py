"""
Registry for DBF table handlers in libopenschichtplaner5.
This allows dynamic loading of different DBF tables using a unified interface.
"""

from pathlib import Path
from typing import Callable, Dict, List, Union

from libopenschichtplaner5.models.absence import Absence, load_absences
from libopenschichtplaner5.models.book import Book, load_books
from libopenschichtplaner5.models.build import Build, load_builds
from libopenschichtplaner5.models.cycle_assignment import CycleAssignment, load_cycle_assignments
from libopenschichtplaner5.models.cycle import Cycle, load_cycles
from libopenschichtplaner5.models.cycle_entitlement import CycleEntitlement, load_cycle_entitlements
from libopenschichtplaner5.models.cycle_exception import CycleException, load_cycle_exceptions
from libopenschichtplaner5.models.shift_demand import ShiftDemand, load_shift_demands
from libopenschichtplaner5.models.employee_access import EmployeeAccess, load_employee_access
from libopenschichtplaner5.models.employee import Employee, load_employees
from libopenschichtplaner5.models.group_access import GroupAccess, load_group_access
from libopenschichtplaner5.models.group_assignment import GroupAssignment, load_group_assignments
from libopenschichtplaner5.models.group import Group, load_groups
from libopenschichtplaner5.models.holiday_assignment import HolidayAssignment, load_holiday_assignments
from libopenschichtplaner5.models.holiday import Holiday, load_holidays
from libopenschichtplaner5.models.leave_entitlement import LeaveEntitlement, load_leave_entitlements
from libopenschichtplaner5.models.leave_type import LeaveType, load_leavetypes
from libopenschichtplaner5.models.employee_shift import EmployeeShift, load_employee_shifts
from libopenschichtplaner5.models.note import Note, load_notes
from libopenschichtplaner5.models.period import Period, load_periods
from libopenschichtplaner5.models.shift_restriction import ShiftRestriction, load_shift_restrictions
from libopenschichtplaner5.models.shift_schedule import load_shift_schedules
from libopenschichtplaner5.models.shift import Shift, load_shifts
from libopenschichtplaner5.models.shift_plan_demand import ShiftPlanDemand, load_shift_plan_demands
from libopenschichtplaner5.models.shift_detail import ShiftDetail, load_shift_details
from libopenschichtplaner5.models.user import User, load_users
from libopenschichtplaner5.models.user_setting import UserSetting, load_user_settings
from libopenschichtplaner5.models.work_location import WorkLocation, load_work_locations
from libopenschichtplaner5.models.shift_rule import ShiftRule, load_shift_rules

# Define a generic return type for table loaders
ModelType = Union[Employee, Shift, Group, User]
LoaderFunction = Callable[[Path], List[ModelType]]

# Central registry mapping DBF table names to their corresponding loader function
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
    "5LEAVT": load_leavetypes,
    "5MASHI": load_employee_shifts,
    "5NOTE": load_notes,
    "5PERIO": load_periods,
    "5RESTR": load_shift_restrictions,
    "5SHDEM": load_shift_schedules,
    "5SHIFT": load_shifts,
    "5SPDEM": load_shift_plan_demands,
    "5SPSHI": load_shift_details,
    "5USER": load_users,
    "5USETT": load_user_settings,
    "5WOPL": load_work_locations,
    "5XCHAR": load_shift_rules,
***REMOVED***

# List of allowed table names (used in CLI argument parser)
TABLE_NAMES = list(TABLE_REGISTRY.keys())

def load_table(name: str, path: Path) -> List[ModelType]:
    """
    Loads a DBF table based on its registered name using the correct loader.

    :param name: Table name, e.g. "5EMPL"
    :param path: Path to DBF file
    :return: List of parsed model instances
    """
    if name not in TABLE_REGISTRY:
        raise ValueError(f"Unknown table name: {name***REMOVED***")
    return TABLE_REGISTRY[name](path)
