"""
Registry for DBF table handlers in libopenschichtplaner5.
This allows dynamic loading of different DBF tables using a unified interface.
"""

from pathlib import Path
from typing import Callable, Dict, List, Union

from libopenschichtplaner5.models.employee import Employee, load_employees
from libopenschichtplaner5.models.shift import Shift, load_shifts
from libopenschichtplaner5.models.group import Group, load_groups
from libopenschichtplaner5.models.user import User, load_users
from libopenschichtplaner5.models.period import Period, load_periods
from libopenschichtplaner5.models.holiday import Holiday, load_holidays
from libopenschichtplaner5.models.leave_entitlement import LeaveEntitlement, load_leave_entitlements
from libopenschichtplaner5.models.leave_type import LeaveType, load_leavetypes
from libopenschichtplaner5.models.book import Book, load_books
from libopenschichtplaner5.models.absence import Absence, load_absences
from libopenschichtplaner5.models.note import Note, load_notes

# Define a generic return type for table loaders
ModelType = Union[Employee, Shift, Group, User]
LoaderFunction = Callable[[Path], List[ModelType]]

# Central registry mapping DBF table names to their corresponding loader function
TABLE_REGISTRY: Dict[str, LoaderFunction] = {
    "5EMPL": load_employees,
    "5SHIFT": load_shifts,
    "5GROUP": load_groups,
    "5USER": load_users,
    "5PERIO": load_periods,
    "5HOLID": load_holidays,
    "5LEAEN": load_leave_entitlements,
    "5LEAVT": load_leavetypes,
    "5BOOK": load_books,
    "5ABSEN": load_absences,
    "5NOTE": load_notes,
}

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
        raise ValueError(f"Unknown table name: {name}")
    return TABLE_REGISTRY[name](path)
