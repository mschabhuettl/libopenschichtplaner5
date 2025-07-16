"""
Registry for DBF table handlers in libopenschichtplaner5.
This allows dynamic loading of different DBF tables using a unified interface.
"""

from pathlib import Path
from typing import Callable, Dict, List, Union

from libopenschichtplaner5.models.employee import Employee, load_employees
from libopenschichtplaner5.models.shift import Shift, load_shifts
from libopenschichtplaner5.models.group import Group, load_groups

# Define a generic return type for table loaders
ModelType = Union[Employee, Shift, Group]
LoaderFunction = Callable[[Path], List[ModelType]]

# Central registry mapping DBF table names to their corresponding loader function
TABLE_REGISTRY: Dict[str, LoaderFunction] = {
    "5EMPL": load_employees,
    "5SHIFT": load_shifts,
    "5GROUP": load_groups,
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
