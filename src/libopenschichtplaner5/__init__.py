"""
OpenSchichtplaner5 Core Library

A comprehensive Python library for reading, analyzing, and processing 
Schichtplaner5 database files (DBF format).
"""

__version__ = "1.0.0"

# Export core components
from .registry_improved import enhanced_registry
from .query_engine import QueryEngine
from .export import DataExporter, ExportFormat
from .relationships_improved import RelationshipResolver

# Export commonly used models
from .models.employee import Employee
from .models.group import Group
from .models.shift import Shift
from .models.absence import Absence
from .models.work_location import WorkLocation

# Export utilities
from .utils.validation import DataValidator

__all__ = [
    "enhanced_registry",
    "QueryEngine", 
    "DataExporter",
    "ExportFormat",
    "RelationshipResolver",
    "Employee",
    "Group", 
    "Shift",
    "Absence",
    "WorkLocation",
    "DataValidator",
]