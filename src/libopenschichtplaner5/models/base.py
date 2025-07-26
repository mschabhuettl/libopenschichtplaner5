# src/libopenschichtplaner5/models/base.py
"""
Base classes and mixins for all Schichtplaner5 data models.
Provides validation, constraints, and common functionality.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional, Set, Type, Union, Callable
from datetime import date, datetime
from enum import Enum
import re
from ..exceptions import SchichtplanerError


class ValidationLevel(Enum):
    """Validation strictness levels."""
    STRICT = "strict"      # Fail on any validation error
    LENIENT = "lenient"    # Warn on validation errors
    PERMISSIVE = "permissive"  # Log validation errors but continue


@dataclass
class FieldConstraint:
    """Constraint definition for model fields."""
    field_name: str
    constraint_type: str
    value: Any
    message: str = ""
    
    def validate(self, field_value: Any) -> bool:
        """Validate field value against constraint."""
        if self.constraint_type == "required" and field_value is None:
            return False
        elif self.constraint_type == "min_length" and len(str(field_value)) < self.value:
            return False
        elif self.constraint_type == "max_length" and len(str(field_value)) > self.value:
            return False
        elif self.constraint_type == "pattern" and not re.match(self.value, str(field_value)):
            return False
        elif self.constraint_type == "range" and not (self.value[0] <= field_value <= self.value[1]):
            return False
        elif self.constraint_type == "in_set" and field_value not in self.value:
            return False
        
        return True


@dataclass
class ValidationError:
    """Represents a validation error."""
    field_name: str
    constraint_type: str
    message: str
    value: Any
    model_type: str


class ValidatedModel(ABC):
    """Base class for all validated data models."""
    
    _constraints: Dict[str, List[FieldConstraint]] = {}
    _validation_level: ValidationLevel = ValidationLevel.LENIENT
    
    def __post_init__(self):
        """Run validation after initialization."""
        if hasattr(super(), '__post_init__'):
            super().__post_init__()
        self.validate()
    
    @classmethod
    def define_constraint(cls, field_name: str, constraint_type: str, 
                         value: Any, message: str = ""):
        """Define a constraint for a field."""
        if cls.__name__ not in cls._constraints:
            cls._constraints[cls.__name__] = {}
        if field_name not in cls._constraints[cls.__name__]:
            cls._constraints[cls.__name__][field_name] = []
        
        constraint = FieldConstraint(field_name, constraint_type, value, message)
        cls._constraints[cls.__name__][field_name].append(constraint)
    
    @classmethod  
    def set_validation_level(cls, level: ValidationLevel):
        """Set validation level for this model."""
        cls._validation_level = level
    
    def validate(self, level: Optional[ValidationLevel] = None) -> List[ValidationError]:
        """Validate the model against defined constraints."""
        validation_level = level or self._validation_level
        errors = []
        
        model_constraints = self._constraints.get(self.__class__.__name__, {})
        
        for field_name, constraints in model_constraints.items():
            field_value = getattr(self, field_name, None)
            
            for constraint in constraints:
                if not constraint.validate(field_value):
                    error = ValidationError(
                        field_name=field_name,
                        constraint_type=constraint.constraint_type,
                        message=constraint.message or f"{constraint.constraint_type} constraint failed",
                        value=field_value,
                        model_type=self.__class__.__name__
                    )
                    errors.append(error)
                    
                    if validation_level == ValidationLevel.STRICT:
                        raise SchichtplanerError(f"Validation failed: {error.message}")
        
        return errors
    
    @abstractmethod
    def get_primary_key(self) -> Any:
        """Return the primary key value."""
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        result = {}
        for field_def in fields(self):
            value = getattr(self, field_def.name)
            if isinstance(value, (date, datetime)):
                result[field_def.name] = value.isoformat()
            else:
                result[field_def.name] = value
        return result
    
    @classmethod
    @abstractmethod
    def from_record(cls, record: Dict[str, Any]) -> "ValidatedModel":
        """Create instance from DBF record."""
        pass


class TimestampMixin:
    """Mixin for models with timestamp fields."""
    
    def format_date(self, date_value: Optional[Union[date, datetime, str]]) -> Optional[date]:
        """Format various date inputs to date object."""
        if not date_value:
            return None
        
        if isinstance(date_value, date):
            return date_value
        
        if isinstance(date_value, datetime):
            return date_value.date()
        
        if isinstance(date_value, str):
            # Try common formats
            formats = ["%Y-%m-%d", "%d.%m.%Y", "%Y%m%d", "%d/%m/%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt).date()
                except ValueError:
                    continue
        
        return None


class EmployeeModelMixin:
    """Mixin for employee-related models."""
    
    def get_employee_id(self) -> Optional[int]:
        """Get employee ID if this model has one."""
        return getattr(self, 'employee_id', None)


@dataclass
class BaseSchichtplanerModel(ValidatedModel, TimestampMixin):
    """Enhanced base class for all Schichtplaner5 models."""
    
    id: int
    
    def get_primary_key(self) -> int:
        """Return the primary key value."""
        return self.id
    
    @classmethod
    def define_standard_constraints(cls):
        """Define standard constraints for Schichtplaner models."""
        cls.define_constraint('id', 'required', True, 'ID is required')
        cls.define_constraint('id', 'range', (1, 999999999), 'ID must be positive')


# Decorator for easy constraint definition
def constraint(field: str, type: str, value: Any, message: str = ""):
    """Decorator to define field constraints."""
    def decorator(cls):
        cls.define_constraint(field, type, value, message)
        return cls
    return decorator


# Common constraint validators
def email_validator(value: str) -> bool:
    """Validate email format."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, value) is not None


def phone_validator(value: str) -> bool:
    """Validate phone number format."""
    # Simple German phone number validation
    cleaned = re.sub(r'[^\d+]', '', value)
    return len(cleaned) >= 10


# Model registry for runtime discovery
model_registry: Dict[str, Type[ValidatedModel]] = {}


def register_model(table_name: str):
    """Decorator to register a model with a table name."""
    def decorator(cls: Type[ValidatedModel]):
        model_registry[table_name] = cls
        return cls
    return decorator


def get_model_for_table(table_name: str) -> Optional[Type[ValidatedModel]]:
    """Get the model class for a table name."""
    return model_registry.get(table_name)


# Factory function for creating models
def create_model_instance(table_name: str, record: Dict[str, Any]) -> Optional[ValidatedModel]:
    """Create a model instance from a record."""
    model_class = get_model_for_table(table_name)
    if model_class:
        return model_class.from_record(record)
    return None