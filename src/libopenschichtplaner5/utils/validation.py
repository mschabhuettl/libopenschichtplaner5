# libopenschichtplaner5/src/libopenschichtplaner5/utils/validation.py
"""
Data validation and cleaning utilities for Schichtplaner5 data.
Handles common data quality issues in DBF files.
"""

from typing import Any, List, Dict, Optional, Set, Tuple
from datetime import datetime, date
from dataclasses import dataclass, field
import re
from collections import defaultdict


@dataclass
class ValidationError:
    """Represents a single validation error."""
    table: str
    record_id: Any
    field: str
    error_type: str
    message: str
    value: Any = None

    def __str__(self):
        return f"[{self.table}:{self.record_id}] {self.field}: {self.message}"


@dataclass
class ValidationReport:
    """Report of all validation errors found."""
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    statistics: Dict[str, int] = field(default_factory=dict)

    def add_error(self, error: ValidationError):
        """Add an error to the report."""
        self.errors.append(error)
        error_key = f"{error.table}:{error.error_type}"
        self.statistics[error_key] = self.statistics.get(error_key, 0) + 1

    def add_warning(self, warning: ValidationError):
        """Add a warning to the report."""
        self.warnings.append(warning)
        warning_key = f"{warning.table}:{warning.error_type}:warning"
        self.statistics[warning_key] = self.statistics.get(warning_key, 0) + 1

    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0

    def summary(self) -> str:
        """Get a summary of the validation report."""
        lines = [
            f"Validation Report Summary:",
            f"  Total Errors: {len(self.errors)}",
            f"  Total Warnings: {len(self.warnings)}",
            "",
            "Error Statistics:"
        ]

        for key, count in sorted(self.statistics.items()):
            lines.append(f"  {key}: {count}")

        return "\n".join(lines)


class DataValidator:
    """Validates Schichtplaner5 data for consistency and integrity."""

    def __init__(self):
        self.report = ValidationReport()

        # Define validation rules for each table
        self.field_rules = {
            "5EMPL": {
                "id": {"required": True, "type": int, "unique": True},
                "name": {"required": True, "type": str, "min_length": 1},
                "email": {"type": str, "pattern": r'^[\w\.-]+@[\w\.-]+\.\w+$', "required": False},
                "empstart": {"type": date, "required": False},
                "empend": {"type": date, "required": False, "after": "empstart"},
            },
            "5ABSEN": {
                "id": {"required": True, "type": int, "unique": True},
                "employee_id": {"required": True, "type": int, "foreign_key": "5EMPL.id"},
                "date": {"required": True, "type": date},
                "leave_type_id": {"required": True, "type": int, "foreign_key": "5LEAVT.id"},
            },
            "5SPSHI": {
                "id": {"required": True, "type": int, "unique": True},
                "employee_id": {"required": True, "type": int, "foreign_key": "5EMPL.id"},
                "shift_id": {"required": True, "type": int, "foreign_key": "5SHIFT.id"},
                "workplace_id": {"required": True, "type": int, "foreign_key": "5WOPL.id"},
                "date": {"required": True, "type": date},
            },
            "5NOTE": {
                "id": {"required": True, "type": int, "unique": True},
                "employee_id": {"required": True, "type": int, "foreign_key": "5EMPL.id"},
                "date": {"required": True, "type": date},
            },
            "5GRASG": {
                "id": {"required": True, "type": int, "unique": True},
                "employee_id": {"required": True, "type": int, "foreign_key": "5EMPL.id"},
                "group_id": {"required": True, "type": int, "foreign_key": "5GROUP.id"},
            },
            "5CYASS": {
                "id": {"required": True, "type": int, "unique": True},
                "employee_id": {"required": True, "type": int, "foreign_key": "5EMPL.id"},
                "cycle_id": {"required": True, "type": int, "foreign_key": "5CYCLE.id"},
                "start": {"required": True, "type": date},
                "end": {"required": True, "type": date, "after": "start"},
            }
        }

    def validate_all_tables(self, loaded_tables: Dict[str, List[Any]]) -> ValidationReport:
        """Validate all loaded tables."""
        self.report = ValidationReport()

        # First pass: validate individual records
        for table_name, records in loaded_tables.items():
            if table_name in self.field_rules:
                self._validate_table(table_name, records)

        # Second pass: validate referential integrity
        self._validate_referential_integrity(loaded_tables)

        # Third pass: validate business rules
        self._validate_business_rules(loaded_tables)

        return self.report

    def _validate_table(self, table_name: str, records: List[Any]):
        """Validate all records in a table."""
        rules = self.field_rules.get(table_name, {})
        seen_ids = set()

        for record in records:
            record_id = getattr(record, "id", None)

            for field_name, field_rules in rules.items():
                value = getattr(record, field_name, None)

                # Check required fields
                if field_rules.get("required", False) and value is None:
                    self.report.add_error(ValidationError(
                        table_name, record_id, field_name,
                        "missing_required", f"Required field is missing",
                        value
                    ))
                    continue

                if value is not None:
                    # Type validation
                    expected_type = field_rules.get("type")
                    if expected_type and not self._check_type(value, expected_type):
                        self.report.add_error(ValidationError(
                            table_name, record_id, field_name,
                            "invalid_type", f"Expected {expected_type.__name__}, got {type(value).__name__}",
                            value
                        ))

                    # String validations
                    if expected_type == str:
                        # Min length
                        min_length = field_rules.get("min_length", 0)
                        if len(value.strip()) < min_length:
                            self.report.add_error(ValidationError(
                                table_name, record_id, field_name,
                                "too_short", f"Value too short (min: {min_length})",
                                value
                            ))

                        # Pattern matching
                        pattern = field_rules.get("pattern")
                        if pattern and not re.match(pattern, value):
                            self.report.add_warning(ValidationError(
                                table_name, record_id, field_name,
                                "invalid_format", f"Value doesn't match expected pattern",
                                value
                            ))

                    # Unique constraint
                    if field_rules.get("unique", False) and field_name == "id":
                        if value in seen_ids:
                            self.report.add_error(ValidationError(
                                table_name, record_id, field_name,
                                "duplicate_id", f"Duplicate ID found: {value}",
                                value
                            ))
                        seen_ids.add(value)

    def _check_type(self, value: Any, expected_type: type) -> bool:
        """Check if value matches expected type."""
        if expected_type == date:
            return isinstance(value, (date, datetime))
        return isinstance(value, expected_type)

    def _validate_referential_integrity(self, loaded_tables: Dict[str, List[Any]]):
        """Validate foreign key relationships."""
        # Build ID indexes for all tables
        id_indexes = {}
        for table_name, records in loaded_tables.items():
            id_indexes[table_name] = set()
            for record in records:
                if hasattr(record, "id"):
                    id_indexes[table_name].add(record.id)

        # Check foreign keys
        for table_name, rules in self.field_rules.items():
            if table_name not in loaded_tables:
                continue

            for field_name, field_rules in rules.items():
                fk = field_rules.get("foreign_key")
                if not fk:
                    continue

                ref_table, ref_field = fk.split(".")
                if ref_table not in id_indexes:
                    continue

                # Check each record
                for record in loaded_tables[table_name]:
                    value = getattr(record, field_name, None)
                    if value is not None and value not in id_indexes[ref_table]:
                        self.report.add_error(ValidationError(
                            table_name, getattr(record, "id", None), field_name,
                            "invalid_reference",
                            f"References non-existent {ref_table}.{ref_field}={value}",
                            value
                        ))

    def _validate_business_rules(self, loaded_tables: Dict[str, List[Any]]):
        """Validate business-specific rules."""
        # Check for overlapping shifts
        if "5SPSHI" in loaded_tables:
            self._check_overlapping_shifts(loaded_tables["5SPSHI"])

        # Check for overlapping absences
        if "5ABSEN" in loaded_tables:
            self._check_overlapping_absences(loaded_tables["5ABSEN"])

        # Check cycle assignments
        if "5CYASS" in loaded_tables:
            self._check_cycle_assignments(loaded_tables["5CYASS"])

    def _check_overlapping_shifts(self, shifts: List[Any]):
        """Check for employees with multiple shifts on the same day."""
        employee_dates = defaultdict(list)

        for shift in shifts:
            key = (shift.employee_id, shift.date)
            employee_dates[key].append(shift)

        for (emp_id, date), shift_list in employee_dates.items():
            if len(shift_list) > 1:
                self.report.add_warning(ValidationError(
                    "5SPSHI", shift_list[0].id, "date",
                    "multiple_shifts",
                    f"Employee {emp_id} has {len(shift_list)} shifts on {date}",
                    date
                ))

    def _check_overlapping_absences(self, absences: List[Any]):
        """Check for employees with multiple absences on the same day."""
        employee_dates = defaultdict(list)

        for absence in absences:
            key = (absence.employee_id, absence.date)
            employee_dates[key].append(absence)

        for (emp_id, date), absence_list in employee_dates.items():
            if len(absence_list) > 1:
                self.report.add_error(ValidationError(
                    "5ABSEN", absence_list[0].id, "date",
                    "multiple_absences",
                    f"Employee {emp_id} has {len(absence_list)} absences on {date}",
                    date
                ))

    def _check_cycle_assignments(self, assignments: List[Any]):
        """Check for overlapping cycle assignments."""
        employee_cycles = defaultdict(list)

        for assignment in assignments:
            employee_cycles[assignment.employee_id].append(assignment)

        for emp_id, cycles in employee_cycles.items():
            # Sort by start date
            cycles.sort(key=lambda x: x.start if x.start else date.min)

            # Check for overlaps
            for i in range(len(cycles) - 1):
                current = cycles[i]
                next_cycle = cycles[i + 1]

                if current.end and next_cycle.start and current.end >= next_cycle.start:
                    self.report.add_error(ValidationError(
                        "5CYASS", current.id, "end",
                        "overlapping_cycles",
                        f"Cycle overlaps with next cycle (ends {current.end}, next starts {next_cycle.start})",
                        current.end
                    ))


class DataCleaner:
    """Cleans and normalizes Schichtplaner5 data."""

    @staticmethod
    def clean_string(value: str) -> str:
        """Clean string values."""
        if not value:
            return ""

        # Remove null bytes
        value = value.replace("\x00", "")

        # Strip whitespace
        value = value.strip()

        # Normalize whitespace
        value = " ".join(value.split())

        return value

    @staticmethod
    def clean_date(value: Any) -> Optional[date]:
        """Clean and normalize date values."""
        if not value:
            return None

        if isinstance(value, date):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            # Try common date formats
            formats = [
                "%Y-%m-%d",
                "%d.%m.%Y",
                "%d/%m/%Y",
                "%Y%m%d",
                "%d-%m-%Y"
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue

        return None

    @staticmethod
    def clean_email(value: str) -> str:
        """Clean and validate email addresses."""
        if not value:
            return ""

        value = DataCleaner.clean_string(value).lower()

        # Basic email validation
        if "@" in value and "." in value.split("@")[1]:
            return value

        return ""

    @staticmethod
    def clean_phone(value: str) -> str:
        """Clean phone numbers."""
        if not value:
            return ""

        # Remove all non-digit characters
        digits = re.sub(r'\D', '', value)

        # Format based on length (assuming German numbers)
        if len(digits) >= 10:
            return digits

        return value

    @classmethod
    def clean_record(cls, record: Any, table_name: str) -> Any:
        """Clean all fields in a record based on table type."""
        # Define cleaning rules per table/field
        cleaning_rules = {
            "5EMPL": {
                "name": cls.clean_string,
                "firstname": cls.clean_string,
                "email": cls.clean_email,
                "phone": cls.clean_phone,
                "street": cls.clean_string,
                "town": cls.clean_string,
                "empstart": cls.clean_date,
                "empend": cls.clean_date,
                "birthday": cls.clean_date,
            },
            "5ABSEN": {
                "date": cls.clean_date,
            },
            "5SPSHI": {
                "date": cls.clean_date,
            },
            "5NOTE": {
                "date": cls.clean_date,
                "text1": cls.clean_string,
                "text2": cls.clean_string,
            }
        }

        rules = cleaning_rules.get(table_name, {})

        for field, cleaner in rules.items():
            if hasattr(record, field):
                original = getattr(record, field)
                cleaned = cleaner(original)
                if cleaned != original:
                    setattr(record, field, cleaned)

        return record