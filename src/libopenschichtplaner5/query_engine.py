# src/libopenschichtplaner5/query_engine.py
"""
Query engine for complex cross-table queries in Schichtplaner5 data.
Provides a fluent interface for building and executing queries across multiple tables.
"""
import logging

from typing import List, Dict, Any, Optional, Callable, Union, Tuple
from dataclasses import dataclass, field
from pathlib import Path
from datetime import date, datetime
from enum import Enum

from .relationships import relationship_manager, get_entity_with_relations
from .registry import load_table, TABLE_NAMES


class FilterOperator(Enum):
    """Supported filter operators."""
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUALS = ">="
    LESS_EQUALS = "<="
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class Filter:
    """Represents a single filter condition."""
    field: str
    operator: FilterOperator
    value: Any
    
    def apply(self, record: Any) -> bool:
        """Apply this filter to a record."""
        field_value = getattr(record, self.field, None)
        
        if self.operator == FilterOperator.IS_NULL:
            return field_value is None
        elif self.operator == FilterOperator.IS_NOT_NULL:
            return field_value is not None
        elif field_value is None:
            return False
        
        if self.operator == FilterOperator.EQUALS:
            return field_value == self.value
        elif self.operator == FilterOperator.NOT_EQUALS:
            return field_value != self.value
        elif self.operator == FilterOperator.GREATER_THAN:
            return field_value > self.value
        elif self.operator == FilterOperator.LESS_THAN:
            return field_value < self.value
        elif self.operator == FilterOperator.GREATER_EQUALS:
            return field_value >= self.value
        elif self.operator == FilterOperator.LESS_EQUALS:
            return field_value <= self.value
        elif self.operator == FilterOperator.IN:
            return field_value in self.value
        elif self.operator == FilterOperator.NOT_IN:
            return field_value not in self.value
        elif self.operator == FilterOperator.CONTAINS:
            return str(self.value).lower() in str(field_value).lower()
        elif self.operator == FilterOperator.STARTS_WITH:
            return str(field_value).lower().startswith(str(self.value).lower())
        elif self.operator == FilterOperator.ENDS_WITH:
            return str(field_value).lower().endswith(str(self.value).lower())
        
        return False


@dataclass
class QueryResult:
    """Result of a query execution."""
    records: List[Any] = field(default_factory=list)
    count: int = 0
    execution_time: float = 0.0
    
    def to_dict(self) -> List[Dict[str, Any]]:
        """Convert records to dictionary format."""
        return [self._record_to_dict(r) for r in self.records]
    
    def _record_to_dict(self, record: Any) -> Dict[str, Any]:
        """Convert a single record to dictionary."""
        if isinstance(record, dict):
            # Handle enriched records
            result = {***REMOVED***
            if "_entity" in record:
                # Flatten the entity attributes
                entity = record["_entity"]
                for attr in dir(entity):
                    if not attr.startswith("_") and not callable(getattr(entity, attr)):
                        result[attr] = getattr(entity, attr)
                
                # Add relations as nested objects
                if "_relations" in record:
                    for rel_table, rel_data in record["_relations"].items():
                        if isinstance(rel_data, list):
                            result[f"{rel_table***REMOVED***_related"] = [
                                self._record_to_dict(r) for r in rel_data
                            ]
                        else:
                            result[f"{rel_table***REMOVED***_related"] = self._record_to_dict(rel_data)
            return result
        else:
            # Handle simple records
            result = {***REMOVED***
            for attr in dir(record):
                if not attr.startswith("_") and not callable(getattr(record, attr)):
                    result[attr] = getattr(record, attr)
            return result


class Query:
    """
    Fluent query builder for Schichtplaner5 data.
    
    Example usage:
        query = Query(loaded_tables)
        result = (query.select("5EMPL")
                      .where("id", "=", 52)
                      .join("5NOTE")
                      .join("5ABSEN")
                      .execute())
    """
    
    def __init__(self, loaded_tables: Dict[str, List[Any]]):
        self.loaded_tables = loaded_tables
        self._from_table: Optional[str] = None
        self._filters: List[Filter] = []
        self._joins: List[str] = []
        self._select_fields: Optional[List[str]] = None
        self._order_by: Optional[Tuple[str, bool]] = None  # (field, ascending)
        self._limit: Optional[int] = None
        self._offset: int = 0
        self._enrich_depth: int = 0
    
    def select(self, table: str, fields: Optional[List[str]] = None) -> "Query":
        """Select data from a table."""
        if table not in self.loaded_tables:
            raise ValueError(f"Table {table***REMOVED*** not loaded")
        self._from_table = table
        self._select_fields = fields
        return self
    
    def where(self, field: str, operator: Union[str, FilterOperator], value: Any = None) -> "Query":
        """Add a filter condition."""
        if isinstance(operator, str):
            operator = FilterOperator(operator)
        self._filters.append(Filter(field, operator, value))
        return self
    
    def where_employee(self, employee_id: int) -> "Query":
        """Convenience method to filter by employee ID."""
        return self.where("employee_id", FilterOperator.EQUALS, employee_id)
    
    def where_date_range(self, field: str, start_date: date, end_date: date) -> "Query":
        """Filter by date range."""
        return (self.where(field, FilterOperator.GREATER_EQUALS, start_date)
                   .where(field, FilterOperator.LESS_EQUALS, end_date))
    
    def join(self, table: str) -> "Query":
        """Join with another table based on defined relationships."""
        if table not in self.loaded_tables:
            raise ValueError(f"Table {table***REMOVED*** not loaded")
        self._joins.append(table)
        return self
    
    def with_relations(self, depth: int = 1) -> "Query":
        """Enrich results with related data up to specified depth."""
        self._enrich_depth = depth
        return self
    
    def order_by(self, field: str, ascending: bool = True) -> "Query":
        """Order results by a field."""
        self._order_by = (field, ascending)
        return self
    
    def limit(self, limit: int) -> "Query":
        """Limit number of results."""
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> "Query":
        """Skip first N results."""
        self._offset = offset
        return self
    
    def execute(self) -> QueryResult:
        """Execute the query and return results."""
        import time
        start_time = time.time()
        
        if not self._from_table:
            raise ValueError("No table selected")
        
        # Start with all records from the main table
        results = self.loaded_tables[self._from_table].copy()
        
        # Apply filters
        for filter_cond in self._filters:
            results = [r for r in results if filter_cond.apply(r)]
        
        # Handle joins
        if self._joins:
            enriched_results = []
            for record in results:
                enriched = self._enrich_with_joins(record, self._from_table)
                enriched_results.append(enriched)
            results = enriched_results
        elif self._enrich_depth > 0:
            # Enrich with all relations if requested
            results = [
                get_entity_with_relations(r, self._from_table, self.loaded_tables, self._enrich_depth)
                for r in results
            ]
        
        # Apply ordering
        if self._order_by:
            field, ascending = self._order_by
            results.sort(
                key=lambda r: self._get_sort_value(r, field),
                reverse=not ascending
            )
        
        # Apply offset and limit
        if self._offset:
            results = results[self._offset:]
        if self._limit:
            results = results[:self._limit]
        
        execution_time = time.time() - start_time
        
        return QueryResult(
            records=results,
            count=len(results),
            execution_time=execution_time
        )
    
    def _enrich_with_joins(self, record: Any, table: str) -> Dict[str, Any]:
        """Enrich a record with joined data."""
        result = {
            "_entity": record,
            "_table": table,
            "_relations": {***REMOVED***
        ***REMOVED***
        
        for join_table in self._joins:
            # Find matching records in join table
            matches = relationship_manager.resolve_reference(
                [record], table, join_table, self.loaded_tables[join_table]
            )
            
            if matches:
                # Extract just the target records
                result["_relations"][join_table] = [match[1] for match in matches]
        
        return result
    
    def _get_sort_value(self, record: Any, field: str) -> Any:
        """Get value for sorting, handling enriched records."""
        if isinstance(record, dict) and "_entity" in record:
            return getattr(record["_entity"], field, None)
        return getattr(record, field, None)


class QueryEngine:
    """
    High-level query engine for Schichtplaner5 data.
    Provides convenience methods for common queries.
    """

    def __init__(self, dbf_dir: Path, verbose: bool = False):
        self.logger = logging.getLogger('libopenschichtplaner5.QueryEngine')
        self.dbf_dir = dbf_dir
        self.loaded_tables = self._load_all_tables()

        # Validate relationships
        errors = relationship_manager.validate_relationships(self.loaded_tables)
        if errors:
            self.logger.warning("Some relationship validations failed:")
            for error in errors:
                self.logger.warning(f"  - {error***REMOVED***")

    def _load_all_tables(self) -> Dict[str, List[Any]]:
        """Load all available tables from DBF directory."""
        tables = {***REMOVED***
        for table_name in TABLE_NAMES:
            dbf_path = self.dbf_dir / f"{table_name***REMOVED***.DBF"
            if dbf_path.exists():
                try:
                    tables[table_name] = load_table(table_name, dbf_path)
                    self.logger.debug(f"Loaded {table_name***REMOVED***: {len(tables[table_name])***REMOVED*** records")
                except Exception as e:
                    self.logger.error(f"Error loading {table_name***REMOVED***: {e***REMOVED***")

        self.logger.info(f"Loaded {len(tables)***REMOVED*** tables total")
        return tables
    
    def query(self) -> Query:
        """Create a new query."""
        return Query(self.loaded_tables)
    
    def get_employee_full_profile(self, employee_id: int) -> Dict[str, Any]:
        """Get complete profile for an employee with all related data."""
        result = (self.query()
                     .select("5EMPL")
                     .where("id", "=", employee_id)
                     .with_relations(depth=2)
                     .execute())
        
        if result.records:
            return result.to_dict()[0]
        return None

    def get_employee_schedule(self, employee_id: int,
                              start_date: Optional[date] = None,
                              end_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """Get employee's schedule for a date range."""
        query = (self.query()
                 .select("5MASHI")  # Mitarbeiterschichten, nicht SPSHI!
                 .where_employee(employee_id))

        if start_date and end_date:
            query = query.where_date_range("date", start_date, end_date)

        query = query.join("5SHIFT").join("5WOPL").order_by("date")

        result = query.execute()
        return result.to_dict()
    
    def get_group_members(self, group_id: int) -> List[Dict[str, Any]]:
        """Get all employees in a group."""
        # First get group assignments
        assignments = (self.query()
                          .select("5GRASG")
                          .where("group_id", "=", group_id)
                          .execute())
        
        # Then get employee details
        employee_ids = [getattr(a, "employee_id") for a in assignments.records]
        
        if employee_ids:
            result = (self.query()
                         .select("5EMPL")
                         .where("id", "in", employee_ids)
                         .order_by("name")
                         .execute())
            return result.to_dict()
        
        return []
    
    def get_absence_summary(self, year: int) -> Dict[str, Any]:
        """Get absence summary for a year."""
        # This would need date filtering implementation
        absences = self.query().select("5ABSEN").join("5EMPL").join("5LEAVT").execute()
        
        # Group by employee and leave type
        summary = {***REMOVED***
        for record in absences.records:
            # Process and aggregate data
            pass  # Implementation depends on specific requirements
        
        return summary
    
    def search_employees(self, search_term: str) -> List[Dict[str, Any]]:
        """Search employees by name or number."""
        result = (self.query()
                     .select("5EMPL")
                     .where("name", "contains", search_term)
                     .execute())
        
        # Also search in firstname
        result2 = (self.query()
                      .select("5EMPL")
                      .where("firstname", "contains", search_term)
                      .execute())
        
        # Combine and deduplicate results
        all_records = result.records + result2.records
        seen = set()
        unique_records = []
        for r in all_records:
            if r.id not in seen:
                seen.add(r.id)
                unique_records.append(r)
        
        result.records = unique_records
        result.count = len(unique_records)
        
        return result.to_dict()
