# libopenschichtplaner5/src/libopenschichtplaner5/utils/helpers.py
"""
Helper utilities for common Schichtplaner5 operations.
Provides simplified interfaces for frequent tasks.
"""

from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import date, datetime, timedelta
from pathlib import Path
from collections import defaultdict, Counter
import calendar

from ..query_engine import QueryEngine
from ..relationships import get_entity_with_relations
from ..models.employee import Employee
from ..models.shift import Shift
from ..models.group import Group


class ScheduleHelper:
    """Helper functions for working with schedules."""
    
    @staticmethod
    def get_week_schedule(engine: QueryEngine, 
                         employee_id: int, 
                         week_date: date) -> List[Dict[str, Any]]:
        """
        Get complete week schedule for an employee.
        
        Args:
            engine: Query engine instance
            employee_id: Employee ID
            week_date: Any date in the target week
            
        Returns:
            List of schedule entries for the week
        """
        # Calculate week boundaries
        weekday = week_date.weekday()
        week_start = week_date - timedelta(days=weekday)
        week_end = week_start + timedelta(days=6)
        
        # Get schedule
        schedule = engine.get_employee_schedule(employee_id, week_start, week_end)
        
        # Fill in missing days
        scheduled_dates = {entry['date'] for entry in schedule if 'date' in entry***REMOVED***
        full_schedule = []
        
        for i in range(7):
            current_date = week_start + timedelta(days=i)
            day_entry = next((s for s in schedule if s.get('date') == current_date), None)
            
            if day_entry:
                full_schedule.append(day_entry)
            else:
                # Add empty entry for unscheduled day
                full_schedule.append({
                    'date': current_date,
                    'employee_id': employee_id,
                    'shift': None,
                    'is_free': True
                ***REMOVED***)
        
        return full_schedule
    
    @staticmethod
    def get_monthly_summary(engine: QueryEngine, 
                          employee_id: int,
                          year: int,
                          month: int) -> Dict[str, Any]:
        """
        Get monthly summary for an employee.
        
        Returns dict with:
        - total_shifts
        - shifts_by_type
        - weekend_shifts
        - night_shifts (if identifiable)
        - absences
        """
        start_date = date(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        
        # Get shifts
        shifts = (engine.query()
                 .select("5SPSHI")
                 .where_employee(employee_id)
                 .where_date_range("date", start_date, end_date)
                 .join("5SHIFT")
                 .execute())
        
        # Get absences
        absences = (engine.query()
                   .select("5ABSEN")
                   .where_employee(employee_id)
                   .where_date_range("date", start_date, end_date)
                   .join("5LEAVT")
                   .execute())
        
        # Process data
        shift_types = Counter()
        weekend_shifts = 0
        night_shifts = 0
        
        for shift_record in shifts.records:
            if isinstance(shift_record, dict):
                shift = shift_record["_entity"]
                shift_info = shift_record["_relations"].get("5SHIFT", [])
                shift_name = shift_info[0].name if shift_info else "Unknown"
                
                # Check for night shifts (simple heuristic)
                if shift_info and hasattr(shift_info[0], 'startend'):
                    if any(indicator in shift_info[0].startend.lower() 
                          for indicator in ['night', 'nacht', '22:', '23:', '00:', '01:']):
                        night_shifts += 1
            else:
                shift = shift_record
                shift_name = f"Shift {shift.shift_id***REMOVED***"
            
            shift_types[shift_name] += 1
            
            # Check weekend
            if hasattr(shift, 'date') and shift.date:
                if shift.date.weekday() >= 5:
                    weekend_shifts += 1
        
        # Process absences
        absence_days = len(absences.records)
        absence_types = Counter()
        
        for absence_record in absences.records:
            if isinstance(absence_record, dict):
                leave_type = absence_record["_relations"].get("5LEAVT", [])
                type_name = leave_type[0].name if leave_type else "Unknown"
            else:
                type_name = "Unknown"
            absence_types[type_name] += 1
        
        return {
            'year': year,
            'month': month,
            'employee_id': employee_id,
            'total_shifts': len(shifts.records),
            'shifts_by_type': dict(shift_types),
            'weekend_shifts': weekend_shifts,
            'night_shifts': night_shifts,
            'absence_days': absence_days,
            'absence_types': dict(absence_types),
            'working_days': len(shifts.records) + absence_days
        ***REMOVED***


class EmployeeHelper:
    """Helper functions for employee operations."""
    
    @staticmethod
    def find_employees_by_group(engine: QueryEngine, 
                              group_name: str) -> List[Employee]:
        """Find all employees in a group by group name."""
        # Find group
        groups = (engine.query()
                 .select("5GROUP")
                 .where("name", "contains", group_name)
                 .execute())
        
        if not groups.records:
            return []
        
        # Get members of all matching groups
        all_employees = []
        for group in groups.records:
            members = engine.get_group_members(group.id)
            # Convert dict results back to Employee objects if needed
            for member_dict in members:
                emp = (engine.query()
                      .select("5EMPL")
                      .where("id", "=", member_dict['id'])
                      .execute())
                if emp.records:
                    all_employees.append(emp.records[0])
        
        return all_employees
    
    @staticmethod
    def get_employee_groups(engine: QueryEngine, 
                          employee_id: int) -> List[Group]:
        """Get all groups an employee belongs to."""
        assignments = (engine.query()
                      .select("5GRASG")
                      .where_employee(employee_id)
                      .join("5GROUP")
                      .execute())
        
        groups = []
        for assignment in assignments.records:
            if isinstance(assignment, dict):
                group_data = assignment["_relations"].get("5GROUP", [])
                if group_data:
                    groups.extend(group_data)
        
        return groups
    
    @staticmethod
    def find_colleagues(engine: QueryEngine, 
                       employee_id: int) -> List[Dict[str, Any]]:
        """Find all colleagues (employees in same groups)."""
        # Get employee's groups
        employee_groups = EmployeeHelper.get_employee_groups(engine, employee_id)
        
        # Get all members of these groups
        colleagues = set()
        for group in employee_groups:
            members = engine.get_group_members(group.id)
            for member in members:
                if member['id'] != employee_id:
                    colleagues.add(member['id'])
        
        # Get employee details
        if colleagues:
            result = (engine.query()
                     .select("5EMPL")
                     .where("id", "in", list(colleagues))
                     .order_by("name")
                     .execute())
            return result.to_dict()
        
        return []


class ShiftAnalyzer:
    """Analyze shift patterns and statistics."""
    
    @staticmethod
    def get_shift_statistics(engine: QueryEngine,
                           start_date: date,
                           end_date: date,
                           group_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get comprehensive shift statistics for a period.
        
        Returns statistics including:
        - Most/least common shifts
        - Peak days
        - Coverage analysis
        """
        # Build query
        query = engine.query().select("5SPSHI").where_date_range("date", start_date, end_date)
        
        if group_id:
            members = engine.get_group_members(group_id)
            member_ids = [m['id'] for m in members]
            query = query.where("employee_id", "in", member_ids)
        
        shifts = query.join("5SHIFT").execute()
        
        # Analyze data
        shifts_by_date = defaultdict(list)
        shifts_by_type = Counter()
        shifts_by_weekday = defaultdict(Counter)
        employee_shift_counts = Counter()
        
        for shift_record in shifts.records:
            if isinstance(shift_record, dict):
                shift = shift_record["_entity"]
                shift_info = shift_record["_relations"].get("5SHIFT", [])
                shift_name = shift_info[0].name if shift_info else "Unknown"
            else:
                shift = shift_record
                shift_name = f"Shift {shift.shift_id***REMOVED***"
            
            if hasattr(shift, 'date') and shift.date:
                shifts_by_date[shift.date].append(shift)
                weekday = shift.date.strftime("%A")
                shifts_by_weekday[weekday][shift_name] += 1
            
            shifts_by_type[shift_name] += 1
            employee_shift_counts[shift.employee_id] += 1
        
        # Calculate statistics
        total_days = (end_date - start_date).days + 1
        dates_with_shifts = len(shifts_by_date)
        coverage_percentage = (dates_with_shifts / total_days * 100) if total_days > 0 else 0
        
        # Find peak days
        peak_days = sorted(
            [(date, len(shifts)) for date, shifts in shifts_by_date.items()],
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        # Average shifts per day
        avg_shifts_per_day = len(shifts.records) / total_days if total_days > 0 else 0
        
        return {
            'period': {
                'start': str(start_date),
                'end': str(end_date),
                'total_days': total_days
            ***REMOVED***,
            'summary': {
                'total_shifts': len(shifts.records),
                'unique_employees': len(employee_shift_counts),
                'coverage_percentage': round(coverage_percentage, 2),
                'avg_shifts_per_day': round(avg_shifts_per_day, 2)
            ***REMOVED***,
            'shift_types': dict(shifts_by_type.most_common()),
            'most_common_shift': shifts_by_type.most_common(1)[0] if shifts_by_type else None,
            'least_common_shift': shifts_by_type.most_common()[-1] if shifts_by_type else None,
            'peak_days': [
                {'date': str(date), 'shift_count': count***REMOVED***
                for date, count in peak_days
            ],
            'weekday_distribution': {
                day: dict(shifts) for day, shifts in shifts_by_weekday.items()
            ***REMOVED***,
            'busiest_employees': [
                {'employee_id': emp_id, 'shift_count': count***REMOVED***
                for emp_id, count in employee_shift_counts.most_common(10)
            ]
        ***REMOVED***
    
    @staticmethod
    def find_coverage_gaps(engine: QueryEngine,
                         group_id: int,
                         start_date: date,
                         end_date: date,
                         min_coverage: int = 1) -> List[Dict[str, Any]]:
        """
        Find dates where coverage is below minimum.
        
        Args:
            engine: Query engine
            group_id: Group to check
            start_date: Start of period
            end_date: End of period
            min_coverage: Minimum required employees per day
            
        Returns:
            List of dates with insufficient coverage
        """
        members = engine.get_group_members(group_id)
        member_ids = [m['id'] for m in members]
        
        # Get all shifts for the period
        shifts = (engine.query()
                 .select("5SPSHI")
                 .where("employee_id", "in", member_ids)
                 .where_date_range("date", start_date, end_date)
                 .execute())
        
        # Count coverage by date
        coverage_by_date = defaultdict(set)
        for shift in shifts.records:
            if hasattr(shift, 'date') and shift.date:
                coverage_by_date[shift.date].add(shift.employee_id)
        
        # Find gaps
        gaps = []
        current_date = start_date
        while current_date <= end_date:
            coverage = len(coverage_by_date[current_date])
            if coverage < min_coverage:
                gaps.append({
                    'date': current_date,
                    'coverage': coverage,
                    'shortage': min_coverage - coverage,
                    'working_employees': list(coverage_by_date[current_date])
                ***REMOVED***)
            current_date += timedelta(days=1)
        
        return gaps


class QuickStats:
    """Quick statistics and counts."""
    
    @staticmethod
    def get_database_summary(engine: QueryEngine) -> Dict[str, Any]:
        """Get summary of entire database."""
        summary = {
            'tables': {***REMOVED***,
            'totals': {
                'employees': 0,
                'groups': 0,
                'shifts': 0,
                'work_locations': 0
            ***REMOVED***
        ***REMOVED***
        
        # Count records in each table
        for table_name, records in engine.loaded_tables.items():
            summary['tables'][table_name] = {
                'record_count': len(records),
                'has_data': len(records) > 0
            ***REMOVED***
        
        # Get key totals
        if '5EMPL' in engine.loaded_tables:
            summary['totals']['employees'] = len(engine.loaded_tables['5EMPL'])
        if '5GROUP' in engine.loaded_tables:
            summary['totals']['groups'] = len(engine.loaded_tables['5GROUP'])
        if '5SHIFT' in engine.loaded_tables:
            summary['totals']['shifts'] = len(engine.loaded_tables['5SHIFT'])
        if '5WOPL' in engine.loaded_tables:
            summary['totals']['work_locations'] = len(engine.loaded_tables['5WOPL'])
        
        # Get date ranges
        if '5SPSHI' in engine.loaded_tables and engine.loaded_tables['5SPSHI']:
            dates = [s.date for s in engine.loaded_tables['5SPSHI'] 
                    if hasattr(s, 'date') and s.date]
            if dates:
                summary['date_range'] = {
                    'earliest': str(min(dates)),
                    'latest': str(max(dates))
                ***REMOVED***
        
        return summary
    
    @staticmethod
    def get_active_employees(engine: QueryEngine, 
                           reference_date: Optional[date] = None) -> List[Employee]:
        """Get currently active employees."""
        if not reference_date:
            reference_date = date.today()
        
        employees = engine.query().select("5EMPL").execute()
        
        active = []
        for emp in employees.records:
            # Check employment dates
            is_active = True
            
            if hasattr(emp, 'empstart') and emp.empstart:
                if isinstance(emp.empstart, str):
                    try:
                        empstart = datetime.strptime(emp.empstart, "%Y-%m-%d").date()
                        if empstart > reference_date:
                            is_active = False
                    except:
                        pass
                elif isinstance(emp.empstart, date) and emp.empstart > reference_date:
                    is_active = False
            
            if hasattr(emp, 'empend') and emp.empend:
                if isinstance(emp.empend, str):
                    try:
                        empend = datetime.strptime(emp.empend, "%Y-%m-%d").date()
                        if empend < reference_date:
                            is_active = False
                    except:
                        pass
                elif isinstance(emp.empend, date) and emp.empend < reference_date:
                    is_active = False
            
            if is_active:
                active.append(emp)
        
        return active
