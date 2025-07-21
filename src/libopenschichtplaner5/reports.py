# src/libopenschichtplaner5/reports.py
"""
Report generation module for Schichtplaner5 data.
Provides various pre-configured reports and analysis tools.
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import date, datetime, timedelta
from collections import defaultdict, Counter
from dataclasses import dataclass
from pathlib import Path

from .query_engine import QueryEngine
from .relationships import relationship_manager


@dataclass
class ReportResult:
    """Container for report results."""
    title: str
    data: Any
    metadata: Dict[str, Any]
    generated_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "title": self.title,
            "data": self.data,
            "metadata": self.metadata,
            "generated_at": self.generated_at.isoformat()
        ***REMOVED***


class ReportGenerator:
    """Generates various reports from Schichtplaner5 data."""
    
    def __init__(self, engine: QueryEngine):
        self.engine = engine
    
    def employee_absence_report(self, employee_id: int, year: int) -> ReportResult:
        """
        Generate absence report for an employee in a specific year.
        Shows all absences grouped by leave type with totals.
        """
        # Get employee info
        employee = (self.engine.query()
                   .select("5EMPL")
                   .where("id", "=", employee_id)
                   .execute())
        
        if not employee.records:
            raise ValueError(f"Employee {employee_id***REMOVED*** not found")
        
        emp_info = employee.records[0]
        
        # Get absences for the year
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        
        absences = (self.engine.query()
                   .select("5ABSEN")
                   .where_employee(employee_id)
                   .where_date_range("date", start_date, end_date)
                   .join("5LEAVT")
                   .execute())
        
        # Get leave entitlements
        entitlements = (self.engine.query()
                       .select("5LEAEN")
                       .where_employee(employee_id)
                       .where("year", "=", year)
                       .join("5LEAVT")
                       .execute())
        
        # Process absences by leave type
        absence_summary = defaultdict(lambda: {"count": 0, "days": 0.0, "dates": []***REMOVED***)
        
        for absence_record in absences.records:
            if isinstance(absence_record, dict):
                absence = absence_record["_entity"]
                leave_type_data = absence_record["_relations"].get("5LEAVT", [])
                leave_type_name = leave_type_data[0].name if leave_type_data else "Unknown"
            else:
                absence = absence_record
                leave_type_name = f"Type {absence.leave_type_id***REMOVED***"
            
            absence_summary[leave_type_name]["count"] += 1
            # Calculate days (simplified - assumes full day absences)
            absence_summary[leave_type_name]["days"] += 1.0
            absence_summary[leave_type_name]["dates"].append(str(absence.date))
        
        # Process entitlements
        entitlement_summary = {***REMOVED***
        for ent_record in entitlements.records:
            if isinstance(ent_record, dict):
                ent = ent_record["_entity"]
                leave_type_data = ent_record["_relations"].get("5LEAVT", [])
                leave_type_name = leave_type_data[0].name if leave_type_data else "Unknown"
            else:
                ent = ent_record
                leave_type_name = f"Type {ent.leave_type_id***REMOVED***"
            
            entitlement_summary[leave_type_name] = {
                "entitled": ent.entitlement,
                "rest": ent.rest,
                "in_days": ent.in_days
            ***REMOVED***
        
        # Combine data
        report_data = {
            "employee": {
                "id": emp_info.id,
                "name": f"{emp_info.name***REMOVED*** {emp_info.firstname***REMOVED***",
                "position": emp_info.position
            ***REMOVED***,
            "year": year,
            "absences": dict(absence_summary),
            "entitlements": entitlement_summary,
            "summary": {
                "total_absence_days": sum(a["days"] for a in absence_summary.values()),
                "leave_types_used": len(absence_summary)
            ***REMOVED***
        ***REMOVED***
        
        return ReportResult(
            title=f"Absence Report - {emp_info.name***REMOVED*** {emp_info.firstname***REMOVED*** ({year***REMOVED***)",
            data=report_data,
            metadata={"employee_id": employee_id, "year": year***REMOVED***,
            generated_at=datetime.now()
        )
    
    def group_staffing_report(self, group_id: int, 
                            target_date: Optional[date] = None) -> ReportResult:
        """
        Generate staffing report for a group on a specific date.
        Shows who is working, absent, and staffing levels.
        """
        if not target_date:
            target_date = date.today()
        
        # Get group info
        group = (self.engine.query()
                .select("5GROUP")
                .where("id", "=", group_id)
                .execute())
        
        if not group.records:
            raise ValueError(f"Group {group_id***REMOVED*** not found")
        
        group_info = group.records[0]
        
        # Get group members
        members = self.engine.get_group_members(group_id)
        member_ids = [m["id"] for m in members]
        
        # Get shifts for the date
        shifts = (self.engine.query()
                 .select("5SPSHI")
                 .where("employee_id", "in", member_ids)
                 .where("date", "=", target_date)
                 .join("5SHIFT")
                 .join("5WOPL")
                 .execute())
        
        # Get absences for the date
        absences = (self.engine.query()
                   .select("5ABSEN")
                   .where("employee_id", "in", member_ids)
                   .where("date", "=", target_date)
                   .join("5LEAVT")
                   .execute())
        
        # Process data
        working = []
        absent = []
        absent_ids = set()
        
        # Process absences first
        for absence_record in absences.records:
            if isinstance(absence_record, dict):
                absence = absence_record["_entity"]
                leave_type_data = absence_record["_relations"].get("5LEAVT", [])
                leave_type = leave_type_data[0].name if leave_type_data else "Unknown"
            else:
                absence = absence_record
                leave_type = f"Type {absence.leave_type_id***REMOVED***"
            
            emp_info = next((m for m in members if m["id"] == absence.employee_id), None)
            if emp_info:
                absent.append({
                    "employee": f"{emp_info['name']***REMOVED*** {emp_info['firstname']***REMOVED***",
                    "employee_id": absence.employee_id,
                    "reason": leave_type
                ***REMOVED***)
                absent_ids.add(absence.employee_id)
        
        # Process shifts
        shift_by_type = defaultdict(list)
        for shift_record in shifts.records:
            if isinstance(shift_record, dict):
                shift = shift_record["_entity"]
                shift_data = shift_record["_relations"].get("5SHIFT", [])
                workplace_data = shift_record["_relations"].get("5WOPL", [])
                
                shift_name = shift_data[0].name if shift_data else "Unknown Shift"
                workplace = workplace_data[0].name if workplace_data else "Unknown Location"
            else:
                shift = shift_record
                shift_name = f"Shift {shift.shift_id***REMOVED***"
                workplace = f"Location {shift.workplace_id***REMOVED***"
            
            if shift.employee_id not in absent_ids:
                emp_info = next((m for m in members if m["id"] == shift.employee_id), None)
                if emp_info:
                    working.append({
                        "employee": f"{emp_info['name']***REMOVED*** {emp_info['firstname']***REMOVED***",
                        "employee_id": shift.employee_id,
                        "shift": shift_name,
                        "workplace": workplace
                    ***REMOVED***)
                    shift_by_type[shift_name].append(emp_info)
        
        # Calculate staffing summary
        total_members = len(members)
        working_count = len(working)
        absent_count = len(absent)
        not_scheduled = total_members - working_count - absent_count
        
        report_data = {
            "group": {
                "id": group_info.id,
                "name": group_info.name,
                "shortname": group_info.shortname
            ***REMOVED***,
            "date": str(target_date),
            "staffing": {
                "total_members": total_members,
                "working": working_count,
                "absent": absent_count,
                "not_scheduled": not_scheduled,
                "staffing_percentage": (working_count / total_members * 100) if total_members > 0 else 0
            ***REMOVED***,
            "working_employees": working,
            "absent_employees": absent,
            "shifts_coverage": dict(shift_by_type)
        ***REMOVED***
        
        return ReportResult(
            title=f"Staffing Report - {group_info.name***REMOVED*** ({target_date***REMOVED***)",
            data=report_data,
            metadata={"group_id": group_id, "date": str(target_date)***REMOVED***,
            generated_at=datetime.now()
        )
    
    def shift_distribution_report(self, start_date: date, end_date: date,
                                group_id: Optional[int] = None) -> ReportResult:
        """
        Analyze shift distribution over a period.
        Shows which shifts are used most, distribution by weekday, etc.
        """
        # Build base query
        query = self.engine.query().select("5SPSHI").where_date_range("date", start_date, end_date)
        
        # Filter by group if specified
        if group_id:
            members = self.engine.get_group_members(group_id)
            member_ids = [m["id"] for m in members]
            query = query.where("employee_id", "in", member_ids)
        
        shifts = query.join("5SHIFT").execute()
        
        # Analyze distribution
        shift_counts = Counter()
        weekday_distribution = defaultdict(Counter)
        employee_shift_counts = defaultdict(Counter)
        
        for shift_record in shifts.records:
            if isinstance(shift_record, dict):
                shift = shift_record["_entity"]
                shift_data = shift_record["_relations"].get("5SHIFT", [])
                shift_name = shift_data[0].name if shift_data else f"Shift {shift.shift_id***REMOVED***"
            else:
                shift = shift_record
                shift_name = f"Shift {shift.shift_id***REMOVED***"
            
            # Count shifts
            shift_counts[shift_name] += 1
            
            # Weekday distribution
            if hasattr(shift, 'date') and shift.date:
                weekday = shift.date.strftime("%A")
                weekday_distribution[weekday][shift_name] += 1
            
            # Employee distribution
            employee_shift_counts[shift.employee_id][shift_name] += 1
        
        # Calculate statistics
        total_shifts = sum(shift_counts.values())
        
        report_data = {
            "period": {
                "start": str(start_date),
                "end": str(end_date),
                "days": (end_date - start_date).days + 1
            ***REMOVED***,
            "total_shifts": total_shifts,
            "shift_types": dict(shift_counts),
            "shift_percentages": {
                shift: (count / total_shifts * 100) if total_shifts > 0 else 0
                for shift, count in shift_counts.items()
            ***REMOVED***,
            "weekday_distribution": {
                day: dict(shifts) for day, shifts in weekday_distribution.items()
            ***REMOVED***,
            "most_common_shift": shift_counts.most_common(1)[0] if shift_counts else None,
            "unique_employees": len(employee_shift_counts)
        ***REMOVED***
        
        if group_id:
            report_data["group_id"] = group_id
        
        return ReportResult(
            title=f"Shift Distribution Report ({start_date***REMOVED*** to {end_date***REMOVED***)",
            data=report_data,
            metadata={
                "start_date": str(start_date),
                "end_date": str(end_date),
                "group_id": group_id
            ***REMOVED***,
            generated_at=datetime.now()
        )
    
    def overtime_analysis_report(self, employee_id: Optional[int] = None,
                               month: Optional[int] = None,
                               year: Optional[int] = None) -> ReportResult:
        """
        Analyze overtime bookings for employees.
        """
        if not year:
            year = datetime.now().year
        if not month:
            month = datetime.now().month
        
        # Calculate date range
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)
        
        # Query bookings
        query = self.engine.query().select("5BOOK").where_date_range("date", start_date, end_date)
        
        if employee_id:
            query = query.where_employee(employee_id)
        
        bookings = query.execute()
        
        # Analyze bookings
        employee_totals = defaultdict(lambda: {"positive": 0.0, "negative": 0.0, "net": 0.0***REMOVED***)
        daily_totals = defaultdict(float)
        booking_types = Counter()
        
        for booking in bookings.records:
            emp_id = booking.employee_id
            value = float(booking.value)
            
            if value > 0:
                employee_totals[emp_id]["positive"] += value
            else:
                employee_totals[emp_id]["negative"] += value
            
            employee_totals[emp_id]["net"] += value
            
            if hasattr(booking, 'date') and booking.date:
                daily_totals[str(booking.date)] += value
            
            booking_types[booking.type] += 1
        
        # Get employee names
        if employee_totals:
            emp_ids = list(employee_totals.keys())
            employees = (self.engine.query()
                        .select("5EMPL")
                        .where("id", "in", emp_ids)
                        .execute())
            
            emp_names = {
                e.id: f"{e.name***REMOVED*** {e.firstname***REMOVED***" 
                for e in employees.records
            ***REMOVED***
        else:
            emp_names = {***REMOVED***
        
        # Format results
        employee_summary = []
        for emp_id, totals in employee_totals.items():
            employee_summary.append({
                "employee_id": emp_id,
                "employee_name": emp_names.get(emp_id, f"Employee {emp_id***REMOVED***"),
                "overtime_hours": totals["positive"],
                "deductions": abs(totals["negative"]),
                "net_balance": totals["net"]
            ***REMOVED***)
        
        # Sort by net balance
        employee_summary.sort(key=lambda x: x["net_balance"], reverse=True)
        
        report_data = {
            "period": {
                "month": month,
                "year": year,
                "start_date": str(start_date),
                "end_date": str(end_date)
            ***REMOVED***,
            "summary": {
                "total_employees": len(employee_totals),
                "total_overtime": sum(e["overtime_hours"] for e in employee_summary),
                "total_deductions": sum(e["deductions"] for e in employee_summary),
                "net_total": sum(e["net_balance"] for e in employee_summary)
            ***REMOVED***,
            "employee_details": employee_summary,
            "daily_totals": dict(daily_totals),
            "booking_types": dict(booking_types)
        ***REMOVED***
        
        title = f"Overtime Analysis - {month***REMOVED***/{year***REMOVED***"
        if employee_id:
            title += f" - Employee {employee_id***REMOVED***"
        
        return ReportResult(
            title=title,
            data=report_data,
            metadata={
                "month": month,
                "year": year,
                "employee_id": employee_id
            ***REMOVED***,
            generated_at=datetime.now()
        )
    
    def cycle_assignment_report(self) -> ReportResult:
        """
        Overview of all cycle assignments currently active.
        """
        today = date.today()
        
        # Get active cycle assignments
        assignments = (self.engine.query()
                      .select("5CYASS")
                      .where("start", "<=", today)
                      .where("end", ">=", today)
                      .join("5CYCLE")
                      .join("5EMPL")
                      .execute())
        
        # Process assignments
        cycle_summary = defaultdict(list)
        employee_cycles = defaultdict(list)
        
        for assignment_record in assignments.records:
            if isinstance(assignment_record, dict):
                assignment = assignment_record["_entity"]
                cycle_data = assignment_record["_relations"].get("5CYCLE", [])
                employee_data = assignment_record["_relations"].get("5EMPL", [])
                
                cycle_name = cycle_data[0].name if cycle_data else f"Cycle {assignment.cycle_id***REMOVED***"
                employee_name = (f"{employee_data[0].name***REMOVED*** {employee_data[0].firstname***REMOVED***" 
                               if employee_data else f"Employee {assignment.employee_id***REMOVED***")
            else:
                assignment = assignment_record
                cycle_name = f"Cycle {assignment.cycle_id***REMOVED***"
                employee_name = f"Employee {assignment.employee_id***REMOVED***"
            
            cycle_info = {
                "employee_id": assignment.employee_id,
                "employee_name": employee_name,
                "start": str(assignment.start),
                "end": str(assignment.end),
                "entrance": assignment.entrance
            ***REMOVED***
            
            cycle_summary[cycle_name].append(cycle_info)
            employee_cycles[employee_name].append({
                "cycle": cycle_name,
                "start": str(assignment.start),
                "end": str(assignment.end)
            ***REMOVED***)
        
        report_data = {
            "as_of_date": str(today),
            "active_cycles": len(cycle_summary),
            "total_assignments": sum(len(emps) for emps in cycle_summary.values()),
            "cycles": dict(cycle_summary),
            "employees_with_cycles": len(employee_cycles),
            "cycle_coverage": {
                cycle: len(employees) for cycle, employees in cycle_summary.items()
            ***REMOVED***
        ***REMOVED***
        
        return ReportResult(
            title=f"Active Cycle Assignments Report",
            data=report_data,
            metadata={"as_of_date": str(today)***REMOVED***,
            generated_at=datetime.now()
        )
