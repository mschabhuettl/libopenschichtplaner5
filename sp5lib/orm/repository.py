"""
Repository pattern for database-agnostic data access.

These repositories encapsulate all SQL queries behind a clean Python API.
The same code works on SQLite and PostgreSQL — switching backends only
requires changing the connection URL passed to get_engine().

This is the key benefit of the SQLAlchemy ORM: application code never
writes raw SQL, so a database migration is a config change, not a rewrite.
"""


from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import (
    Absence,
    Employee,
    Group,
    GroupAssignment,
    LeaveType,
    Shift,
    ShiftAssignment,
    SpecialShift,
    Workplace,
)


class EmployeeRepository:
    """Data access for Employee entities."""

    def __init__(self, session: Session):
        self.session = session

    def get_all(self, include_hidden: bool = False) -> list[Employee]:
        """Return all employees, ordered by position."""
        stmt = select(Employee).order_by(Employee.position)
        if not include_hidden:
            stmt = stmt.where(Employee.hide == False)  # noqa: E712
        return list(self.session.scalars(stmt).all())

    def get_by_id(self, emp_id: int) -> Employee | None:
        """Return a single employee by ID, or None."""
        return self.session.get(Employee, emp_id)

    def create(self, **kwargs) -> Employee:
        """Create and persist a new employee."""
        emp = Employee(**kwargs)
        self.session.add(emp)
        self.session.flush()  # Assign ID without committing
        return emp

    def update(self, emp_id: int, **kwargs) -> Employee | None:
        """Update an employee by ID. Returns the updated employee or None."""
        emp = self.get_by_id(emp_id)
        if emp is None:
            return None
        for key, value in kwargs.items():
            if hasattr(emp, key):
                setattr(emp, key, value)
        self.session.flush()
        return emp

    def soft_delete(self, emp_id: int) -> bool:
        """Soft-delete an employee (set hide=True). Returns True if found."""
        emp = self.get_by_id(emp_id)
        if emp is None:
            return False
        emp.hide = True
        self.session.flush()
        return True

    def search(self, query: str, include_hidden: bool = False) -> list[Employee]:
        """Search employees by name or shortname (case-insensitive)."""
        pattern = f"%{query}%"
        stmt = (
            select(Employee)
            .where(
                (Employee.name.ilike(pattern))
                | (Employee.firstname.ilike(pattern))
                | (Employee.shortname.ilike(pattern))
            )
            .order_by(Employee.position)
        )
        if not include_hidden:
            stmt = stmt.where(Employee.hide == False)  # noqa: E712
        return list(self.session.scalars(stmt).all())

    def count(self, include_hidden: bool = False) -> int:
        """Return the total number of employees."""
        stmt = select(Employee)
        if not include_hidden:
            stmt = stmt.where(Employee.hide == False)  # noqa: E712
        return len(list(self.session.scalars(stmt).all()))


class GroupRepository:
    """Data access for Group entities."""

    def __init__(self, session: Session):
        self.session = session

    def get_all(self, include_hidden: bool = False) -> list[Group]:
        """Return all groups, ordered by position."""
        stmt = select(Group).order_by(Group.position)
        if not include_hidden:
            stmt = stmt.where(Group.hide == False)  # noqa: E712
        return list(self.session.scalars(stmt).all())

    def get_by_id(self, group_id: int) -> Group | None:
        """Return a single group by ID, or None."""
        return self.session.get(Group, group_id)

    def create(self, **kwargs) -> Group:
        """Create and persist a new group."""
        group = Group(**kwargs)
        self.session.add(group)
        self.session.flush()
        return group

    def update(self, group_id: int, **kwargs) -> Group | None:
        """Update a group by ID. Returns the updated group or None."""
        group = self.get_by_id(group_id)
        if group is None:
            return None
        for key, value in kwargs.items():
            if hasattr(group, key):
                setattr(group, key, value)
        self.session.flush()
        return group

    def soft_delete(self, group_id: int) -> bool:
        """Soft-delete a group (set hide=True). Returns True if found."""
        group = self.get_by_id(group_id)
        if group is None:
            return False
        group.hide = True
        self.session.flush()
        return True

    def get_members(self, group_id: int) -> list[Employee]:
        """Return all employees in a group."""
        stmt = (
            select(Employee)
            .join(GroupAssignment, GroupAssignment.employee_id == Employee.id)
            .where(GroupAssignment.group_id == group_id)
            .order_by(Employee.position)
        )
        return list(self.session.scalars(stmt).all())

    def get_member_ids(self, group_id: int) -> list[int]:
        """Return employee IDs in a group."""
        stmt = select(GroupAssignment.employee_id).where(
            GroupAssignment.group_id == group_id
        )
        return list(self.session.scalars(stmt).all())

    def add_member(self, group_id: int, employee_id: int) -> GroupAssignment:
        """Add an employee to a group. Idempotent — returns existing if already assigned."""
        existing = self.session.scalars(
            select(GroupAssignment).where(
                GroupAssignment.group_id == group_id,
                GroupAssignment.employee_id == employee_id,
            )
        ).first()
        if existing:
            return existing
        assignment = GroupAssignment(group_id=group_id, employee_id=employee_id)
        self.session.add(assignment)
        self.session.flush()
        return assignment

    def remove_member(self, group_id: int, employee_id: int) -> bool:
        """Remove an employee from a group. Returns True if found and removed."""
        assignment = self.session.scalars(
            select(GroupAssignment).where(
                GroupAssignment.group_id == group_id,
                GroupAssignment.employee_id == employee_id,
            )
        ).first()
        if assignment is None:
            return False
        self.session.delete(assignment)
        self.session.flush()
        return True

    def get_employee_groups(self, employee_id: int) -> list[Group]:
        """Return all groups an employee belongs to."""
        stmt = (
            select(Group)
            .join(GroupAssignment, GroupAssignment.group_id == Group.id)
            .where(GroupAssignment.employee_id == employee_id)
            .order_by(Group.position)
        )
        return list(self.session.scalars(stmt).all())


class ShiftRepository:
    """Data access for Shift definitions (5SHIFT.DBF)."""

    def __init__(self, session: Session):
        self.session = session

    def list(self, include_hidden: bool = False) -> list[Shift]:
        """Return all shifts, ordered by position."""
        stmt = select(Shift).order_by(Shift.position)
        if not include_hidden:
            stmt = stmt.where(Shift.hide == False)  # noqa: E712
        return list(self.session.scalars(stmt).all())

    def get(self, shift_id: int) -> Shift | None:
        """Return a single shift by ID, or None."""
        return self.session.get(Shift, shift_id)


class LeaveTypeRepository:
    """Data access for LeaveType definitions (5LEAVT.DBF)."""

    def __init__(self, session: Session):
        self.session = session

    def list(self, include_hidden: bool = False) -> list[LeaveType]:
        """Return all leave types, ordered by position."""
        stmt = select(LeaveType).order_by(LeaveType.position)
        if not include_hidden:
            stmt = stmt.where(LeaveType.hide == False)  # noqa: E712
        return list(self.session.scalars(stmt).all())

    def get(self, leave_type_id: int) -> LeaveType | None:
        """Return a single leave type by ID, or None."""
        return self.session.get(LeaveType, leave_type_id)


class WorkplaceRepository:
    """Data access for Workplace definitions (5WOPL.DBF)."""

    def __init__(self, session: Session):
        self.session = session

    def list(self, include_hidden: bool = False) -> list[Workplace]:
        """Return all workplaces, ordered by position."""
        stmt = select(Workplace).order_by(Workplace.position)
        if not include_hidden:
            stmt = stmt.where(Workplace.hide == False)  # noqa: E712
        return list(self.session.scalars(stmt).all())

    def get(self, workplace_id: int) -> Workplace | None:
        """Return a single workplace by ID, or None."""
        return self.session.get(Workplace, workplace_id)


class ShiftAssignmentRepository:
    """Data access for regular schedule entries (5MASHI.DBF)."""

    def __init__(self, session: Session):
        self.session = session

    def list(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        employee_id: int | None = None,
    ) -> list[ShiftAssignment]:
        """Return schedule entries filtered by ISO date range and/or employee."""
        stmt = select(ShiftAssignment)
        if date_from is not None:
            stmt = stmt.where(ShiftAssignment.date >= date_from)
        if date_to is not None:
            stmt = stmt.where(ShiftAssignment.date <= date_to)
        if employee_id is not None:
            stmt = stmt.where(ShiftAssignment.employee_id == employee_id)
        stmt = stmt.order_by(ShiftAssignment.date, ShiftAssignment.id)
        return list(self.session.scalars(stmt).all())

    def get(self, entry_id: int) -> ShiftAssignment | None:
        """Return a single schedule entry by ID, or None."""
        return self.session.get(ShiftAssignment, entry_id)


class SpecialShiftRepository:
    """Data access for special / one-off shifts (5SPSHI.DBF)."""

    def __init__(self, session: Session):
        self.session = session

    def list(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        employee_id: int | None = None,
    ) -> list[SpecialShift]:
        """Return special shifts filtered by ISO date range and/or employee."""
        stmt = select(SpecialShift)
        if date_from is not None:
            stmt = stmt.where(SpecialShift.date >= date_from)
        if date_to is not None:
            stmt = stmt.where(SpecialShift.date <= date_to)
        if employee_id is not None:
            stmt = stmt.where(SpecialShift.employee_id == employee_id)
        stmt = stmt.order_by(SpecialShift.date, SpecialShift.id)
        return list(self.session.scalars(stmt).all())

    def get(self, entry_id: int) -> SpecialShift | None:
        """Return a single special shift by ID, or None."""
        return self.session.get(SpecialShift, entry_id)


class AbsenceRepository:
    """Data access for absences / leave entries (5ABSEN.DBF)."""

    def __init__(self, session: Session):
        self.session = session

    def list(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        employee_id: int | None = None,
    ) -> list[Absence]:
        """Return absences filtered by ISO date range and/or employee."""
        stmt = select(Absence)
        if date_from is not None:
            stmt = stmt.where(Absence.date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Absence.date <= date_to)
        if employee_id is not None:
            stmt = stmt.where(Absence.employee_id == employee_id)
        stmt = stmt.order_by(Absence.date, Absence.id)
        return list(self.session.scalars(stmt).all())

    def get(self, entry_id: int) -> Absence | None:
        """Return a single absence by ID, or None."""
        return self.session.get(Absence, entry_id)
