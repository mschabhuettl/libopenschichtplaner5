"""
SQLAlchemy ORM models for OpenSchichtplaner5.

Proof-of-concept covering Employees and Groups — the two most central
entities in the shift planning domain. Column names follow the existing
DBF field naming where sensible, with Pythonic aliases for readability.

These models are database-agnostic: they work identically on SQLite and
PostgreSQL (or any other SQLAlchemy-supported backend).
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Employee(Base):
    """Employee (Mitarbeiter) — maps to 5EMPL.DBF."""

    __tablename__ = "employees"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position: Mapped[int] = mapped_column(Integer, default=0, doc="Sort order")
    number: Mapped[str | None] = mapped_column(
        String(50), default="", doc="Employee number / badge ID"
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False, doc="Last name")
    firstname: Mapped[str | None] = mapped_column(
        String(100), default="", doc="First name"
    )
    shortname: Mapped[str | None] = mapped_column(
        String(10), default="", doc="Short identifier (e.g. HMU)"
    )
    sex: Mapped[int | None] = mapped_column(
        Integer, default=0, doc="Gender (0=unset, 1=m, 2=f)"
    )

    # Working hours
    hrsday: Mapped[float] = mapped_column(Float, default=0.0, doc="Hours per day")
    hrsweek: Mapped[float] = mapped_column(Float, default=0.0, doc="Hours per week")
    hrsmonth: Mapped[float] = mapped_column(Float, default=0.0, doc="Hours per month")
    workdays: Mapped[str | None] = mapped_column(
        String(30),
        default="1 1 1 1 1 0 0 0",
        doc="Working days bitmask (Mon-Sun + holiday)",
    )

    # Contact info
    salutation: Mapped[str | None] = mapped_column(String(50), default="")
    street: Mapped[str | None] = mapped_column(String(200), default="")
    zip: Mapped[str | None] = mapped_column(String(20), default="")
    town: Mapped[str | None] = mapped_column(String(100), default="")
    phone: Mapped[str | None] = mapped_column(String(50), default="")
    email: Mapped[str | None] = mapped_column(String(200), default="")

    # Employment dates
    birthday: Mapped[str | None] = mapped_column(
        String(10), default=None, doc="ISO date YYYY-MM-DD"
    )
    empstart: Mapped[str | None] = mapped_column(
        String(10), default=None, doc="Employment start date"
    )
    empend: Mapped[str | None] = mapped_column(
        String(10), default=None, doc="Employment end date"
    )
    function: Mapped[str | None] = mapped_column(
        String(100), default="", doc="Job title / function"
    )

    # Flags
    hide: Mapped[bool] = mapped_column(
        Boolean, default=False, doc="Soft-deleted / hidden"
    )

    # Notes (free-text fields from DBF)
    note1: Mapped[str | None] = mapped_column(Text, default="")
    note2: Mapped[str | None] = mapped_column(Text, default="")
    note3: Mapped[str | None] = mapped_column(Text, default="")
    note4: Mapped[str | None] = mapped_column(Text, default="")

    # Metadata
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    group_assignments: Mapped[list["GroupAssignment"]] = relationship(
        back_populates="employee", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Employee(id={self.id}, name='{self.name}', firstname='{self.firstname}')>"

    def to_dict(self) -> dict:
        """Convert to dictionary matching the existing API response format."""
        return {
            "ID": self.id,
            "POSITION": self.position,
            "NUMBER": self.number or "",
            "NAME": self.name,
            "FIRSTNAME": self.firstname or "",
            "SHORTNAME": self.shortname or "",
            "SEX": self.sex or 0,
            "HRSDAY": self.hrsday,
            "HRSWEEK": self.hrsweek,
            "HRSMONTH": self.hrsmonth,
            "WORKDAYS": self.workdays or "",
            "HIDE": self.hide,
            "EMAIL": self.email or "",
            "PHONE": self.phone or "",
            "FUNCTION": self.function or "",
        }


class Group(Base):
    """Shift group (Schichtgruppe) — maps to 5GROUP.DBF."""

    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, doc="Group name")
    shortname: Mapped[str | None] = mapped_column(
        String(20), default="", doc="Short name"
    )
    super_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("groups.id", ondelete="SET NULL"),
        default=None,
        doc="Parent group ID (hierarchical groups)",
    )
    position: Mapped[int] = mapped_column(Integer, default=0, doc="Sort order")
    hide: Mapped[bool] = mapped_column(
        Boolean, default=False, doc="Soft-deleted / hidden"
    )

    # Metadata
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    member_assignments: Mapped[list["GroupAssignment"]] = relationship(
        back_populates="group", cascade="all, delete-orphan"
    )
    parent: Mapped[Optional["Group"]] = relationship(
        remote_side=[id], foreign_keys=[super_id]
    )

    def __repr__(self) -> str:
        return f"<Group(id={self.id}, name='{self.name}')>"

    def to_dict(self) -> dict:
        """Convert to dictionary matching the existing API response format."""
        return {
            "ID": self.id,
            "NAME": self.name,
            "SHORTNAME": self.shortname or "",
            "SUPERID": self.super_id or 0,
            "POSITION": self.position,
            "HIDE": self.hide,
        }


class GroupAssignment(Base):
    """Employee ↔ Group membership — maps to 5GRASG.DBF."""

    __tablename__ = "group_assignments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("employees.id", ondelete="CASCADE"), nullable=False
    )
    group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("groups.id", ondelete="CASCADE"), nullable=False
    )

    # Relationships
    employee: Mapped["Employee"] = relationship(back_populates="group_assignments")
    group: Mapped["Group"] = relationship(back_populates="member_assignments")

    # Unique constraint: one assignment per employee per group
    __table_args__ = (
        Index("idx_group_assignment_unique", "employee_id", "group_id", unique=True),
    )

    def __repr__(self) -> str:
        return f"<GroupAssignment(employee_id={self.employee_id}, group_id={self.group_id})>"
