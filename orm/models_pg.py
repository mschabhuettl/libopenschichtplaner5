"""
Extended SQLAlchemy ORM models for PostgreSQL backend.

These models cover all DBF tables beyond the core Employee/Group/GroupAssignment
models already defined in models.py.
"""


from sqlalchemy import (
    Boolean,
    Float,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Shift(Base):
    __tablename__ = "shifts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    shortname: Mapped[str | None] = mapped_column(String(20), default="")
    position: Mapped[int] = mapped_column(Integer, default=0)
    hide: Mapped[bool] = mapped_column(Boolean, default=False)
    colortext: Mapped[int] = mapped_column(Integer, default=0)
    colorbar: Mapped[int] = mapped_column(Integer, default=0)
    colorbk: Mapped[int] = mapped_column(Integer, default=16777215)
    duration0: Mapped[float] = mapped_column(Float, default=0.0)
    duration1: Mapped[float] = mapped_column(Float, default=0.0)
    duration2: Mapped[float] = mapped_column(Float, default=0.0)
    duration3: Mapped[float] = mapped_column(Float, default=0.0)
    duration4: Mapped[float] = mapped_column(Float, default=0.0)
    duration5: Mapped[float] = mapped_column(Float, default=0.0)
    duration6: Mapped[float] = mapped_column(Float, default=0.0)
    duration7: Mapped[float] = mapped_column(Float, default=0.0)
    startend0: Mapped[str | None] = mapped_column(String(50), default="")
    startend1: Mapped[str | None] = mapped_column(String(50), default="")
    startend2: Mapped[str | None] = mapped_column(String(50), default="")
    startend3: Mapped[str | None] = mapped_column(String(50), default="")
    startend4: Mapped[str | None] = mapped_column(String(50), default="")
    startend5: Mapped[str | None] = mapped_column(String(50), default="")
    startend6: Mapped[str | None] = mapped_column(String(50), default="")
    startend7: Mapped[str | None] = mapped_column(String(50), default="")

    def to_dict(self) -> dict:
        d = {"ID": self.id, "NAME": self.name, "SHORTNAME": self.shortname or "",
             "POSITION": self.position, "HIDE": self.hide,
             "COLORTEXT": self.colortext, "COLORBAR": self.colorbar, "COLORBK": self.colorbk}
        for i in range(8):
            d[f"DURATION{i}"] = getattr(self, f"duration{i}")
            d[f"STARTEND{i}"] = getattr(self, f"startend{i}") or ""
        return d


class LeaveType(Base):
    __tablename__ = "leave_types"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    shortname: Mapped[str | None] = mapped_column(String(20), default="")
    position: Mapped[int] = mapped_column(Integer, default=0)
    hide: Mapped[bool] = mapped_column(Boolean, default=False)
    entitled: Mapped[bool] = mapped_column(Boolean, default=False)
    stdentit: Mapped[float] = mapped_column(Float, default=0.0)
    chargetyp: Mapped[int] = mapped_column(Integer, default=0)
    colortext: Mapped[int] = mapped_column(Integer, default=0)
    colorbar: Mapped[int] = mapped_column(Integer, default=0)
    colorbk: Mapped[int] = mapped_column(Integer, default=16777215)

    def to_dict(self) -> dict:
        return {"ID": self.id, "NAME": self.name, "SHORTNAME": self.shortname or "",
                "POSITION": self.position, "HIDE": self.hide,
                "ENTITLED": self.entitled, "STDENTIT": self.stdentit,
                "CHARGETYP": self.chargetyp,
                "COLORTEXT": self.colortext, "COLORBAR": self.colorbar, "COLORBK": self.colorbk}


class Workplace(Base):
    __tablename__ = "workplaces"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    shortname: Mapped[str | None] = mapped_column(String(20), default="")
    position: Mapped[int] = mapped_column(Integer, default=0)
    hide: Mapped[bool] = mapped_column(Boolean, default=False)
    colortext: Mapped[int] = mapped_column(Integer, default=0)
    colorbar: Mapped[int] = mapped_column(Integer, default=0)
    colorbk: Mapped[int] = mapped_column(Integer, default=16777215)

    def to_dict(self) -> dict:
        return {"ID": self.id, "NAME": self.name, "SHORTNAME": self.shortname or "",
                "POSITION": self.position, "HIDE": self.hide,
                "COLORTEXT": self.colortext, "COLORBAR": self.colorbar, "COLORBK": self.colorbk}


class Holiday(Base):
    __tablename__ = "holidays"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    interval: Mapped[int] = mapped_column(Integer, default=0)

    def to_dict(self) -> dict:
        return {"ID": self.id, "DATE": self.date, "NAME": self.name, "INTERVAL": self.interval}


class ScheduleEntry(Base):
    __tablename__ = "schedule_entries"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    shift_id: Mapped[int] = mapped_column(Integer, nullable=False)
    workplace_id: Mapped[int] = mapped_column(Integer, default=0)
    entry_type: Mapped[int] = mapped_column(Integer, default=0)
    __table_args__ = (Index("idx_schedule_emp_date", "employee_id", "date"), Index("idx_schedule_date", "date"))


class SpecialShift(Base):
    __tablename__ = "special_shifts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str | None] = mapped_column(String(100), default="")
    shortname: Mapped[str | None] = mapped_column(String(20), default="")
    shift_id: Mapped[int] = mapped_column(Integer, default=0)
    workplace_id: Mapped[int] = mapped_column(Integer, default=0)
    entry_type: Mapped[int] = mapped_column(Integer, default=0)
    colortext: Mapped[int] = mapped_column(Integer, default=0)
    colorbar: Mapped[int] = mapped_column(Integer, default=0)
    colorbk: Mapped[int] = mapped_column(Integer, default=16777215)
    bold: Mapped[int] = mapped_column(Integer, default=0)
    startend: Mapped[str | None] = mapped_column(String(50), default="")
    duration: Mapped[float] = mapped_column(Float, default=0.0)
    noextra: Mapped[int] = mapped_column(Integer, default=0)
    __table_args__ = (Index("idx_spshi_emp_date", "employee_id", "date"), Index("idx_spshi_date", "date"))


class Absence(Base):
    __tablename__ = "absences"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    leave_type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_type: Mapped[int] = mapped_column(Integer, default=0)
    interval: Mapped[int] = mapped_column(Integer, default=0)
    start: Mapped[int] = mapped_column(Integer, default=0)
    end: Mapped[int] = mapped_column(Integer, default=0)
    __table_args__ = (Index("idx_absence_emp_date", "employee_id", "date"), Index("idx_absence_date", "date"))


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position: Mapped[int] = mapped_column(Integer, default=0)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    descrip: Mapped[str | None] = mapped_column(String(200), default="")
    admin: Mapped[bool] = mapped_column(Boolean, default=False)
    rights: Mapped[int] = mapped_column(Integer, default=0)
    digest: Mapped[bytes | None] = mapped_column(LargeBinary(16), default=None)
    bcrypt_hash: Mapped[str | None] = mapped_column(String(100), default=None)
    hide: Mapped[bool] = mapped_column(Boolean, default=False)
    wduties: Mapped[bool] = mapped_column(Boolean, default=False)
    wabsences: Mapped[bool] = mapped_column(Boolean, default=False)
    wovertimes: Mapped[bool] = mapped_column(Boolean, default=False)
    wnotes: Mapped[bool] = mapped_column(Boolean, default=False)
    wdeviation: Mapped[bool] = mapped_column(Boolean, default=False)
    wcycleass: Mapped[bool] = mapped_column(Boolean, default=False)
    wswaponly: Mapped[bool] = mapped_column(Boolean, default=False)
    wpast: Mapped[bool] = mapped_column(Boolean, default=False)
    waccemwnd: Mapped[bool] = mapped_column(Boolean, default=True)
    waccgrwnd: Mapped[bool] = mapped_column(Boolean, default=True)
    showabs: Mapped[bool] = mapped_column(Boolean, default=False)
    shownotes: Mapped[bool] = mapped_column(Boolean, default=True)
    showstats: Mapped[bool] = mapped_column(Boolean, default=True)
    raccemwnd: Mapped[bool] = mapped_column(Boolean, default=True)
    raccgrwnd: Mapped[bool] = mapped_column(Boolean, default=True)
    backup: Mapped[bool] = mapped_column(Boolean, default=False)
    accadmwnd: Mapped[bool] = mapped_column(Boolean, default=False)
    addempl: Mapped[int] = mapped_column(Integer, default=0)
    totp_secret: Mapped[str | None] = mapped_column(String(64), default=None)
    totp_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    totp_backup_codes: Mapped[str | None] = mapped_column(Text, default=None)


class Cycle(Base):
    __tablename__ = "cycles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    position: Mapped[int] = mapped_column(Integer, default=0)
    size: Mapped[int] = mapped_column(Integer, default=1)
    unit: Mapped[int] = mapped_column(Integer, default=1)
    hide: Mapped[bool] = mapped_column(Boolean, default=False)


class CycleEntry(Base):
    __tablename__ = "cycle_entries"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cycle_id: Mapped[int] = mapped_column(Integer, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    shift_id: Mapped[int] = mapped_column(Integer, default=0)
    workplace_id: Mapped[int] = mapped_column(Integer, default=0)


class CycleAssignment(Base):
    __tablename__ = "cycle_assignments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    cycle_id: Mapped[int] = mapped_column(Integer, nullable=False)
    start: Mapped[str | None] = mapped_column(String(10), default="")
    end: Mapped[str | None] = mapped_column(String(10), default="")
    entrance: Mapped[str | None] = mapped_column(String(10), default="")


class Note(Base):
    __tablename__ = "notes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, default=0)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    text1: Mapped[str | None] = mapped_column(Text, default="")
    text2: Mapped[str | None] = mapped_column(Text, default="")
    category: Mapped[str | None] = mapped_column(String(20), default="")


class Booking(Base):
    __tablename__ = "bookings_pg"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    booking_type: Mapped[int] = mapped_column(Integer, default=0)
    value: Mapped[float] = mapped_column(Float, default=0.0)
    note: Mapped[str | None] = mapped_column(Text, default="")


class OvertimeRecord(Base):
    __tablename__ = "overtime_records"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    hours: Mapped[float] = mapped_column(Float, default=0.0)


class LeaveEntitlement(Base):
    __tablename__ = "leave_entitlements"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    leave_type_id: Mapped[int] = mapped_column(Integer, default=0)
    entitlement: Mapped[float] = mapped_column(Float, default=0.0)
    carry_forward: Mapped[float] = mapped_column(Float, default=0.0)
    in_days: Mapped[bool] = mapped_column(Boolean, default=True)


class Restriction(Base):
    __tablename__ = "restrictions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_id: Mapped[int] = mapped_column(Integer, nullable=False)
    shift_id: Mapped[int] = mapped_column(Integer, nullable=False)
    weekday: Mapped[int] = mapped_column(Integer, default=0)
    restrict: Mapped[int] = mapped_column(Integer, default=1)
    reason: Mapped[str | None] = mapped_column(String(20), default="")


class HolidayBan(Base):
    __tablename__ = "holiday_bans"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(Integer, default=0)
    start_date: Mapped[str] = mapped_column(String(10), nullable=False)
    end_date: Mapped[str] = mapped_column(String(10), nullable=False)
    restrict: Mapped[int] = mapped_column(Integer, default=1)
    description: Mapped[str | None] = mapped_column(String(200), default="")


class ExtraCharge(Base):
    __tablename__ = "extra_charges"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    position: Mapped[int] = mapped_column(Integer, default=0)
    start: Mapped[int] = mapped_column(Integer, default=0)
    end: Mapped[int] = mapped_column(Integer, default=0)
    validity: Mapped[int] = mapped_column(Integer, default=0)
    validdays: Mapped[str | None] = mapped_column(String(20), default="0000000")
    holrule: Mapped[int] = mapped_column(Integer, default=0)
    hide: Mapped[bool] = mapped_column(Boolean, default=False)


class StaffingRequirement(Base):
    __tablename__ = "staffing_requirements"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(Integer, default=0)
    weekday: Mapped[int] = mapped_column(Integer, default=0)
    shift_id: Mapped[int] = mapped_column(Integer, default=0)
    workplace_id: Mapped[int] = mapped_column(Integer, default=0)
    min_staff: Mapped[int] = mapped_column(Integer, default=0)
    max_staff: Mapped[int] = mapped_column(Integer, default=0)


class Settings(Base):
    __tablename__ = "settings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=0)
    login: Mapped[int] = mapped_column(Integer, default=0)
    spshcat: Mapped[int] = mapped_column(Integer, default=0)
    overtcat: Mapped[int] = mapped_column(Integer, default=0)
    anoaname: Mapped[str | None] = mapped_column(String(100), default="Abwesend")
    anoashort: Mapped[str | None] = mapped_column(String(20), default="X")


class ChangelogEntry(Base):
    __tablename__ = "changelog"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[str] = mapped_column(String(30), nullable=False)
    user: Mapped[str] = mapped_column(String(100), nullable=False)
    user_id: Mapped[int | None] = mapped_column(Integer, default=None)
    action: Mapped[str] = mapped_column(String(20), nullable=False)
    entity: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_id: Mapped[int] = mapped_column(Integer, default=0)
    details: Mapped[str | None] = mapped_column(Text, default="")
    old_value: Mapped[str | None] = mapped_column(Text, default=None)
    new_value: Mapped[str | None] = mapped_column(Text, default=None)
