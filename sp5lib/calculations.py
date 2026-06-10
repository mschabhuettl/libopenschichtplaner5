"""Central calculation layer for Schichtplaner5 (original spec, chapter 3).

Pure functions over plain dict records exactly as :func:`sp5lib.dbf_reader.read_dbf`
delivers them — no I/O and no SP5Database dependency. The per-employee calculation
parameters (5EMPL) travel as a lightweight :class:`EmployeeContext`; the holiday
calendar (5HOLID) as a plain ``date -> INTERVAL`` mapping built by
:func:`holiday_calendar`. All record collections passed to the functions are
expected to be pre-filtered to a single employee (5MASHI/5SPSHI/5ABSEN/5BOOK/
5OVER/5LEAEN of that employee); shift definitions are passed as ``shifts_by_id``.

Conventions (spec 3.1):

- weekday index ``0 = Monday .. 6 = Sunday``; day index ``7`` is the holiday
  ("Ft") slot used by ``WORKDAYS``/``STARTEND``/``DURATION``/demand tables.
- times of day are minutes since midnight (full day 1440, half-day boundary 720).
- hour values are floats (minutes / 60); float zero comparisons use ``EPSILON``.
- every employee-related period sum clamps ``[von, bis]`` to the employment
  period ``EMPSTART``/``EMPEND`` (unset bounds stay open); 5BOOK account
  bookings are summed BEFORE clamping and act solely via their ``DATE``.
"""

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

Record = dict[str, Any]
# 5HOLID calendar: DATE -> INTERVAL (0 = full-day holiday, 1 = first half
# 0..720 min, 2 = second half 720..1440 min).
Holidays = Mapping[date, int]

EPSILON = 1e-6  # float zero comparisons (spec 3.1 no. 7)
DAY_MINUTES = 1440
HALF_DAY_MINUTES = 720  # half-day boundary 12:00 (spec 3.1 no. 5)
HOLIDAY_INDEX = 7  # day index of the "Ft" slot (spec 3.1 no. 4)

# 5BOOK.TYPE account codes (spec 3.6.1)
BOOKING_ACTUAL = 0
BOOKING_NOMINAL = 1
BOOKING_OVERTIME = 2


# ── Basic conversions (spec 3.1, 2.3) ───────────────────────────


def to_date(value: Any) -> date | None:
    """Coerce a record date value (ISO string from read_dbf or date) to date."""
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def parse_day_mask(mask: str, slots: int) -> tuple[bool, ...]:
    """Parse a space-separated flag mask (D-35/D-36), e.g. ``"1 1 1 1 1 0 0 0"``.

    ``slots`` is 8 for WORKDAYS/5LEAVT.VALIDDAYS (Mon..Sun + Ft) and 7 for
    5XCHAR.VALIDDAYS (Mon..Sun without the Ft slot). Missing slots are False.
    """
    tokens = (mask or "").split()
    return tuple(
        bool(tokens[i]) and tokens[i] != "0" if i < len(tokens) else False
        for i in range(slots)
    )


def _parse_minutes(token: str) -> int | None:
    """Parse ``"HH:MM"`` to minutes since midnight (D-28/D-29)."""
    hours, sep, minutes = token.partition(":")
    if not sep:
        return None
    try:
        return int(hours) * 60 + int(minutes)
    except ValueError:
        return None


def parse_startend(value: str) -> list[tuple[int, int]]:
    """Parse a STARTEND work-time string (D-31) into (start, end) minute pairs.

    Format: up to three space-separated ``HH:MM-HH:MM`` intervals; an empty
    string means "not defined on this day index". End <= start means the
    interval crosses midnight (D-30).
    """
    windows = []
    for token in (value or "").split():
        start_s, sep, end_s = token.partition("-")
        if not sep:
            continue
        start = _parse_minutes(start_s)
        end = _parse_minutes(end_s)
        if start is not None and end is not None:
            windows.append((start, end))
    return windows


def holiday_calendar(holid_records: Iterable[Record]) -> dict[date, int]:
    """Build the ``date -> INTERVAL`` holiday mapping from 5HOLID records."""
    calendar: dict[date, int] = {}
    for rec in holid_records:
        d = to_date(rec.get("DATE"))
        if d is not None:
            calendar[d] = int(rec.get("INTERVAL") or 0)
    return calendar


def day_index(d: date, holidays: Holidays) -> int:
    """Day index for STARTEND/DURATION/demand lookups: 7 on holidays (3.4.3 no. 5)."""
    return HOLIDAY_INDEX if d in holidays else d.weekday()


def _dated_entries(
    entries: Iterable[Record], von: date, bis: date
) -> Iterator[tuple[date, Record]]:
    """Yield (date, record) for records whose DATE lies in [von, bis]."""
    for rec in entries:
        d = to_date(rec.get("DATE"))
        if d is not None and von <= d <= bis:
            yield d, rec


# ── Employee context (5EMPL calculation parameters) ─────────────


@dataclass(frozen=True)
class EmployeeContext:
    """Calculation-relevant 5EMPL parameters (spec 3.1/3.3.1)."""

    workdays: tuple[bool, ...]  # 8 flags Mon..Sun + Ft (WORKDAYS, D-35)
    calcbase: int = 0  # 0=day, 1=week, 2=month, 3=total
    hrs_day: float = 0.0
    hrs_week: float = 0.0
    hrs_month: float = 0.0
    hrs_total: float = 0.0
    deduct_hol: bool = False  # DEDUCTHOL, only effective for calcbase 1/2
    emp_start: date | None = None  # EMPSTART; None = open (serial 0/1 unset)
    emp_end: date | None = None  # EMPEND; None = open

    @classmethod
    def from_record(cls, rec: Record) -> "EmployeeContext":
        return cls(
            workdays=parse_day_mask(str(rec.get("WORKDAYS") or ""), 8),
            calcbase=int(rec.get("CALCBASE") or 0),
            hrs_day=float(rec.get("HRSDAY") or 0.0),
            hrs_week=float(rec.get("HRSWEEK") or 0.0),
            hrs_month=float(rec.get("HRSMONTH") or 0.0),
            hrs_total=float(rec.get("HRSTOTAL") or 0.0),
            deduct_hol=bool(rec.get("DEDUCTHOL")),
            emp_start=to_date(rec.get("EMPSTART")),
            emp_end=to_date(rec.get("EMPEND")),
        )


def clamp_to_employment(
    emp: EmployeeContext, von: date, bis: date
) -> tuple[date, date]:
    """Clamp an evaluation period to the employment period (spec 3.1 no. 8).

    Unset bounds do not clamp; the result may be empty (von > bis).
    """
    if emp.emp_start is not None:
        von = max(von, emp.emp_start)
    if emp.emp_end is not None:
        bis = min(bis, emp.emp_end)
    return von, bis


def is_employed(emp: EmployeeContext, d: date) -> bool:
    """EMPSTART <= d <= EMPEND with unset bounds open."""
    if emp.emp_start is not None and d < emp.emp_start:
        return False
    if emp.emp_end is not None and d > emp.emp_end:
        return False
    return True


# ── 3.2 Working days and holidays ───────────────────────────────


def count_holidays_on_workdays(
    emp: EmployeeContext, von: date, bis: date, holidays: Holidays
) -> float:
    """Spec 3.2.2 no. 7: holidays in [von, bis] falling on a marked workday.

    Full holidays (INTERVAL = 0) weigh 1.0, half holidays 0.5. The caller is
    responsible for the WORKDAYS[7] gate (3.2.2 no. 6).
    """
    total = 0.0
    for d, interval in holidays.items():
        if von <= d <= bis and emp.workdays[d.weekday()]:
            total += 1.0 if interval == 0 else 0.5
    return total


def count_working_days(
    emp: EmployeeContext, von: date, bis: date, holidays: Holidays
) -> float:
    """Spec 3.2.2 no. 6: working days in [von, bis], clamped to employment."""
    von, bis = clamp_to_employment(emp, von, bis)
    if von > bis:
        return 0.0
    n = (bis - von).days + 1
    cnt = [n // 7] * 7  # whole weeks
    for i in range(n % 7):  # distribute remainder days
        cnt[(von.weekday() + i) % 7] += 1
    for wd in range(7):
        if not emp.workdays[wd]:
            cnt[wd] = 0
    if emp.workdays[HOLIDAY_INDEX]:
        ft_deduction = 0.0  # holidays ARE workdays
    else:
        ft_deduction = count_holidays_on_workdays(emp, von, bis, holidays)
    return sum(cnt) - ft_deduction


def is_working_day(emp: EmployeeContext, d: date, holidays: Holidays) -> bool:
    """Spec 3.2.2 no. 9: half holidays do NOT make the single day free."""
    if not is_employed(emp, d):
        return False
    if not emp.workdays[d.weekday()]:
        return False
    if not emp.workdays[HOLIDAY_INDEX]:
        interval = holidays.get(d)
        if interval is not None and interval == 0:
            return False
    return True


# ── 3.6.1 Account bookings (needed by 3.3/3.4/3.6) ──────────────


def booking_sum(
    bookings: Iterable[Record], account: int, von: date, bis: date
) -> float:
    """Sum 5BOOK values of one account (TYPE) with DATE in [von, bis].

    Spec 3.6.1 no. 2: bookings act solely via their DATE — no employment
    clamping (the callers sum them before clamping).
    """
    total = 0.0
    for _d, rec in _dated_entries(bookings, von, bis):
        if int(rec.get("TYPE") or 0) == account:
            total += float(rec.get("VALUE") or 0.0)
    return total


# ── 3.3 Nominal hours ───────────────────────────────────────────


def _nominal_week(
    emp: EmployeeContext, von: date, bis: date, holidays: Holidays
) -> float:
    """Spec 3.3.3 no. 7 (CALCBASE = 1): calendar-fixed Mon-Sun weeks."""
    if (bis - von).days + 1 < 7:
        return count_working_days(emp, von, bis, holidays) * emp.hrs_day
    wd_v = von.weekday()
    wd_b = bis.weekday()
    rest = 0.0
    if wd_v != 0:  # head rest week up to Sunday
        rest += count_working_days(emp, von, von + timedelta(days=6 - wd_v), holidays)
        von = von + timedelta(days=7 - wd_v)  # next Monday
    if wd_b < 6:  # tail rest week from Monday
        rest += count_working_days(emp, bis - timedelta(days=wd_b), bis, holidays)
        bis = bis - timedelta(days=wd_b + 1)  # previous Sunday
    if emp.deduct_hol and von <= bis:  # only the full-weeks region
        rest -= count_holidays_on_workdays(emp, von, bis, holidays)
    full_weeks = max(0, (bis - von).days + 1) // 7
    return rest * emp.hrs_day + full_weeks * emp.hrs_week


def _last_of_month(d: date) -> date:
    nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return nxt - timedelta(days=1)


def _nominal_month(
    emp: EmployeeContext, von: date, bis: date, holidays: Holidays
) -> float:
    """Spec 3.3.4 no. 9 (CALCBASE = 2): decomposition at calendar months."""
    same_month = (von.year, von.month) == (bis.year, bis.month)
    full_month = von.day == 1 and bis == _last_of_month(bis)
    if same_month and not full_month:
        return count_working_days(emp, von, bis, holidays) * emp.hrs_day
    rest = 0.0
    if von.day != 1:
        rest += count_working_days(emp, von, _last_of_month(von), holidays)
        von = _last_of_month(von) + timedelta(days=1)  # 1st of next month
    if bis != _last_of_month(bis):
        rest += count_working_days(emp, date(bis.year, bis.month, 1), bis, holidays)
        bis = date(bis.year, bis.month, 1) - timedelta(days=1)  # end of prev. month
    full_months = 12 * (bis.year - von.year) + bis.month - von.month + 1
    if emp.deduct_hol and von <= bis:  # only the full-months region
        rest -= count_holidays_on_workdays(emp, von, bis, holidays)
    return rest * emp.hrs_day + max(0, full_months) * emp.hrs_month


def _nominal_total(
    emp: EmployeeContext, von: date, bis: date, holidays: Holidays
) -> float:
    """Spec 3.3.5 no. 12 (CALCBASE = 3): HRSTOTAL proportional to workdays."""
    if emp.hrs_total <= EPSILON:
        return 0.0
    if emp.emp_start is None or emp.emp_end is None:
        return 0.0  # invalid configuration (3.3.1 no. 3 requires closed period)
    if von == emp.emp_start and bis == emp.emp_end:
        return emp.hrs_total
    part = count_working_days(emp, von, bis, holidays)
    full = count_working_days(emp, emp.emp_start, emp.emp_end, holidays)
    if part <= EPSILON or full <= EPSILON:
        return 0.0
    return emp.hrs_total * part / full


def get_nominal_hours(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    bookings: Iterable[Record] = (),
) -> float:
    """Spec 3.3.2 no. 4: nominal hours = TYPE-1 bookings + CALCBASE formula."""
    soll = booking_sum(bookings, BOOKING_NOMINAL, von, bis)
    von, bis = clamp_to_employment(emp, von, bis)
    if von > bis:
        return soll
    if emp.calcbase == 1:
        return soll + _nominal_week(emp, von, bis, holidays)
    if emp.calcbase == 2:
        return soll + _nominal_month(emp, von, bis, holidays)
    if emp.calcbase == 3:
        return soll + _nominal_total(emp, von, bis, holidays)
    return soll + count_working_days(emp, von, bis, holidays) * emp.hrs_day


# ── 3.4 Actual hours / work hours ───────────────────────────────


def shift_hours_on_day(shift: Record, d: date, holidays: Holidays) -> float:
    """Spec 3.4.3 no. 5/6: DURATION[idx] with idx = holiday ? 7 : weekday.

    The duration only counts if work times are defined for that day index
    (STARTEND{idx} has a window with non-zero minutes); otherwise 0.
    """
    idx = day_index(d, holidays)
    windows = parse_startend(str(shift.get(f"STARTEND{idx}") or ""))
    if not any(w != (0, 0) for w in windows):
        return 0.0
    return float(shift.get(f"DURATION{idx}") or 0.0)


def _replaced_days(special: list[tuple[date, Record]]) -> set[date]:
    """Days whose normal duty is replaced by a work-time deviation.

    Spec 3.4.4 no. 12: a 5SPSHI record with SHIFTID set is a work-time
    deviation and replaces the normal duty of that day (no double counting).
    """
    return {d for d, rec in special if int(rec.get("SHIFTID") or 0)}


def get_work_hours(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    shifts_by_id: Mapping[int, Record],
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
    special_shifts: Iterable[Record] = (),
) -> float:
    """Spec 3.4.2 no. 2: work time from 5MASHI + expanded 5CYASS + 5SPSHI.

    ``cycle_shifts`` are day entries produced by :func:`expand_cycle_assignments`.
    5OVER entries are NOT part of the work time (3.4.2 no. 3).
    """
    von, bis = clamp_to_employment(emp, von, bis)
    if von > bis:
        return 0.0
    special = list(_dated_entries(special_shifts, von, bis))
    replaced = _replaced_days(special)
    total = 0.0
    for source in (manual_shifts, cycle_shifts):
        for d, rec in _dated_entries(source, von, bis):
            if d in replaced:
                continue
            shift = shifts_by_id.get(int(rec.get("SHIFTID") or 0))
            if shift is not None:
                total += shift_hours_on_day(shift, d, holidays)
    for _d, rec in special:  # 5SPSHI carries its own hours (3.4.4 no. 10)
        total += float(rec.get("DURATION") or 0.0)
    return total


def duty_day_counts(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
    special_shifts: Iterable[Record] = (),
) -> list[int]:
    """Spec 3.4.5 no. 14/15: per day index, days with at least one duty.

    Returns ``cnt[0..7]``; a duty on a Sunday that is also a holiday increments
    both ``cnt[6]`` and ``cnt[7]``.
    """
    von, bis = clamp_to_employment(emp, von, bis)
    counts = [0] * 8
    if von > bis:
        return counts
    days = {
        d
        for source in (manual_shifts, cycle_shifts, special_shifts)
        for d, _rec in _dated_entries(source, von, bis)
    }
    for d in days:
        counts[d.weekday()] += 1
        if d in holidays:
            counts[HOLIDAY_INDEX] += 1
    return counts


def get_actual_hours(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    shifts_by_id: Mapping[int, Record],
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
    special_shifts: Iterable[Record] = (),
    absences: Iterable[Record] = (),
    leave_types_by_id: Mapping[int, Record] | None = None,
    bookings: Iterable[Record] = (),
) -> float:
    """Spec 3.4.1 no. 1: actual hours composition.

    TYPE-0 bookings + work hours + charged absence (types without DEDUCTACT)
    − charged absence of DEDUCTACT types. Absences never touch the nominal
    hours (3.3.2 no. 6); 5OVER never touches the actual hours (3.6.3 no. 8).
    """
    ist = booking_sum(bookings, BOOKING_ACTUAL, von, bis)
    cvon, cbis = clamp_to_employment(emp, von, bis)
    if cvon > cbis:
        return ist
    ist += get_work_hours(
        emp,
        cvon,
        cbis,
        holidays=holidays,
        shifts_by_id=shifts_by_id,
        manual_shifts=manual_shifts,
        cycle_shifts=cycle_shifts,
        special_shifts=special_shifts,
    )
    sums = absence_sums(
        emp,
        cvon,
        cbis,
        holidays=holidays,
        absences=absences,
        leave_types_by_id=leave_types_by_id or {},
    )
    return ist + sums.charged - sums.charged_deduct_actual


# ── 3.5 Absences: hour value and charging ───────────────────────


def absence_hours(
    emp: EmployeeContext, absence: Record, leave_type: Record, holidays: Holidays
) -> float:
    """Spec 3.5.2 no. 3: raw hour value of one absence entry on its day."""
    d = to_date(absence.get("DATE"))
    if d is None:
        return 0.0
    interval = int(absence.get("INTERVAL") or 0)
    if not leave_type.get("COUNTALL"):
        if not emp.workdays[d.weekday()]:
            return 0.0
        hol = holidays.get(d)
        if hol is not None:
            if hol == 0:  # full holiday
                if not emp.workdays[HOLIDAY_INDEX]:
                    return 0.0
            elif not emp.workdays[HOLIDAY_INDEX]:
                # Half holiday on a free Ft: clip the absence to the
                # non-holiday half (holiday half: 1 -> 0..720, 2 -> 720..1440).
                free = (
                    (HALF_DAY_MINUTES, DAY_MINUTES) if hol == 1 else (0, HALF_DAY_MINUTES)
                )
                if interval == 0:  # full-day absence -> the free half
                    return emp.hrs_day * 0.5
                if interval in (1, 2):
                    half = (
                        (0, HALF_DAY_MINUTES) if interval == 1 else (HALF_DAY_MINUTES, DAY_MINUTES)
                    )
                    return emp.hrs_day * 0.5 if half == free else 0.0
                if interval == 3:
                    start = int(absence.get("START") or 0)
                    end = int(absence.get("END") or 0)
                    if end <= start:
                        end += DAY_MINUTES
                    minutes = max(0, min(end, free[1]) - max(start, free[0]))
                    return minutes / 60.0
    if interval in (1, 2):
        return emp.hrs_day * 0.5
    if interval == 3:
        minutes = int(absence.get("END") or 0) - int(absence.get("START") or 0)
        if minutes <= 0:
            minutes += DAY_MINUTES  # computational day change
        return minutes / 60.0
    return emp.hrs_day  # INTERVAL = 0, full day


def charge_factor(leave_type: Record, hrs_day: float) -> float:
    """Spec 3.5.3 no. 4: charging factor by CHARGETYP (0 = none, 1 = time,
    2 = fixed hours per day)."""
    chargetyp = int(leave_type.get("CHARGETYP") or 0)
    if chargetyp == 1:
        return 1.0
    if chargetyp == 2:
        if hrs_day <= EPSILON:
            return 0.0
        return float(leave_type.get("CHARGEHRS") or 0.0) / hrs_day
    return 0.0


@dataclass(frozen=True)
class AbsenceSums:
    """Spec 3.5.4 no. 5: the three hour sums over a period."""

    charged: float  # sum 1: charged hours of types WITHOUT DEDUCTACT (paid)
    charged_deduct_actual: float  # sum 2: charged hours of DEDUCTACT types
    raw_deduct_overtime: float  # sum 3: raw hours of DEDUCTOVT types


def absence_sums(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    absences: Iterable[Record],
    leave_types_by_id: Mapping[int, Record],
) -> AbsenceSums:
    """Period sums of absence charging, clamped to employment (3.5.4)."""
    von, bis = clamp_to_employment(emp, von, bis)
    charged = deduct_actual = raw_overtime = 0.0
    if von <= bis:
        for _d, rec in _dated_entries(absences, von, bis):
            lt = leave_types_by_id.get(int(rec.get("LEAVETYPID") or 0))
            if lt is None:
                continue
            hours = absence_hours(emp, rec, lt, holidays)
            value = hours * charge_factor(lt, emp.hrs_day)
            if lt.get("DEDUCTACT"):
                deduct_actual += value
            else:
                charged += value
            if lt.get("DEDUCTOVT"):
                raw_overtime += hours
    return AbsenceSums(charged, deduct_actual, raw_overtime)


def absence_days_by_type(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    absences: Iterable[Record],
    leave_types_by_id: Mapping[int, Record],
) -> dict[int, float]:
    """Spec 3.5.4 no. 5 (sum 4) / 3.9.3 no. 5: absence days per leave type.

    Days = charged hours of the type / HRSDAY (half days count 0.5).
    """
    von, bis = clamp_to_employment(emp, von, bis)
    days: dict[int, float] = {}
    if von > bis or emp.hrs_day <= EPSILON:
        return days
    for _d, rec in _dated_entries(absences, von, bis):
        lt_id = int(rec.get("LEAVETYPID") or 0)
        lt = leave_types_by_id.get(lt_id)
        if lt is None:
            continue
        hours = absence_hours(emp, rec, lt, holidays) * charge_factor(lt, emp.hrs_day)
        days[lt_id] = days.get(lt_id, 0.0) + hours / emp.hrs_day
    return days


# ── 3.6 Accounts, saldo, overtime ───────────────────────────────


def get_saldo(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    shifts_by_id: Mapping[int, Record],
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
    special_shifts: Iterable[Record] = (),
    absences: Iterable[Record] = (),
    leave_types_by_id: Mapping[int, Record] | None = None,
    bookings: Iterable[Record] = (),
) -> float:
    """Spec 3.6.2 no. 4: saldo = actual − nominal over the same period.

    There is no persisted account balance and no year-end close for hour
    accounts (3.6.2 no. 5/6) — corrections are 5BOOK bookings only.
    """
    ist = get_actual_hours(
        emp,
        von,
        bis,
        holidays=holidays,
        shifts_by_id=shifts_by_id,
        manual_shifts=manual_shifts,
        cycle_shifts=cycle_shifts,
        special_shifts=special_shifts,
        absences=absences,
        leave_types_by_id=leave_types_by_id,
        bookings=bookings,
    )
    soll = get_nominal_hours(emp, von, bis, holidays=holidays, bookings=bookings)
    return ist - soll


def get_overtime_hours(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    bookings: Iterable[Record] = (),
    overtimes: Iterable[Record] = (),
    absences: Iterable[Record] = (),
    leave_types_by_id: Mapping[int, Record] | None = None,
) -> float:
    """Spec 3.6.3 no. 7: overtime account (5OVER, Win-Fehlzeiten).

    TYPE-2 bookings + 5OVER hours − raw absence hours of DEDUCTOVT types.
    5OVER entries never flow into actual hours or work time (no. 8).
    """
    total = booking_sum(bookings, BOOKING_OVERTIME, von, bis)
    von, bis = clamp_to_employment(emp, von, bis)
    if von > bis:
        return total
    for _d, rec in _dated_entries(overtimes, von, bis):
        total += float(rec.get("HOURS") or 0.0)
    total -= absence_sums(
        emp,
        von,
        bis,
        holidays=holidays,
        absences=absences,
        leave_types_by_id=leave_types_by_id or {},
    ).raw_deduct_overtime
    return total


# ── 3.7 Leave entitlements (5LEAEN) ─────────────────────────────


def leave_taken(
    emp: EmployeeContext,
    leave_type: Record,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    absences: Iterable[Record],
    in_days: bool = True,
) -> float:
    """Spec 3.7.1 no. 4: consumption of one leave type over [von, bis].

    Per-entry contribution follows 3.5.2/3.5.3; result in days (charged hours /
    HRSDAY) or, with ``in_days=False`` (INDAYS = 0), directly in hours.
    """
    von, bis = clamp_to_employment(emp, von, bis)
    if von > bis:
        return 0.0
    lt_id = int(leave_type.get("ID") or 0)
    factor = charge_factor(leave_type, emp.hrs_day)
    hours = 0.0
    for _d, rec in _dated_entries(absences, von, bis):
        if int(rec.get("LEAVETYPID") or 0) == lt_id:
            hours += absence_hours(emp, rec, leave_type, holidays) * factor
    if in_days:
        return hours / emp.hrs_day if emp.hrs_day > EPSILON else 0.0
    return hours


@dataclass(frozen=True)
class LeaveAccount:
    """Spec 3.7.1 no. 5: the five values of one entitlement statistics row."""

    entitlement: float  # ENTITLEMNT ("Normal")
    rest: float  # REST (carry-over from previous year)
    total: float  # entitlement + rest
    taken: float
    remaining: float  # total − taken (negative = exceeded, 3.7.1 no. 6)


def _entitlements_by_year_type(
    entitlements: Iterable[Record],
) -> dict[tuple[int, int], Record]:
    return {
        (int(rec.get("YEAR") or 0), int(rec.get("LEAVETYPID") or 0)): rec
        for rec in entitlements
    }


def leave_account(
    emp: EmployeeContext,
    year: int,
    leave_type: Record,
    *,
    holidays: Holidays,
    entitlements: Iterable[Record],
    absences: Iterable[Record],
) -> LeaveAccount:
    """Spec 3.7.1: entitlement account for (employee × year × leave type)."""
    rec = _entitlements_by_year_type(entitlements).get(
        (year, int(leave_type.get("ID") or 0))
    )
    entitlement = float(rec.get("ENTITLEMNT") or 0.0) if rec else 0.0
    rest = float(rec.get("REST") or 0.0) if rec else 0.0
    in_days = bool(rec.get("INDAYS", 1)) if rec else True
    taken = leave_taken(
        emp,
        leave_type,
        date(year, 1, 1),
        date(year, 12, 31),
        holidays=holidays,
        absences=absences,
        in_days=in_days,
    )
    total = entitlement + rest
    return LeaveAccount(entitlement, rest, total, taken, total - taken)


def annual_close(
    emp: EmployeeContext,
    year: int,
    *,
    holidays: Holidays,
    leave_types: Iterable[Record],
    entitlements: Iterable[Record],
    absences: Iterable[Record],
    keep_entitlements: bool = False,
) -> list[Record]:
    """Spec 3.7.2 no. 7: year-end close — compute the 5LEAEN rows for year+1.

    Pure computation: returns the new/updated rows as plain dicts (YEAR,
    LEAVETYPID, ENTITLEMNT, REST, INDAYS); persisting them is the caller's job.
    ``keep_entitlements`` is the dialog option "Urlaubsansprüche bleiben im
    Folgejahr gleich".
    """
    by_year_type = _entitlements_by_year_type(entitlements)
    result = []
    for lt in leave_types:
        if not lt.get("ENTITLED"):
            continue
        carry = bool(lt.get("CARRYFWD"))
        if not (carry or keep_entitlements):
            continue
        lt_id = int(lt.get("ID") or 0)
        cur = by_year_type.get((year, lt_id))
        normal = float(cur.get("ENTITLEMNT") or 0.0) if cur else 0.0
        rest = float(cur.get("REST") or 0.0) if cur else 0.0
        in_days = bool(cur.get("INDAYS", 1)) if cur else True
        taken = leave_taken(
            emp,
            lt,
            date(year, 1, 1),
            date(year, 12, 31),
            holidays=holidays,
            absences=absences,
            in_days=in_days,
        )
        rest_new = (normal + rest - taken) if carry else 0.0
        if keep_entitlements:
            normal_new = normal
        else:
            nxt = by_year_type.get((year + 1, lt_id))
            normal_new = (
                float(nxt.get("ENTITLEMNT") or 0.0)
                if nxt
                else float(lt.get("STDENTIT") or 0.0)
            )
        result.append(
            {
                "YEAR": year + 1,
                "LEAVETYPID": lt_id,
                "ENTITLEMNT": normal_new,
                "REST": rest_new,
                "INDAYS": int(in_days),
            }
        )
    return result


def forfeit_rest(
    emp: EmployeeContext,
    cutoff: date,
    *,
    holidays: Holidays,
    leave_types: Iterable[Record],
    entitlements: Iterable[Record],
    absences: Iterable[Record],
) -> list[Record]:
    """Spec 3.7.3 no. 8: cut REST to the consumption up to the cutoff date.

    Returns the rows to update (YEAR, LEAVETYPID, REST); REST is only ever
    reduced, never increased, and ENTITLEMNT stays untouched.
    """
    year = cutoff.year
    by_year_type = _entitlements_by_year_type(entitlements)
    result = []
    for lt in leave_types:
        if not lt.get("ENTITLED"):
            continue
        lt_id = int(lt.get("ID") or 0)
        rec = by_year_type.get((year, lt_id))
        rest = float(rec.get("REST") or 0.0) if rec else 0.0
        in_days = bool(rec.get("INDAYS", 1)) if rec else True
        taken = leave_taken(
            emp,
            lt,
            date(year, 1, 1),
            cutoff,
            holidays=holidays,
            absences=absences,
            in_days=in_days,
        )
        if rest - taken > EPSILON:
            result.append({"YEAR": year, "LEAVETYPID": lt_id, "REST": taken})
    return result


# ── 3.8 Extra charges (5XCHAR) ──────────────────────────────────


def _window_day_parts(
    start: int, end: int
) -> list[tuple[int, tuple[int, int]]]:
    """Split a (start, end) minute window into per-day parts (3.4.3 no. 8).

    End 0 counts as 1440; end <= start crosses midnight and yields a part on
    the entry day (offset 0) and one on the next day (offset 1).
    """
    end = DAY_MINUTES if end == 0 else end
    if end <= start:
        parts = [(0, (start, DAY_MINUTES)), (1, (0, end))]
    else:
        parts = [(0, (start, end))]
    return [(off, (s, e)) for off, (s, e) in parts if e > s]


def _charge_window_parts(start: int, end: int) -> list[tuple[int, int]]:
    """Spec 3.8.3 no. 9: charge window; day-change windows checked two-part."""
    return [part for _off, part in _window_day_parts(start, end)]


def daily_work_intervals(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    shifts_by_id: Mapping[int, Record],
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
    special_shifts: Iterable[Record] = (),
) -> dict[date, list[tuple[int, int]]]:
    """Work-side intervals per calendar day for the charge intersection.

    Uses the shift time windows STARTEND{idx} (idx = holiday ? 7 : weekday, up
    to 3 sub-windows, spec 3.8.3 no. 10), splits day-crossing windows onto the
    calendar days d and d+1 (3.4.3 no. 8), honours NOEXTRA (3.8.3 no. 13; for
    5SPSHI via the referenced shift if SHIFTID is set, else the own field) and
    the work-time-deviation replacement rule (3.4.4 no. 12).
    """
    von, bis = clamp_to_employment(emp, von, bis)
    intervals: dict[date, list[tuple[int, int]]] = {}
    if von > bis:
        return intervals
    scan_from = von - timedelta(days=1)  # overflow from the previous day
    special = list(_dated_entries(special_shifts, scan_from, bis))
    replaced = _replaced_days(special)

    def add(day: date, windows: list[tuple[int, int]]) -> None:
        for start, end in windows:
            if (start, end) == (0, 0):
                continue  # zero-minutes window = not defined
            for off, part in _window_day_parts(start, end):
                target = day + timedelta(days=off)
                if von <= target <= bis:
                    intervals.setdefault(target, []).append(part)

    for source in (manual_shifts, cycle_shifts):
        for d, rec in _dated_entries(source, scan_from, bis):
            if d in replaced:
                continue
            shift = shifts_by_id.get(int(rec.get("SHIFTID") or 0))
            if shift is None or shift.get("NOEXTRA"):
                continue
            idx = day_index(d, holidays)
            add(d, parse_startend(str(shift.get(f"STARTEND{idx}") or "")))
    for d, rec in special:
        shift_id = int(rec.get("SHIFTID") or 0)
        if shift_id:
            ref = shifts_by_id.get(shift_id)
            noextra = bool(ref.get("NOEXTRA")) if ref else False
        else:
            noextra = bool(rec.get("NOEXTRA"))
        if noextra:
            continue
        add(d, parse_startend(str(rec.get("STARTEND") or "")))
    return intervals


def extracharge_hours_on_day(
    extracharge: Record,
    d: date,
    work_intervals: Iterable[tuple[int, int]],
    *,
    holidays: Holidays,
    half_as_full: bool = False,
) -> float:
    """Spec 3.8.2/3.8.3: charge-eligible hours of one charge type on one day.

    ``work_intervals`` are the day's work intervals (minutes) as produced by
    :func:`daily_work_intervals`. ``half_as_full`` is the report option
    "halbe wie ganze Feiertage behandeln" (3.8.2 no. 8).
    """
    if int(extracharge.get("VALIDITY") or 0) == 1:  # fixed date (3.8.2 no. 5)
        if to_date(extracharge.get("DATE")) != d:
            return 0.0
    else:  # 7-slot weekday mask Mon..Sun (3.8.2 no. 6, D-36)
        if not parse_day_mask(str(extracharge.get("VALIDDAYS") or ""), 7)[d.weekday()]:
            return 0.0
    holrule = int(extracharge.get("HOLRULE") or 0)
    interval = holidays.get(d)
    half: tuple[int, int] | None = None
    if holrule == 1:  # only on holidays
        if interval is None:
            return 0.0
        if interval != 0 and not half_as_full:
            half = (
                (HALF_DAY_MINUTES, DAY_MINUTES) if interval == 1 else (0, HALF_DAY_MINUTES)
            )
    elif holrule == 2 and interval is not None:  # not on holidays
        if interval == 0 or half_as_full:
            return 0.0
        half = (
            (HALF_DAY_MINUTES, DAY_MINUTES) if interval == 1 else (0, HALF_DAY_MINUTES)
        )
    parts = _charge_window_parts(
        int(extracharge.get("START") or 0), int(extracharge.get("END") or 0)
    )
    if half is not None:  # restrict to one day half (3.8.2 no. 8)
        parts = [
            (max(s, half[0]), min(e, half[1]))
            for s, e in parts
            if min(e, half[1]) > max(s, half[0])
        ]
    minutes = 0
    for ps, pe in parts:
        for ws, we in work_intervals:
            minutes += max(0, min(pe, we) - max(ps, ws))
    return minutes / 60.0


def get_extracharge_hours(
    emp: EmployeeContext,
    extracharge: Record,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    shifts_by_id: Mapping[int, Record],
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
    special_shifts: Iterable[Record] = (),
    half_as_full: bool = False,
) -> float:
    """Charge-eligible hours of one charge type over [von, bis] (3.8)."""
    von, bis = clamp_to_employment(emp, von, bis)
    if von > bis:
        return 0.0
    intervals = daily_work_intervals(
        emp,
        von,
        bis,
        holidays=holidays,
        shifts_by_id=shifts_by_id,
        manual_shifts=manual_shifts,
        cycle_shifts=cycle_shifts,
        special_shifts=special_shifts,
    )
    total = 0.0
    for d, work in intervals.items():
        total += extracharge_hours_on_day(
            extracharge, d, work, holidays=holidays, half_as_full=half_as_full
        )
    return total


# ── 3.9 Personnel table and utilization ─────────────────────────


def shift_assignment_counts(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
) -> dict[int, int]:
    """Spec 3.9.3 no. 4: per shift type, number of assignments in the period."""
    von, bis = clamp_to_employment(emp, von, bis)
    counts: dict[int, int] = {}
    if von > bis:
        return counts
    for source in (manual_shifts, cycle_shifts):
        for _d, rec in _dated_entries(source, von, bis):
            shift_id = int(rec.get("SHIFTID") or 0)
            if shift_id:
                counts[shift_id] = counts.get(shift_id, 0) + 1
    return counts


def count_special_shifts(
    emp: EmployeeContext,
    von: date,
    bis: date,
    special_shifts: Iterable[Record] = (),
) -> int:
    """Spec 3.9.2 no. 2: number of special duties in the period."""
    von, bis = clamp_to_employment(emp, von, bis)
    if von > bis:
        return 0
    return sum(1 for _ in _dated_entries(special_shifts, von, bis))


def personnel_table_row(
    emp: EmployeeContext,
    von: date,
    bis: date,
    *,
    holidays: Holidays,
    shifts_by_id: Mapping[int, Record],
    manual_shifts: Iterable[Record] = (),
    cycle_shifts: Iterable[Record] = (),
    special_shifts: Iterable[Record] = (),
    absences: Iterable[Record] = (),
    leave_types_by_id: Mapping[int, Record] | None = None,
    bookings: Iterable[Record] = (),
) -> dict[str, float]:
    """Spec 3.9.2 no. 2: the standard personnel-table columns for one employee."""
    leave_types_by_id = leave_types_by_id or {}
    plan = dict(
        holidays=holidays,
        shifts_by_id=shifts_by_id,
        manual_shifts=manual_shifts,
        cycle_shifts=cycle_shifts,
        special_shifts=special_shifts,
    )
    ist = get_actual_hours(
        emp,
        von,
        bis,
        absences=absences,
        leave_types_by_id=leave_types_by_id,
        bookings=bookings,
        **plan,
    )
    soll = get_nominal_hours(emp, von, bis, holidays=holidays, bookings=bookings)
    counts = duty_day_counts(
        emp,
        von,
        bis,
        holidays=holidays,
        manual_shifts=manual_shifts,
        cycle_shifts=cycle_shifts,
        special_shifts=special_shifts,
    )
    paid = absence_sums(
        emp,
        von,
        bis,
        holidays=holidays,
        absences=absences,
        leave_types_by_id=leave_types_by_id,
    ).charged
    return {
        "iststunden": ist,
        "sollstunden": soll,
        "saldo": ist - soll,
        "arbeitszeit": get_work_hours(emp, von, bis, **plan),
        "abwesenheit_bezahlt": paid,
        "sonntag": counts[6],
        "feiertag": counts[HOLIDAY_INDEX],
        "sonderdienste": count_special_shifts(emp, von, bis, special_shifts),
    }


def count_assigned(
    entries_by_employee: Mapping[Any, Iterable[Record]],
    d: date,
    shift_ids: Iterable[int],
) -> int:
    """Spec 3.9.4 no. 8: employees assigned to the shift type(s) on day d.

    ``entries_by_employee`` maps an employee key to its plan entries (5MASHI +
    expanded 5CYASS + 5SPSHI with SHIFTID); each employee counts at most once.
    """
    wanted = set(shift_ids)
    count = 0
    for entries in entries_by_employee.values():
        for rec in entries:
            if (
                to_date(rec.get("DATE")) == d
                and int(rec.get("SHIFTID") or 0) in wanted
            ):
                count += 1
                break
    return count


def utilization_status(actual: int, min_demand: int, max_demand: int) -> int:
    """Spec 3.9.4 no. 9: −1 = understaffed, +1 = overstaffed, 0 = within demand."""
    if actual < min_demand:
        return -1
    if actual > max_demand:
        return 1
    return 0


def demand_for_day(
    demands: Iterable[Record],
    d: date,
    *,
    holidays: Holidays,
    shift_id: int | None = None,
) -> tuple[int, int] | None:
    """Look up (MIN, MAX) demand from 5SHDEM-style records for day d.

    Records carry WEEKDAY as day index 0..7 (7 = holiday slot); the caller
    pre-filters by group. Returns None if no demand is defined.
    """
    idx = day_index(d, holidays)
    for rec in demands:
        weekday = rec.get("WEEKDAY")
        if weekday is None or int(weekday) != idx:
            continue
        if shift_id is not None and int(rec.get("SHIFTID") or 0) != shift_id:
            continue
        return int(rec.get("MIN") or 0), int(rec.get("MAX") or 0)
    return None


# ── 5CYASS cycle expansion (spec 6.3 / 5.7) ─────────────────────


def expand_cycle_assignments(
    assignments: Iterable[Record],
    *,
    cycles: Iterable[Record],
    cycle_entries: Iterable[Record],
    cycle_exceptions: Iterable[Record] = (),
    von: date,
    bis: date,
) -> list[Record]:
    """Expand 5CYASS assignments into concrete day duties within [von, bis].

    Per assignment day the cycle position is taken modulo the cycle length
    (rolling application, R6.3-7). ENTRANCE is the 0-based entry offset: for
    week models (5CYCLE.UNIT = 1) the model row (week) valid at the assignment
    start — within a row the weekday of the date aligns (model rows run
    Mon..Sun, R6.3-4); for day models the day position at the start (R6.3-5).
    Cycle positions without a 5CYENT entry (or SHIFTID 0) are free days
    (D-50); 5CYEXC records suppress the cycle duty on their DATE.

    Returns 5MASHI-shaped dicts (EMPLOYEEID, DATE, SHIFTID, WORKPLACID).
    """
    cycles_by_id = {int(rec.get("ID") or 0): rec for rec in cycles}
    entries_by_cycle: dict[int, dict[int, Record]] = {}
    for rec in cycle_entries:
        positions = entries_by_cycle.setdefault(int(rec.get("CYCLEEID") or 0), {})
        positions[int(rec.get("INDEX") or 0)] = rec
    exceptions: dict[tuple[int, int], set[date]] = {}
    for rec in cycle_exceptions:
        d = to_date(rec.get("DATE"))
        if d is not None:
            key = (int(rec.get("EMPLOYEEID") or 0), int(rec.get("CYCLEASSID") or 0))
            exceptions.setdefault(key, set()).add(d)

    result: list[Record] = []
    for ass in assignments:
        cycle = cycles_by_id.get(int(ass.get("CYCLEID") or 0))
        start = to_date(ass.get("START"))
        if cycle is None or start is None:
            continue
        size = int(cycle.get("SIZE") or 0)
        week_model = int(cycle.get("UNIT") or 0) == 1
        length = size * 7 if week_model else size
        if length <= 0:
            continue
        end = to_date(ass.get("END"))
        lo = max(von, start)
        hi = bis if end is None else min(bis, end)
        entrance = int(ass.get("ENTRANCE") or 0)
        base = entrance * 7 + start.weekday() if week_model else entrance
        positions = entries_by_cycle.get(int(cycle.get("ID") or 0), {})
        employee_id = int(ass.get("EMPLOYEEID") or 0)
        skip = exceptions.get((employee_id, int(ass.get("ID") or 0)), set())
        d = lo
        while d <= hi:
            entry = positions.get((base + (d - start).days) % length)
            if entry is not None and int(entry.get("SHIFTID") or 0) and d not in skip:
                result.append(
                    {
                        "EMPLOYEEID": employee_id,
                        "DATE": d.isoformat(),
                        "SHIFTID": int(entry.get("SHIFTID") or 0),
                        "WORKPLACID": int(entry.get("WORKPLACID") or 0),
                    }
                )
            d += timedelta(days=1)
    return result
