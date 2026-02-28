"""
High-level database access for Schichtplaner5 .DBF files.
"""
import os
import json
import calendar
from datetime import datetime
from typing import List, Dict, Any, Optional
from .dbf_reader import read_dbf, get_table_fields
from .color_utils import bgr_to_hex, is_light_color
from .dbf_writer import append_record, delete_record, find_all_records, update_record

# ── Global cross-request DBF cache ──────────────────────────────
# Maps (db_path, table_name) → (mtime, data)
# Avoids re-reading unchanged DBF files across requests.
_GLOBAL_DBF_CACHE: Dict[tuple, tuple] = {}


class SP5Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _table(self, name: str) -> str:
        return os.path.join(self.db_path, f"5{name}.DBF")

    def _read(self, name: str) -> List[Dict[str, Any]]:
        """Read a DBF table, using a global mtime-based cache.

        The cache is shared across all requests and instances for the same
        db_path. Data is refreshed automatically when the file changes on disk
        (mtime check), so write operations are picked up without manual
        invalidation — while read-heavy workloads avoid redundant disk I/O.
        """
        path = self._table(name)
        key = (self.db_path, name)
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            mtime = 0.0

        cached = _GLOBAL_DBF_CACHE.get(key)
        if cached is not None and cached[0] == mtime:
            return cached[1]

        data = read_dbf(path)
        _GLOBAL_DBF_CACHE[key] = (mtime, data)
        return data

    def _invalidate_cache(self, name: str) -> None:
        """Invalidate the global cache for a table after a write operation.

        Usually not needed because _read() re-checks mtime automatically,
        but explicit invalidation is useful when the file is written and
        immediately re-read within the same OS tick (mtime granularity).
        """
        key = (self.db_path, name)
        _GLOBAL_DBF_CACHE.pop(key, None)

    def _color_fields(self, record: Dict) -> Dict:
        """Convert BGR color fields to hex strings."""
        for key in ('COLORTEXT', 'COLORBAR', 'COLORBK', 'CBKLABEL', 'CBKSCHED', 'CFGLABEL'):
            if key in record and isinstance(record[key], int):
                record[key + '_HEX'] = bgr_to_hex(record[key])
                record[key + '_LIGHT'] = is_light_color(record[key])
        return record

    # ── Helpers ────────────────────────────────────────────────
    def _count_working_days(self, year: int, month: int, workdays_list: list = None) -> int:
        """Count working days in a month, using WORKDAYS_LIST when available.
        
        workdays_list: list of 7+ bools [Mon, Tue, Wed, Thu, Fri, Sat, Sun]
        Falls back to Mon-Fri (weekday < 5) if not provided or too short.
        """
        num_days = calendar.monthrange(year, month)[1]
        if workdays_list and len(workdays_list) >= 7:
            return sum(
                1 for d in range(1, num_days + 1)
                if workdays_list[datetime(year, month, d).weekday()]
            )
        # Default: Mon-Fri
        return sum(
            1 for d in range(1, num_days + 1)
            if datetime(year, month, d).weekday() < 5
        )

    # ── Employees ──────────────────────────────────────────────
    def get_employees(self, include_hidden: bool = False) -> List[Dict]:
        rows = self._read('EMPL')
        result = []
        for r in rows:
            if not include_hidden and r.get('HIDE'):
                continue
            # Parse WORKDAYS: stored as plain ASCII "1 1 1 1 1 0 0 0"
            # (dbf_reader now correctly decodes ASCII vs UTF-16 LE fields)
            wd = r.get('WORKDAYS', '')
            if wd:
                r['WORKDAYS_LIST'] = [x == '1' for x in wd.split()]
            else:
                r['WORKDAYS_LIST'] = []
            # Auto-generate SHORTNAME if empty:
            # Format: Vorname-Initial + erste 2 Buchstaben Nachname (uppercase)
            # e.g. "Hans Mueller" → "HMU"  (H from Hans, MU from Mueller)
            original_shortname = (r.get('SHORTNAME') or '').strip()
            if not original_shortname:
                surname = (r.get('NAME', '') or '').strip()
                firstname = (r.get('FIRSTNAME', '') or '').strip()
                if firstname and surname:
                    r['SHORTNAME'] = (firstname[0] + surname[:2]).upper()
                elif surname:
                    r['SHORTNAME'] = surname[:3].upper()
                elif firstname:
                    r['SHORTNAME'] = firstname[:3].upper()
                else:
                    r['SHORTNAME'] = '???'
                r['SHORTNAME_GENERATED'] = True   # flag: was auto-generated, not stored in DB
            else:
                r['SHORTNAME'] = original_shortname
                r['SHORTNAME_GENERATED'] = False
            # Convert color fields to hex
            self._color_fields(r)
            result.append(r)
        result.sort(key=lambda x: x.get('POSITION', 0))
        return result

    def get_employee(self, emp_id: int) -> Optional[Dict]:
        for e in self.get_employees(include_hidden=True):
            if e.get('ID') == emp_id:
                return e
        return None

    # ── Groups ─────────────────────────────────────────────────
    def get_groups(self, include_hidden: bool = False) -> List[Dict]:
        rows = self._read('GROUP')
        if not include_hidden:
            rows = [r for r in rows if not r.get('HIDE')]
        for r in rows:
            self._color_fields(r)
        rows.sort(key=lambda x: x.get('POSITION', 0))
        return rows

    def get_group_members(self, group_id: int) -> List[int]:
        """Return list of employee IDs in a group."""
        assignments = self._read('GRASG')
        return [a['EMPLOYEEID'] for a in assignments if a.get('GROUPID') == group_id]

    def get_employee_groups(self, emp_id: int) -> List[int]:
        """Return list of group IDs an employee belongs to."""
        assignments = self._read('GRASG')
        return [a['GROUPID'] for a in assignments if a.get('EMPLOYEEID') == emp_id]

    # ── Shifts ─────────────────────────────────────────────────
    def get_shifts(self, include_hidden: bool = False) -> List[Dict]:
        rows = self._read('SHIFT')
        if not include_hidden:
            rows = [r for r in rows if not r.get('HIDE')]
        for r in rows:
            self._color_fields(r)
            # Parse STARTEND per weekday
            times = {}
            for i in range(7):
                key = f'STARTEND{i}'
                val = r.get(key, '').strip()
                if val and '-' in val:
                    parts = val.split('-')
                    if len(parts) == 2:
                        times[i] = {'start': parts[0].strip(), 'end': parts[1].strip()}
                    else:
                        times[i] = None
                else:
                    times[i] = None
            r['TIMES_BY_WEEKDAY'] = times
        rows.sort(key=lambda x: x.get('POSITION', 0))
        return rows

    def get_shift(self, shift_id: int) -> Optional[Dict]:
        for s in self.get_shifts(include_hidden=True):
            if s.get('ID') == shift_id:
                return s
        return None

    # ── Leave Types ────────────────────────────────────────────
    def get_leave_types(self, include_hidden: bool = False) -> List[Dict]:
        rows = self._read('LEAVT')
        if not include_hidden:
            rows = [r for r in rows if not r.get('HIDE')]
        for r in rows:
            self._color_fields(r)
        rows.sort(key=lambda x: x.get('POSITION', 0))
        return rows

    def get_leave_type(self, lt_id: int) -> Optional[Dict]:
        for lt in self.get_leave_types(include_hidden=True):
            if lt.get('ID') == lt_id:
                return lt
        return None

    # ── Workplaces ─────────────────────────────────────────────
    def get_workplaces(self, include_hidden: bool = False) -> List[Dict]:
        rows = self._read('WOPL')
        if not include_hidden:
            rows = [r for r in rows if not r.get('HIDE')]
        for r in rows:
            self._color_fields(r)
        rows.sort(key=lambda x: x.get('POSITION', 0))
        return rows

    # ── Holidays ───────────────────────────────────────────────
    def get_holidays(self, year: Optional[int] = None) -> List[Dict]:
        rows = self._read('HOLID')
        if year is not None:
            result = []
            year_str = str(year)
            for r in rows:
                interval = r.get('INTERVAL', 0)
                date = r.get('DATE', '')
                if interval == 1:
                    # Recurring: show every year, replace year in date
                    if date and len(date) >= 10:
                        adjusted_date = year_str + date[4:]
                    else:
                        adjusted_date = date
                    row_copy = dict(r)
                    row_copy['DATE'] = adjusted_date
                    result.append(row_copy)
                elif date.startswith(year_str):
                    result.append(r)
            result.sort(key=lambda x: x.get('DATE', ''))
            return result
        rows.sort(key=lambda x: x.get('DATE', ''))
        return rows

    def get_holiday_dates(self, year: int) -> set:
        return {r['DATE'] for r in self.get_holidays(year) if r.get('DATE')}

    # ── Schedule ───────────────────────────────────────────────
    def get_schedule(self, year: int, month: int, group_id: Optional[int] = None) -> List[Dict]:
        """
        Get schedule entries for a month.
        Returns list of {employee_id, date, type, shift_id, leave_type_id, 
                          shift_name, shift_short, color_bk, color_text, workplace_id, ...}
        """
        prefix = f"{year:04d}-{month:02d}"
        entries = []

        # Get shift assignments (MASHI)
        for r in self._read('MASHI'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                entries.append({
                    'employee_id': r.get('EMPLOYEEID'),
                    'date': d,
                    'kind': 'shift',
                    'shift_id': r.get('SHIFTID'),
                    'workplace_id': r.get('WORKPLACID'),
                    'leave_type_id': None,
                })

        # Get special shifts (SPSHI)
        for r in self._read('SPSHI'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                entries.append({
                    'employee_id': r.get('EMPLOYEEID'),
                    'date': d,
                    'kind': 'special_shift',
                    'shift_id': r.get('SHIFTID'),
                    'workplace_id': r.get('WORKPLACID'),
                    'leave_type_id': None,
                    'custom_name': r.get('NAME', ''),
                    'custom_short': r.get('SHORTNAME', ''),
                    'color_bk': bgr_to_hex(r.get('COLORBK', 16777215)) if r.get('COLORBK') else None,
                    'color_text': bgr_to_hex(r.get('COLORTEXT', 0)) if r.get('COLORTEXT') else None,
                })

        # Get absences (ABSEN)
        for r in self._read('ABSEN'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                entries.append({
                    'employee_id': r.get('EMPLOYEEID'),
                    'date': d,
                    'kind': 'absence',
                    'shift_id': None,
                    'workplace_id': None,
                    'leave_type_id': r.get('LEAVETYPID'),
                })

        # Enrich with shift/leave-type data
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}

        for e in entries:
            if e['shift_id'] and e['shift_id'] in shifts_map:
                s = shifts_map[e['shift_id']]
                e['display_name'] = s.get('SHORTNAME', s.get('NAME', ''))
                e['color_bk'] = e.get('color_bk') or bgr_to_hex(s.get('COLORBK', 16777215))
                e['color_text'] = e.get('color_text') or bgr_to_hex(s.get('COLORTEXT', 0))
                e['shift_name'] = s.get('NAME', '')
            elif e['leave_type_id'] and e['leave_type_id'] in lt_map:
                lt = lt_map[e['leave_type_id']]
                e['display_name'] = lt.get('SHORTNAME', lt.get('NAME', ''))
                e['color_bk'] = bgr_to_hex(lt.get('COLORBK', 16777215))
                e['color_text'] = bgr_to_hex(lt.get('COLORBAR', 0))
                e['leave_name'] = lt.get('NAME', '')
            else:
                e['display_name'] = e.get('custom_short', '')
                e['color_bk'] = e.get('color_bk', '#FFFFFF')
                e['color_text'] = e.get('color_text', '#000000')

        # Filter by group
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            entries = [e for e in entries if e['employee_id'] in member_ids]

        return entries

    # ── Users ──────────────────────────────────────────────────
    def _role_from_record(self, r: Dict) -> str:
        if r.get('ADMIN'):
            return 'Admin'
        if r.get('RIGHTS') == 1:
            return 'Planer'
        return 'Leser'

    def _hash_password(self, password: str) -> bytes:
        """Return 16-byte MD5 digest of password (matches 5USER.DBF DIGEST field)."""
        import hashlib
        return hashlib.md5(password.encode('utf-8')).digest()

    def get_users(self) -> List[Dict]:
        rows = self._read('USER')
        result = []
        for r in rows:
            if r.get('HIDE'):
                continue
            result.append({
                'ID': r.get('ID'),
                'POSITION': r.get('POSITION', 0),
                'NAME': r.get('NAME', ''),
                'DESCRIP': r.get('DESCRIP', ''),
                'ADMIN': bool(r.get('ADMIN')),
                'RIGHTS': r.get('RIGHTS', 0),
                'HIDE': bool(r.get('HIDE')),
                'WDUTIES': bool(r.get('WDUTIES')),
                'WABSENCES': bool(r.get('WABSENCES')),
                'WOVERTIMES': bool(r.get('WOVERTIMES')),
                'BACKUP': bool(r.get('BACKUP')),
                'role': self._role_from_record(r),
            })
        result.sort(key=lambda x: x.get('POSITION', 999))
        return result

    def create_user(self, data: dict) -> dict:
        """Create a new SP5 user in 5USER.DBF."""
        filepath = self._table('USER')
        fields = get_table_fields(filepath)
        new_id = self._next_id('USER')
        rows = self._read('USER')
        # Uniqueness check: NAME (username) must be unique among active users
        name_lower = (data.get('NAME') or '').strip().lower()
        for row in rows:
            if row.get('HIDE') or row.get('HIDE') == 1:
                continue
            if (row.get('NAME') or '').strip().lower() == name_lower:
                raise ValueError(f"DUPLICATE:USERNAME:{data.get('NAME')}")
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1

        role = data.get('role', 'Leser')
        is_admin = 1 if role == 'Admin' else 0
        rights = 1 if role == 'Planer' else 0

        password = data.get('PASSWORD', '')
        digest = self._hash_password(password) if password else b'\x00' * 16

        # Default permissions based on role
        write_perms = 1 if role in ('Admin', 'Planer') else 0
        record = {
            'ID': new_id,
            'POSITION': max_pos,
            'NAME': data.get('NAME', ''),
            'DESCRIP': data.get('DESCRIP', ''),
            'ADMIN': is_admin,
            'DIGEST': digest,
            'RIGHTS': rights,
            'CATEGORY': '\x31\x00' * 20,
            'ADDEMPL': 0,
            'WDUTIES': write_perms,
            'WABSENCES': write_perms,
            'WOVERTIMES': write_perms,
            'WNOTES': write_perms,
            'WDEVIATION': write_perms,
            'WCYCLEASS': write_perms,
            'WSWAPONLY': 0,
            'WPAST': write_perms,
            'WACCEMWND': 1,
            'WACCGRWND': 1,
            'SHOWABS': 0,
            'SHOWNOTES': 1,
            'SHOWSTATS': 1,
            'RACCEMWND': 1,
            'RACCGRWND': 1,
            'BACKUP': is_admin,
            'HIDEBARIN': 0,
            'HIDEBARNO': 0,
            'ACCVOWND': 1,
            'ACCADMWND': is_admin,
            'MINITABLE': 0,
            'REPORT': '\x31\x31' * 20,
            'HIDE': 0,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'ID': new_id,
            'NAME': record['NAME'],
            'DESCRIP': record['DESCRIP'],
            'ADMIN': bool(is_admin),
            'RIGHTS': rights,
            'HIDE': False,
            'role': role,
        }

    def update_user(self, user_id: int, data: dict) -> dict:
        """Update an existing SP5 user."""
        filepath = self._table('USER')
        fields = get_table_fields(filepath)
        raw_idx, existing = self._find_record('USER', user_id)
        if raw_idx is None:
            raise ValueError(f"User {user_id} not found")

        update_data: Dict[str, Any] = {}

        if 'NAME' in data:
            update_data['NAME'] = data['NAME']
        if 'DESCRIP' in data:
            update_data['DESCRIP'] = data['DESCRIP']
        if 'role' in data:
            role = data['role']
            update_data['ADMIN'] = 1 if role == 'Admin' else 0
            update_data['RIGHTS'] = 1 if role == 'Planer' else 0
            write_perms = 1 if role in ('Admin', 'Planer') else 0
            update_data['WDUTIES'] = write_perms
            update_data['WABSENCES'] = write_perms
            update_data['WOVERTIMES'] = write_perms
            update_data['WNOTES'] = write_perms
            update_data['WDEVIATION'] = write_perms
            update_data['WCYCLEASS'] = write_perms
            update_data['WPAST'] = write_perms
            update_data['BACKUP'] = 1 if role == 'Admin' else 0
            update_data['ACCADMWND'] = 1 if role == 'Admin' else 0
        if 'PASSWORD' in data and data['PASSWORD']:
            update_data['DIGEST'] = self._hash_password(data['PASSWORD'])

        update_record(filepath, fields, raw_idx, update_data)

        # Return updated user
        role = data.get('role')
        if role is None and existing:
            role = self._role_from_record(existing)
        return {
            'ID': user_id,
            'NAME': data.get('NAME', existing.get('NAME', '') if existing else ''),
            'DESCRIP': data.get('DESCRIP', existing.get('DESCRIP', '') if existing else ''),
            'role': role,
        }

    def delete_user(self, user_id: int) -> int:
        """Soft-delete (hide) a user. Returns 1 if found, 0 otherwise."""
        filepath = self._table('USER')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('USER', user_id)
        if raw_idx is None:
            return 0
        update_record(filepath, fields, raw_idx, {'HIDE': 1})
        return 1

    def check_user_permission(self, user_id: int, action: str) -> bool:
        """
        Check if a user has permission for a given action.
        Actions: 'admin', 'write_duties', 'write_absences', 'write_overtimes',
                 'write_notes', 'backup', 'read_employees', 'read_groups'
        """
        rows = self._read('USER')
        user = next((r for r in rows if r.get('ID') == user_id and not r.get('HIDE')), None)
        if user is None:
            return False
        perm_map = {
            'admin': 'ADMIN',
            'write_duties': 'WDUTIES',
            'write_absences': 'WABSENCES',
            'write_overtimes': 'WOVERTIMES',
            'write_notes': 'WNOTES',
            'backup': 'BACKUP',
            'read_employees': 'WACCEMWND',
            'read_groups': 'WACCGRWND',
        }
        field = perm_map.get(action)
        if field:
            return bool(user.get(field))
        # For unknown actions, only allow admin
        return bool(user.get('ADMIN'))

    def verify_user_password(self, name: str, password: str) -> Optional[Dict]:
        """Verify username+password, return user dict (without hash) or None."""
        import hashlib
        rows = self._read('USER')
        expected_bytes = hashlib.md5(password.encode('utf-8')).digest()
        for r in rows:
            if r.get('HIDE'):
                continue
            if r.get('NAME', '').strip().lower() != name.strip().lower():
                continue
            digest = r.get('DIGEST', '')
            # Normalize: stored as bytes or as latin-1 decoded string
            if isinstance(digest, bytes):
                digest_bytes = digest
            elif isinstance(digest, str):
                digest_bytes = digest.encode('latin-1')
            else:
                continue
            if digest_bytes == expected_bytes:
                role = self._role_from_record(r)
                is_admin = role == 'Admin'
                return {
                    'ID': r.get('ID'),
                    'NAME': r.get('NAME', ''),
                    'DESCRIP': r.get('DESCRIP', ''),
                    'ADMIN': bool(r.get('ADMIN')),
                    'RIGHTS': r.get('RIGHTS', 0),
                    'role': role,
                    'WDUTIES': bool(r.get('WDUTIES')) if not is_admin else True,
                    'WABSENCES': bool(r.get('WABSENCES')) if not is_admin else True,
                    'WOVERTIMES': bool(r.get('WOVERTIMES')) if not is_admin else True,
                    'WNOTES': bool(r.get('WNOTES')) if not is_admin else True,
                    'WCYCLEASS': bool(r.get('WCYCLEASS')) if not is_admin else True,
                    'WPAST': bool(r.get('WPAST')) if not is_admin else True,
                    'WACCEMWND': bool(r.get('WACCEMWND')) if not is_admin else True,
                    'WACCGRWND': bool(r.get('WACCGRWND')) if not is_admin else True,
                    'BACKUP': bool(r.get('BACKUP')) if not is_admin else True,
                    'SHOWSTATS': bool(r.get('SHOWSTATS')) if not is_admin else True,
                    'ACCADMWND': is_admin,
                }
        return None

    # ── Cycles ─────────────────────────────────────────────────
    def get_cycles(self) -> List[Dict]:
        cycles = self._read('CYCLE')
        entries = self._read('CYENT')
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}

        for cycle in cycles:
            cyc_entries = [e for e in entries if e.get('CYCLEEID') == cycle.get('ID')]
            cyc_entries.sort(key=lambda x: x.get('INDEX', 0))
            cycle['entries'] = []
            for e in cyc_entries:
                sid = e.get('SHIFTID')
                shift = shifts_map.get(sid) if sid else None
                cycle['entries'].append({
                    'index': e.get('INDEX', 0),
                    'shift_id': sid,
                    'shift_name': shift.get('NAME', '') if shift else '',
                    'shift_short': shift.get('SHORTNAME', '') if shift else '',
                    'workplace_id': e.get('WORKPLACID'),
                })
        return cycles

    # ── Shift Cycles (rich) ───────────────────────────────────
    def get_shift_cycles(self) -> List[Dict]:
        """Return all shift cycles with their weekly schedule from CYCLE + CYENT."""
        cycles = self._read('CYCLE')
        entries = self._read('CYENT')
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}

        result = []
        for cycle in cycles:
            if cycle.get('HIDE'):
                continue
            cid = cycle.get('ID')
            size = cycle.get('SIZE', 1)  # number of weeks
            unit = cycle.get('UNIT', 1)  # 1 = weeks

            # Build flat day array for all weeks
            # INDEX in CYENT is the flat day offset (0 = Mon week1, 7 = Mon week2, ...)
            cyc_entries = {e['INDEX']: e for e in entries if e.get('CYCLEEID') == cid}

            weeks = []
            for w in range(size):
                week_days = []
                for d in range(7):
                    idx = w * 7 + d
                    entry = cyc_entries.get(idx)
                    if entry:
                        sid = entry.get('SHIFTID', 0)
                        shift = shifts_map.get(sid) if sid else None
                        week_days.append({
                            'index': idx,
                            'weekday': d,
                            'shift_id': sid or None,
                            'shift_name': shift.get('NAME', '') if shift else '',
                            'shift_short': shift.get('SHORTNAME', '') if shift else '',
                            'color_bk': bgr_to_hex(shift.get('COLORBK', 16777215)) if shift else '#FFFFFF',
                            'color_text': bgr_to_hex(shift.get('COLORTEXT', 0)) if shift else '#000000',
                            'workplace_id': entry.get('WORKPLACID') or None,
                        })
                    else:
                        week_days.append({
                            'index': idx,
                            'weekday': d,
                            'shift_id': None,
                            'shift_name': '',
                            'shift_short': '',
                            'color_bk': '#FFFFFF',
                            'color_text': '#000000',
                            'workplace_id': None,
                        })
                weeks.append(week_days)

            # Build a simple pattern string like "F/F/F/S/S/–/– | S/S/S/F/F/–/–"
            pattern_parts = []
            for week in weeks:
                part = '/'.join(d['shift_short'] or '–' for d in week)
                pattern_parts.append(part)
            pattern = '  |  '.join(pattern_parts)

            result.append({
                'ID': cid,
                'name': cycle.get('NAME', ''),
                'weeks': size,
                'unit': unit,
                'position': cycle.get('POSITION', 0),
                'pattern': pattern,
                'schedule': weeks,
            })

        result.sort(key=lambda x: x.get('position', 0))
        return result

    def get_shift_cycle(self, cycle_id: int) -> Optional[Dict]:
        """Return a single cycle by ID with full schedule."""
        for c in self.get_shift_cycles():
            if c['ID'] == cycle_id:
                return c
        return None

    # ── Shift Cycle CRUD ──────────────────────────────────────

    def create_shift_cycle(self, name: str, size_weeks: int) -> Dict:
        """Create a new shift cycle in 5CYCLE.DBF. Returns the new cycle dict."""
        filepath = self._table('CYCLE')
        fields = get_table_fields(filepath)
        new_id = self._next_id('CYCLE')
        rows = self._read('CYCLE')
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1
        record = {
            'ID': new_id,
            'NAME': name,
            'POSITION': max_pos,
            'SIZE': size_weeks,
            'UNIT': 1,       # 1 = weeks
            'HIDE': False,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'ID': new_id,
            'name': name,
            'weeks': size_weeks,
            'unit': 1,
            'position': max_pos,
            'pattern': '',
            'schedule': [],
        }

    def update_shift_cycle(self, cycle_id: int, name: str, size_weeks: int) -> Dict:
        """Update name and/or size of an existing shift cycle."""
        filepath = self._table('CYCLE')
        fields = get_table_fields(filepath)
        raw_idx, existing = self._find_record('CYCLE', cycle_id)
        if raw_idx is None:
            raise ValueError(f"Shift cycle {cycle_id} not found")
        update_record(filepath, fields, raw_idx, {'NAME': name, 'SIZE': size_weeks})
        return {'ID': cycle_id, 'name': name, 'weeks': size_weeks}

    def delete_shift_cycle(self, cycle_id: int) -> int:
        """Delete a shift cycle from 5CYCLE and all its entries from 5CYENT."""
        # Delete cycle entries first
        self.clear_cycle_entries(cycle_id)
        # Delete the cycle record
        raw_idx, _ = self._find_record('CYCLE', cycle_id)
        if raw_idx is None:
            return 0
        filepath = self._table('CYCLE')
        fields = get_table_fields(filepath)
        delete_record(filepath, fields, raw_idx)
        return 1

    def set_cycle_entry(self, cycle_id: int, index: int, shift_id: Optional[int]) -> None:
        """Create or update a 5CYENT entry for the given cycle and day index."""
        filepath = self._table('CYENT')
        fields = get_table_fields(filepath)
        matches = find_all_records(filepath, fields, CYCLEEID=cycle_id, INDEX=index)
        if matches:
            raw_idx, _ = matches[0]
            update_record(filepath, fields, raw_idx, {'SHIFTID': shift_id or 0})
        else:
            new_id = self._next_id('CYENT')
            record = {
                'ID': new_id,
                'CYCLEEID': cycle_id,
                'INDEX': index,
                'SHIFTID': shift_id or 0,
                'WORKPLACID': 0,
                'RESERVED': '',
            }
            append_record(filepath, fields, record)

    def clear_cycle_entries(self, cycle_id: int) -> int:
        """Delete all 5CYENT entries for the given cycle. Returns count deleted."""
        filepath = self._table('CYENT')
        fields = get_table_fields(filepath)
        matches = find_all_records(filepath, fields, CYCLEEID=cycle_id)
        count = 0
        for raw_idx, _ in matches:
            delete_record(filepath, fields, raw_idx)
            count += 1
        return count

    # ── Cycle Assignments ─────────────────────────────────────
    def get_cycle_assignments(self) -> List[Dict]:
        """Return all cycle assignments from CYASS."""
        rows = self._read('CYASS')
        result = []
        for r in rows:
            result.append({
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'cycle_id': r.get('CYCLEID'),
                'start': r.get('START', ''),
                'end': r.get('END', ''),
                'entrance': r.get('ENTRANCE', ''),
            })
        return result

    def get_cycle_assignment_for_employee(self, employee_id: int) -> Optional[Dict]:
        """Return the active cycle assignment for an employee, if any."""
        for a in self.get_cycle_assignments():
            if a['employee_id'] == employee_id:
                return a
        return None

    def assign_cycle(self, employee_id: int, cycle_id: int, start_date: str) -> Dict:
        """Assign a cycle to an employee (append to CYASS)."""
        filepath = self._table('CYASS')
        fields = get_table_fields(filepath)
        existing = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in existing), default=0)
        new_id = max_id + 1
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'CYCLEID': cycle_id,
            'START': start_date,
            'END': '',
            'ENTRANCE': start_date,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'id': new_id,
            'employee_id': employee_id,
            'cycle_id': cycle_id,
            'start': start_date,
            'end': '',
        }

    def remove_cycle_assignment(self, employee_id: int) -> int:
        """Remove cycle assignment(s) for an employee. Returns count removed."""
        filepath = self._table('CYASS')
        fields = get_table_fields(filepath)
        matches = find_all_records(filepath, fields, EMPLOYEEID=employee_id)
        count = 0
        for idx, _ in matches:
            delete_record(filepath, fields, idx)
            count += 1
        return count

    def generate_schedule_from_cycle(
        self,
        year: int,
        month: int,
        employee_ids: list = None,
        force: bool = False,
        dry_run: bool = False,
        respect_restrictions: bool = True,
    ) -> dict:
        """
        Befüllt den Dienstplan für year/month aus den Schichtmodell-Zuweisungen.
        employee_ids: None = alle MAs mit Zyklus-Zuweisung, sonst nur diese
        force: True = bestehende Einträge überschreiben
        dry_run: True = nur Vorschau (nichts wird geschrieben)
        respect_restrictions: True = Schicht-Sperren aus RESTR beachten
        Gibt zurück: {'created': N, 'skipped': N, 'skipped_restriction': N, 'errors': [...], 'preview': [...], 'report': {...}}
        """
        from datetime import date as _date
        import calendar as _cal

        # Collect all cycle assignments
        all_assignments = self.get_cycle_assignments()
        if employee_ids is not None:
            emp_id_set = set(employee_ids)
            all_assignments = [a for a in all_assignments if a['employee_id'] in emp_id_set]

        if not all_assignments:
            return {'created': 0, 'skipped': 0, 'skipped_restriction': 0, 'errors': [], 'preview': [], 'report': {}}

        # Build cycle lookup: cycle_id -> cycle record
        cycles_raw = self._read('CYCLE')
        cycle_map = {c['ID']: c for c in cycles_raw}

        # Build cycle entries lookup: cycle_id -> {index: shift_id}
        cyent_raw = self._read('CYENT')
        cycle_entries: dict = {}
        for e in cyent_raw:
            cid = e.get('CYCLEEID')
            idx = e.get('INDEX', 0)
            sid = e.get('SHIFTID', 0)
            if cid is None:
                continue
            if cid not in cycle_entries:
                cycle_entries[cid] = {}
            if sid:  # only non-zero shift IDs mean "has a shift"
                cycle_entries[cid][idx] = sid

        # Gather existing schedule entries for this month (all three tables)
        prefix = f"{year:04d}-{month:02d}"
        existing_entries: set = set()  # (employee_id, date_str)
        for table in ('MASHI', 'SPSHI', 'ABSEN'):
            for r in self._read(table):
                d = r.get('DATE', '')
                if d and d.startswith(prefix):
                    eid = r.get('EMPLOYEEID')
                    if eid is not None:
                        existing_entries.add((eid, d))

        # Build restrictions lookup: (employee_id, shift_id, weekday) -> True
        # weekday 0 = all days, 1-7 = Mon-Sun
        restrictions: set = set()  # (employee_id, shift_id, weekday)
        if respect_restrictions:
            try:
                for r in self._read('RESTR'):
                    eid = r.get('EMPLOYEEID')
                    sid = r.get('SHIFTID')
                    wday = r.get('WEEKDAY', 0) or 0
                    if eid is not None and sid:
                        restrictions.add((eid, sid, wday))
            except Exception:
                pass

        num_days = _cal.monthrange(year, month)[1]
        created = 0
        skipped = 0
        skipped_restriction = 0
        errors: list = []
        preview: list = []  # List of {employee_id, date, shift_id, status: 'new'|'skip'|'overwrite'|'restricted'}
        # Per-employee stats for optimization report
        emp_stats: dict = {}  # emp_id -> {'total': 0, 'weekend': 0, 'night': 0}

        # Build employee name lookup for preview
        try:
            employees_raw = self._read('EMPLO')
            emp_names = {e.get('ID'): f"{e.get('NAME', '')} {e.get('FIRSTNAME', '')}".strip() for e in employees_raw}
        except Exception:
            emp_names = {}

        # Build shift name lookup for preview + identify night shifts
        night_shift_ids: set = set()
        try:
            shifts_raw = self._read('SHIFT')
            shift_names = {s.get('ID'): s.get('SHORTNAME', '') or s.get('NAME', '') for s in shifts_raw}
            for s in shifts_raw:
                t = s.get('STARTEND0', '') or ''
                if '-' in t:
                    start_t = t.split('-')[0].strip()
                    try:
                        h = int(start_t.split(':')[0])
                        if h >= 20 or h < 6:
                            night_shift_ids.add(s.get('ID'))
                    except Exception:
                        pass
        except Exception:
            shift_names = {}

        # Deduplicate: keep only one assignment per employee (last one in list)
        emp_assignment: dict = {}
        for a in all_assignments:
            emp_assignment[a['employee_id']] = a

        for emp_id, assignment in emp_assignment.items():
            cycle_id = assignment.get('cycle_id')
            start_str = assignment.get('start', '') or ''

            if not start_str:
                errors.append(f"MA {emp_id}: Kein Startdatum für Zyklus-Zuweisung")
                continue

            # Parse start date (stored as 'YYYY-MM-DD' ISO format)
            try:
                start_date = _date.fromisoformat(start_str[:10])
            except ValueError:
                errors.append(f"MA {emp_id}: Ungültiges Startdatum '{start_str}'")
                continue

            cycle = cycle_map.get(cycle_id)
            if not cycle:
                errors.append(f"MA {emp_id}: Zyklus {cycle_id} nicht gefunden")
                continue

            size_weeks = int(cycle.get('SIZE', 1) or 1)
            cycle_length = size_weeks * 7
            entries_for_cycle = cycle_entries.get(cycle_id, {})

            for day in range(1, num_days + 1):
                target_date = _date(year, month, day)
                date_str = target_date.isoformat()

                delta = (target_date - start_date).days
                if delta < 0:
                    # Day is before cycle start → skip
                    continue

                position = delta % cycle_length
                shift_id = entries_for_cycle.get(position)
                if not shift_id:
                    # No shift defined for this position (Frei / day off) → skip
                    continue

                # Check restrictions: weekday 0=all, 1=Mon...7=Sun (iso_weekday 1-7)
                if respect_restrictions and restrictions:
                    iso_wd = target_date.isoweekday()  # 1=Mon, 7=Sun
                    restricted = (
                        (emp_id, shift_id, 0) in restrictions or
                        (emp_id, shift_id, iso_wd) in restrictions
                    )
                    if restricted:
                        skipped_restriction += 1
                        preview.append({
                            'employee_id': emp_id,
                            'employee_name': emp_names.get(emp_id, f'MA {emp_id}'),
                            'date': date_str,
                            'shift_id': shift_id,
                            'shift_name': shift_names.get(shift_id, str(shift_id)),
                            'status': 'restricted',
                        })
                        continue

                key = (emp_id, date_str)
                if key in existing_entries:
                    if not force:
                        skipped += 1
                        preview.append({
                            'employee_id': emp_id,
                            'employee_name': emp_names.get(emp_id, f'MA {emp_id}'),
                            'date': date_str,
                            'shift_id': shift_id,
                            'shift_name': shift_names.get(shift_id, str(shift_id)),
                            'status': 'skip',
                        })
                        continue
                    # force=True: delete existing entry first (unless dry_run)
                    if not dry_run:
                        try:
                            self.delete_schedule_entry(emp_id, date_str)
                            existing_entries.discard(key)
                        except Exception as e:
                            errors.append(f"MA {emp_id}, {date_str}: Fehler beim Löschen: {e}")
                            continue
                    preview.append({
                        'employee_id': emp_id,
                        'employee_name': emp_names.get(emp_id, f'MA {emp_id}'),
                        'date': date_str,
                        'shift_id': shift_id,
                        'shift_name': shift_names.get(shift_id, str(shift_id)),
                        'status': 'overwrite',
                    })
                else:
                    preview.append({
                        'employee_id': emp_id,
                        'employee_name': emp_names.get(emp_id, f'MA {emp_id}'),
                        'date': date_str,
                        'shift_id': shift_id,
                        'shift_name': shift_names.get(shift_id, str(shift_id)),
                        'status': 'new',
                    })

                # Track stats for optimization report
                if emp_id not in emp_stats:
                    emp_stats[emp_id] = {
                        'employee_id': emp_id,
                        'name': emp_names.get(emp_id, f'MA {emp_id}'),
                        'total': 0,
                        'weekend': 0,
                        'night': 0,
                    }
                emp_stats[emp_id]['total'] += 1
                if target_date.weekday() >= 5:
                    emp_stats[emp_id]['weekend'] += 1
                if shift_id in night_shift_ids:
                    emp_stats[emp_id]['night'] += 1

                if not dry_run:
                    try:
                        self.add_schedule_entry(emp_id, date_str, shift_id)
                        existing_entries.add(key)
                        created += 1
                    except Exception as e:
                        errors.append(f"MA {emp_id}, {date_str}: {e}")
                        preview.pop()  # remove from preview if write failed
                else:
                    if key not in existing_entries:
                        created += 1

        # Build optimization report
        report_employees = sorted(emp_stats.values(), key=lambda x: -x['total'])
        report: dict = {
            'employees': report_employees,
            'skipped_restriction': skipped_restriction,
        }
        if len(report_employees) >= 2:
            import statistics as _stats
            totals = [e['total'] for e in report_employees]
            weekends = [e['weekend'] for e in report_employees]
            nights = [e['night'] for e in report_employees]
            report['std_total'] = round(_stats.stdev(totals), 2) if len(totals) > 1 else 0
            report['std_weekend'] = round(_stats.stdev(weekends), 2) if len(weekends) > 1 else 0
            report['std_night'] = round(_stats.stdev(nights), 2) if len(nights) > 1 else 0
            # Gini coefficient for total shifts
            n = len(totals)
            mean = sum(totals) / n
            if mean > 0:
                gini = sum(abs(a - b) for a in totals for b in totals) / (2 * n * n * mean)
            else:
                gini = 0.0
            report['gini'] = round(gini, 4)
            report['fairness_label'] = 'sehr gut' if gini < 0.05 else ('gut' if gini < 0.1 else ('mittel' if gini < 0.2 else 'schlecht'))

        return {
            'created': created,
            'skipped': skipped,
            'skipped_restriction': skipped_restriction,
            'errors': errors,
            'preview': preview,
            'report': report,
        }

    # ── Staffing requirements ─────────────────────────────────
    def get_staffing(self, year: int, month: int) -> List[Dict]:
        """Return staffing requirements from 5SHDEM (min/max per weekday/shift/group)."""
        rows = self._read('SHDEM')
        return rows

    # ── Write: SPSHI entries (Einsatzplan) ───────────────────
    def add_spshi_entry(
        self,
        employee_id: int,
        date_str: str,
        name: str = '',
        shortname: str = '',
        shift_id: int = 0,
        workplace_id: int = 0,
        entry_type: int = 0,
        startend: str = '',
        duration: float = 0.0,
        colortext: int = 0,
        colorbar: int = 0,
        colorbk: int = 16777215,
    ) -> Dict:
        """Append a new record to 5SPSHI.DBF. Returns the created record."""
        filepath = self._table('SPSHI')
        fields = get_table_fields(filepath)
        new_id = self._next_id('SPSHI')
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'DATE': date_str,
            'NAME': name,
            'SHORTNAME': shortname,
            'SHIFTID': shift_id,
            'WORKPLACID': workplace_id,
            'TYPE': entry_type,
            'COLORTEXT': colortext,
            'COLORBAR': colorbar,
            'COLORBK': colorbk,
            'BOLD': 0,
            'STARTEND': startend,
            'DURATION': duration,
            'NOEXTRA': 0,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_spshi_entry(self, entry_id: int, data: dict) -> dict:
        """Update fields of an existing 5SPSHI record."""
        filepath = self._table('SPSHI')
        fields = get_table_fields(filepath)
        raw_idx, existing = self._find_record('SPSHI', entry_id)
        if raw_idx is None:
            raise ValueError(f"SPSHI entry {entry_id} not found")
        allowed = ('NAME', 'SHORTNAME', 'SHIFTID', 'WORKPLACID', 'DATE',
                   'TYPE', 'COLORTEXT', 'COLORBAR', 'COLORBK', 'STARTEND', 'DURATION')
        update_data = {k: v for k, v in data.items() if k in allowed}
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': entry_id, **update_data}

    def delete_spshi_entry_by_id(self, entry_id: int) -> int:
        """Delete a SPSHI entry by its ID. Returns 1 if found, 0 otherwise."""
        filepath = self._table('SPSHI')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('SPSHI', entry_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    def get_spshi_entries_for_day(self, date_str: str, group_id: Optional[int] = None) -> List[Dict]:
        """Return all SPSHI entries for a specific date, optionally filtered by group."""
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
        else:
            member_ids = None
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        workplaces_map = {w['ID']: w for w in self.get_workplaces(include_hidden=True)}
        result = []
        for r in self._read('SPSHI'):
            if r.get('DATE') != date_str:
                continue
            eid = r.get('EMPLOYEEID')
            if member_ids is not None and eid not in member_ids:
                continue
            shift = shifts_map.get(r.get('SHIFTID', 0)) if r.get('SHIFTID') else None
            wp = workplaces_map.get(r.get('WORKPLACID', 0)) if r.get('WORKPLACID') else None
            result.append({
                'id': r.get('ID'),
                'employee_id': eid,
                'date': date_str,
                'name': r.get('NAME', ''),
                'shortname': r.get('SHORTNAME', ''),
                'shift_id': r.get('SHIFTID', 0),
                'shift_name': shift.get('NAME', '') if shift else '',
                'shift_short': shift.get('SHORTNAME', '') if shift else '',
                'workplace_id': r.get('WORKPLACID', 0),
                'workplace_name': wp.get('NAME', '') if wp else '',
                'type': r.get('TYPE', 0),
                'startend': r.get('STARTEND', ''),
                'duration': float(r.get('DURATION', 0) or 0),
                'colortext': r.get('COLORTEXT', 0),
                'colorbar': r.get('COLORBAR', 0),
                'colorbk': r.get('COLORBK', 16777215),
                'color_bk': bgr_to_hex(r.get('COLORBK', 16777215)) if r.get('COLORBK') else '#FFFFFF',
                'color_text': bgr_to_hex(r.get('COLORTEXT', 0)) if r.get('COLORTEXT') else '#000000',
            })
        return result

    # ── Day schedule ──────────────────────────────────────────
    def get_schedule_day(self, date_str: str, group_id: Optional[int] = None) -> List[Dict]:
        """
        Return all employees with their assignment for a specific day.
        date_str: 'YYYY-MM-DD'
        """
        employees = self.get_employees(include_hidden=False)
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            employees = [e for e in employees if e['ID'] in member_ids]

        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}
        workplaces_map = {w['ID']: w for w in self.get_workplaces(include_hidden=True)}

        # Build assignment lookup for the day
        day_entries: Dict[int, Dict] = {}

        for r in self._read('MASHI'):
            if r.get('DATE') == date_str:
                eid = r.get('EMPLOYEEID')
                if eid:
                    day_entries[eid] = {
                        'kind': 'shift',
                        'shift_id': r.get('SHIFTID'),
                        'workplace_id': r.get('WORKPLACID'),
                        'leave_type_id': None,
                    }

        for r in self._read('SPSHI'):
            if r.get('DATE') == date_str:
                eid = r.get('EMPLOYEEID')
                if eid:
                    day_entries[eid] = {
                        'kind': 'special_shift',
                        'shift_id': r.get('SHIFTID'),
                        'workplace_id': r.get('WORKPLACID'),
                        'leave_type_id': None,
                        'custom_name': r.get('NAME', ''),
                        'custom_short': r.get('SHORTNAME', ''),
                        'color_bk': bgr_to_hex(r.get('COLORBK', 16777215)) if r.get('COLORBK') else None,
                        'color_text': bgr_to_hex(r.get('COLORTEXT', 0)) if r.get('COLORTEXT') else None,
                        'spshi_id': r.get('ID'),
                        'spshi_type': r.get('TYPE', 0),
                        'spshi_startend': r.get('STARTEND', ''),
                        'spshi_duration': float(r.get('DURATION', 0) or 0),
                    }

        for r in self._read('ABSEN'):
            if r.get('DATE') == date_str:
                eid = r.get('EMPLOYEEID')
                if eid:
                    day_entries[eid] = {
                        'kind': 'absence',
                        'shift_id': None,
                        'workplace_id': None,
                        'leave_type_id': r.get('LEAVETYPID'),
                    }

        result = []
        for emp in employees:
            eid = emp['ID']
            entry = day_entries.get(eid, {})
            kind = entry.get('kind')
            shift_id = entry.get('shift_id')
            leave_type_id = entry.get('leave_type_id')
            workplace_id = entry.get('workplace_id')

            shift_name = ''
            shift_short = ''
            color_bk = '#FFFFFF'
            color_text = '#000000'
            leave_name = ''
            display_name = ''

            if shift_id and shift_id in shifts_map:
                s = shifts_map[shift_id]
                shift_name = s.get('NAME', '')
                shift_short = s.get('SHORTNAME', '')
                color_bk = entry.get('color_bk') or bgr_to_hex(s.get('COLORBK', 16777215))
                color_text = entry.get('color_text') or bgr_to_hex(s.get('COLORTEXT', 0))
                display_name = shift_short or shift_name
            elif leave_type_id and leave_type_id in lt_map:
                lt = lt_map[leave_type_id]
                leave_name = lt.get('NAME', '')
                color_bk = bgr_to_hex(lt.get('COLORBK', 16777215))
                color_text = bgr_to_hex(lt.get('COLORBAR', 0))
                display_name = lt.get('SHORTNAME', lt.get('NAME', ''))
            elif kind == 'special_shift':
                color_bk = entry.get('color_bk', '#FFFFFF')
                color_text = entry.get('color_text', '#000000')
                display_name = entry.get('custom_short', entry.get('custom_name', ''))

            wp_name = ''
            if workplace_id and workplace_id in workplaces_map:
                wp_name = workplaces_map[workplace_id].get('NAME', '')

            result.append({
                'employee_id': eid,
                'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
                'employee_short': emp.get('SHORTNAME', ''),
                'shift_id': shift_id,
                'shift_name': shift_name,
                'shift_short': shift_short,
                'color_bk': color_bk,
                'color_text': color_text,
                'workplace_id': workplace_id,
                'workplace_name': wp_name,
                'kind': kind,
                'leave_name': leave_name,
                'display_name': display_name,
                'spshi_id': entry.get('spshi_id'),
                'spshi_type': entry.get('spshi_type'),
                'spshi_startend': entry.get('spshi_startend', ''),
                'spshi_duration': entry.get('spshi_duration', 0.0),
            })

        return result

    # ── Monthly statistics ────────────────────────────────────
    def get_statistics(self, year: int, month: int, group_id: Optional[int] = None) -> List[Dict]:
        """Return per-employee statistics for a month."""
        employees = self.get_employees(include_hidden=False)
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            employees = [e for e in employees if e['ID'] in member_ids]

        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}

        # Build employee→primary group mapping
        groups_all = self.get_groups()
        emp_group: Dict[int, str] = {}
        emp_group_id: Dict[int, int] = {}
        for grp in groups_all:
            try:
                members = self.get_group_members(grp['ID'])
                for mid in members:
                    if mid not in emp_group:
                        emp_group[mid] = grp.get('NAME', '')
                        emp_group_id[mid] = grp['ID']
            except Exception:
                pass

        prefix = f"{year:04d}-{month:02d}"
        num_days = calendar.monthrange(year, month)[1]

        # Count working days in month (Mon-Fri)
        working_days = sum(
            1 for d in range(1, num_days + 1)
            if datetime(year, month, d).weekday() < 5
        )

        # Collect schedule data for the month
        shift_hours: Dict[int, float] = {}  # employee_id -> sum of shift hours
        shifts_count: Dict[int, int] = {}   # employee_id -> number of shift entries
        absence_days: Dict[int, int] = {}   # employee_id -> count of absences
        vacation_used: Dict[int, int] = {}  # employee_id -> count of vacation days
        sick_days: Dict[int, int] = {}      # employee_id -> count of sick days

        for r in self._read('MASHI'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                eid = r.get('EMPLOYEEID')
                if eid:
                    sid = r.get('SHIFTID')
                    hrs = 0.0
                    if sid and sid in shifts_map:
                        s = shifts_map[sid]
                        # Use DURATION0 as default shift duration (hours)
                        hrs = float(s.get('DURATION0', 0) or 0)
                    shift_hours[eid] = shift_hours.get(eid, 0.0) + hrs
                    shifts_count[eid] = shifts_count.get(eid, 0) + 1

        for r in self._read('SPSHI'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                eid = r.get('EMPLOYEEID')
                if eid:
                    hrs = float(r.get('DURATION', 0) or 0)
                    shift_hours[eid] = shift_hours.get(eid, 0.0) + hrs
                    shifts_count[eid] = shifts_count.get(eid, 0) + 1

        for r in self._read('ABSEN'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                eid = r.get('EMPLOYEEID')
                if eid:
                    absence_days[eid] = absence_days.get(eid, 0) + 1
                    ltid = r.get('LEAVETYPID')
                    if ltid and ltid in lt_map:
                        lt = lt_map[ltid]
                        # Vacation: ENTITLED=1 or CHARGETYP=1
                        if lt.get('ENTITLED') or lt.get('CHARGETYP') == 1:
                            vacation_used[eid] = vacation_used.get(eid, 0) + 1
                        # Sick: detect by name keyword
                        lt_name = (lt.get('NAME', '') or '').lower()
                        lt_short = (lt.get('SHORTNAME', '') or '').lower()
                        if any(kw in lt_name or kw in lt_short for kw in ['krank', 'sick', 'ku']):
                            sick_days[eid] = sick_days.get(eid, 0) + 1

        result = []
        for emp in employees:
            eid = emp['ID']
            # Target hours: prefer HRSMONTH, fallback to HRSDAY * working_days
            target = float(emp.get('HRSMONTH') or 0)
            if target == 0:
                target = float(emp.get('HRSDAY') or 0) * working_days
            actual = shift_hours.get(eid, 0.0)
            abs_days = absence_days.get(eid, 0)
            vac = vacation_used.get(eid, 0)
            sick = sick_days.get(eid, 0)
            overtime = actual - target

            result.append({
                'employee_id': eid,
                'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
                'employee_short': emp.get('SHORTNAME', ''),
                'group_name': emp_group.get(eid, ''),
                'group_id': emp_group_id.get(eid, None),
                'target_hours': round(target, 2),
                'actual_hours': round(actual, 2),
                'shifts_count': shifts_count.get(eid, 0),
                'absence_days': abs_days,
                'overtime_hours': round(overtime, 2),
                'vacation_used': vac,
                'sick_days': sick,
            })

        return result

    # ── Year overview ─────────────────────────────────────────
    def get_schedule_year(self, year: int, employee_id: int) -> List[Dict]:
        """Return schedule summary per month for a given employee."""
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}

        # Collect all entries for this employee this year
        year_str = f"{year:04d}"
        monthly: Dict[int, Dict] = {}

        for m in range(1, 13):
            monthly[m] = {
                'month': m,
                'shifts': 0,
                'absences': 0,
                'target_hours': 0.0,
                'actual_hours': 0.0,
                'label_counts': {},
            }

        for r in self._read('MASHI'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if d and d.startswith(year_str):
                m = int(d[5:7])
                monthly[m]['shifts'] += 1
                sid = r.get('SHIFTID')
                if sid and sid in shifts_map:
                    s = shifts_map[sid]
                    hrs = float(s.get('DURATION0', 0) or 0)
                    monthly[m]['actual_hours'] += hrs
                    label = s.get('SHORTNAME', '?')
                    monthly[m]['label_counts'][label] = monthly[m]['label_counts'].get(label, 0) + 1

        for r in self._read('SPSHI'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if d and d.startswith(year_str):
                m = int(d[5:7])
                monthly[m]['shifts'] += 1
                monthly[m]['actual_hours'] += float(r.get('DURATION', 0) or 0)
                label = r.get('SHORTNAME', '?')
                monthly[m]['label_counts'][label] = monthly[m]['label_counts'].get(label, 0) + 1

        for r in self._read('ABSEN'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if d and d.startswith(year_str):
                m = int(d[5:7])
                monthly[m]['absences'] += 1
                ltid = r.get('LEAVETYPID')
                if ltid and ltid in lt_map:
                    label = lt_map[ltid].get('SHORTNAME', '?')
                    monthly[m]['label_counts'][label] = monthly[m]['label_counts'].get(label, 0) + 1

        # Fill target hours per month
        emp = self.get_employee(employee_id)
        if emp:
            workdays_list = emp.get('WORKDAYS_LIST', [])
            for m in range(1, 13):
                working_days = self._count_working_days(year, m, workdays_list)
                target = float(emp.get('HRSMONTH') or 0)
                if target == 0:
                    target = float(emp.get('HRSDAY') or 0) * working_days
                monthly[m]['target_hours'] = round(target, 2)
                monthly[m]['actual_hours'] = round(monthly[m]['actual_hours'], 2)

        return list(monthly.values())

    # ── Week schedule ─────────────────────────────────────────
    def get_schedule_week(self, date_str: str, group_id: Optional[int] = None) -> Dict:
        """
        Return schedule for the full ISO week (Mon–Sun) that contains date_str.
        Returns { week_start, week_end, days: [{ date, entries: [...] }] }
        """
        from datetime import timedelta
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        monday = dt - timedelta(days=dt.weekday())
        week_dates = [(monday + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

        employees = self.get_employees(include_hidden=False)
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            employees = [e for e in employees if e['ID'] in member_ids]

        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}
        workplaces_map = {w['ID']: w for w in self.get_workplaces(include_hidden=True)}
        emp_map = {e['ID']: e for e in employees}
        allowed_ids = set(emp_map.keys())

        week_set = set(week_dates)

        # Build lookup: date -> employee_id -> entry
        day_entries: Dict[str, Dict[int, Dict]] = {d: {} for d in week_dates}

        for r in self._read('MASHI'):
            d = r.get('DATE', '')
            if d in week_set:
                eid = r.get('EMPLOYEEID')
                if eid in allowed_ids:
                    day_entries[d][eid] = {
                        'kind': 'shift',
                        'shift_id': r.get('SHIFTID'),
                        'workplace_id': r.get('WORKPLACID'),
                        'leave_type_id': None,
                    }

        for r in self._read('SPSHI'):
            d = r.get('DATE', '')
            if d in week_set:
                eid = r.get('EMPLOYEEID')
                if eid in allowed_ids:
                    day_entries[d][eid] = {
                        'kind': 'special_shift',
                        'shift_id': r.get('SHIFTID'),
                        'workplace_id': r.get('WORKPLACID'),
                        'leave_type_id': None,
                        'custom_name': r.get('NAME', ''),
                        'custom_short': r.get('SHORTNAME', ''),
                        'color_bk': bgr_to_hex(r.get('COLORBK', 16777215)) if r.get('COLORBK') else None,
                        'color_text': bgr_to_hex(r.get('COLORTEXT', 0)) if r.get('COLORTEXT') else None,
                        'spshi_id': r.get('ID'),
                        'spshi_type': r.get('TYPE', 0),
                        'spshi_startend': r.get('STARTEND', ''),
                        'spshi_duration': float(r.get('DURATION', 0) or 0),
                    }

        for r in self._read('ABSEN'):
            d = r.get('DATE', '')
            if d in week_set:
                eid = r.get('EMPLOYEEID')
                if eid in allowed_ids:
                    day_entries[d][eid] = {
                        'kind': 'absence',
                        'shift_id': None,
                        'workplace_id': None,
                        'leave_type_id': r.get('LEAVETYPID'),
                    }

        def _build_entry(emp: Dict, entry: Dict) -> Dict:
            eid = emp['ID']
            kind = entry.get('kind')
            shift_id = entry.get('shift_id')
            leave_type_id = entry.get('leave_type_id')
            workplace_id = entry.get('workplace_id')

            shift_name = ''
            shift_short = ''
            color_bk = '#FFFFFF'
            color_text = '#000000'
            leave_name = ''
            display_name = ''

            if shift_id and shift_id in shifts_map:
                s = shifts_map[shift_id]
                shift_name = s.get('NAME', '')
                shift_short = s.get('SHORTNAME', '')
                color_bk = entry.get('color_bk') or bgr_to_hex(s.get('COLORBK', 16777215))
                color_text = entry.get('color_text') or bgr_to_hex(s.get('COLORTEXT', 0))
                display_name = shift_short or shift_name
            elif leave_type_id and leave_type_id in lt_map:
                lt = lt_map[leave_type_id]
                leave_name = lt.get('NAME', '')
                color_bk = bgr_to_hex(lt.get('COLORBK', 16777215))
                color_text = bgr_to_hex(lt.get('COLORBAR', 0))
                display_name = lt.get('SHORTNAME', lt.get('NAME', ''))
            elif kind == 'special_shift':
                color_bk = entry.get('color_bk', '#FFFFFF')
                color_text = entry.get('color_text', '#000000')
                display_name = entry.get('custom_short', entry.get('custom_name', ''))

            wp_name = ''
            if workplace_id and workplace_id in workplaces_map:
                wp_name = workplaces_map[workplace_id].get('NAME', '')

            return {
                'employee_id': eid,
                'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
                'employee_short': emp.get('SHORTNAME', ''),
                'shift_id': shift_id,
                'shift_name': shift_name,
                'shift_short': shift_short,
                'color_bk': color_bk,
                'color_text': color_text,
                'workplace_id': workplace_id,
                'workplace_name': wp_name,
                'kind': kind,
                'leave_name': leave_name,
                'display_name': display_name,
                'spshi_id': entry.get('spshi_id'),
                'spshi_type': entry.get('spshi_type'),
                'spshi_startend': entry.get('spshi_startend', ''),
                'spshi_duration': entry.get('spshi_duration', 0.0),
            }

        days = []
        for d in week_dates:
            entries = []
            for emp in employees:
                eid = emp['ID']
                entry = day_entries[d].get(eid, {})
                if entry:
                    entries.append(_build_entry(emp, entry))
                else:
                    entries.append({
                        'employee_id': eid,
                        'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
                        'employee_short': emp.get('SHORTNAME', ''),
                        'shift_id': None,
                        'shift_name': '',
                        'shift_short': '',
                        'color_bk': '#FFFFFF',
                        'color_text': '#000000',
                        'workplace_id': None,
                        'workplace_name': '',
                        'kind': None,
                        'leave_name': '',
                        'display_name': '',
                        'spshi_id': None,
                        'spshi_type': None,
                        'spshi_startend': '',
                        'spshi_duration': 0.0,
                    })
            days.append({'date': d, 'entries': entries})

        return {
            'week_start': week_dates[0],
            'week_end': week_dates[6],
            'days': days,
        }

    # ── Write: add schedule entry ─────────────────────────────
    def add_schedule_entry(self, employee_id: int, date_str: str, shift_id: int) -> Dict:
        """Write a new schedule entry to MASHI.

        Raises ValueError if an entry for this employee+date already exists in MASHI.
        Callers that want upsert semantics should call delete_schedule_entry first.
        """
        filepath = self._table('MASHI')
        fields = get_table_fields(filepath)
        # Guard against duplicate entries (same employee + same date)
        duplicates = find_all_records(filepath, fields, EMPLOYEEID=employee_id, DATE=date_str)
        if duplicates:
            raise ValueError(
                f"Schedule entry for employee {employee_id} on {date_str} already exists. "
                "Delete it first or use the bulk/update endpoint."
            )
        # Get max existing ID
        existing = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in existing), default=0)
        new_id = max_id + 1
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'DATE': date_str,
            'SHIFTID': shift_id,
            'WORKPLACID': 0,
            'TYPE': 0,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return record

    # ── Write: delete schedule entry ──────────────────────────
    def delete_schedule_entry(self, employee_id: int, date_str: str) -> int:
        """Delete schedule entries for employee+date from MASHI, SPSHI, ABSEN. Returns count deleted."""
        count = 0
        for table in ['MASHI', 'SPSHI', 'ABSEN']:
            filepath = self._table(table)
            fields = get_table_fields(filepath)
            matches = find_all_records(filepath, fields, EMPLOYEEID=employee_id, DATE=date_str)
            for idx, _ in matches:
                delete_record(filepath, fields, idx)
                count += 1
        return count

    def delete_shift_only(self, employee_id: int, date_str: str) -> int:
        """Delete only shift entries (MASHI, SPSHI) for employee+date, leaving absences intact."""
        count = 0
        for table in ['MASHI', 'SPSHI']:
            filepath = self._table(table)
            fields = get_table_fields(filepath)
            matches = find_all_records(filepath, fields, EMPLOYEEID=employee_id, DATE=date_str)
            for idx, _ in matches:
                delete_record(filepath, fields, idx)
                count += 1
        return count

    def delete_absence_only(self, employee_id: int, date_str: str) -> int:
        """Delete only absence entries (ABSEN) for employee+date, leaving shifts intact."""
        count = 0
        filepath = self._table('ABSEN')
        fields = get_table_fields(filepath)
        matches = find_all_records(filepath, fields, EMPLOYEEID=employee_id, DATE=date_str)
        for idx, _ in matches:
            delete_record(filepath, fields, idx)
            count += 1
        return count

    # ── Write: add absence ────────────────────────────────────
    def add_absence(self, employee_id: int, date_str: str, leave_type_id: int) -> Dict:
        """Write a new absence entry to ABSEN.

        Raises ValueError if an absence for this employee+date already exists
        (regardless of leave type — one absence per day per employee).
        """
        filepath = self._table('ABSEN')
        fields = get_table_fields(filepath)
        # Guard against duplicate absences for the same employee on the same date
        duplicates = find_all_records(filepath, fields, EMPLOYEEID=employee_id, DATE=date_str)
        if duplicates:
            raise ValueError(
                f"Absence for employee {employee_id} on {date_str} already exists."
            )
        existing = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in existing), default=0)
        new_id = max_id + 1
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'DATE': date_str,
            'LEAVETYPID': leave_type_id,
            'TYPE': 0,
            'INTERVAL': 0,
            'START': 0,
            'END': 0,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return record

    # ── Staffing requirements (SHDEM + DADEM) ────────────────
    def get_staffing_requirements(self, year: Optional[int] = None, month: Optional[int] = None) -> Dict:
        """Return staffing requirements: SHDEM (shift min/max per weekday) + DADEM (daily totals)."""
        shdem_rows = self._read('SHDEM')
        dadem_rows = self._read('DADEM')
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}

        # Enrich SHDEM rows with shift info
        enriched_shdem = []
        for r in shdem_rows:
            sid = r.get('SHIFTID')
            shift = shifts_map.get(sid) if sid else None
            enriched_shdem.append({
                'id': r.get('ID'),
                'group_id': r.get('GROUPID'),
                'weekday': r.get('WEEKDAY'),  # 0=Mon..6=Sun (assumed)
                'shift_id': sid,
                'shift_name': shift.get('NAME', '') if shift else '',
                'shift_short': shift.get('SHORTNAME', '') if shift else '',
                'color_bk': bgr_to_hex(shift.get('COLORBK', 16777215)) if shift else '#FFFFFF',
                'color_text': bgr_to_hex(shift.get('COLORTEXT', 0)) if shift else '#000000',
                'workplace_id': r.get('WORKPLACID'),
                'min': r.get('MIN', 0),
                'max': r.get('MAX', 0),
            })

        return {
            'shift_requirements': enriched_shdem,
            'daily_requirements': dadem_rows,
        }

    # ── Notes ─────────────────────────────────────────────────
    def get_notes(self, date: Optional[str] = None, employee_id: Optional[int] = None) -> List[Dict]:
        """Return notes, optionally filtered by date and/or employee."""
        rows = self._read('NOTE')
        result = []
        for r in rows:
            if date and r.get('DATE') != date:
                continue
            if employee_id is not None and r.get('EMPLOYEEID') != employee_id:
                continue
            result.append({
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'date': r.get('DATE', ''),
                'text1': r.get('TEXT1', ''),
                'text2': r.get('TEXT2', ''),
                'category': (r.get('RESERVED') or '').strip(),
            })
        return result

    def add_note(self, date: str, text: str, employee_id: int = 0, text2: str = '', category: str = '') -> Dict:
        """Append a note to 5NOTE."""
        filepath = self._table('NOTE')
        fields = get_table_fields(filepath)
        existing = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in existing), default=0)
        new_id = max_id + 1
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'DATE': date,
            'TEXT1': text[:252] if text else '',
            'TEXT2': text2[:252] if text2 else '',
            'RESERVED': category[:20] if category else '',
        }
        append_record(filepath, fields, record)
        return {
            'id': new_id,
            'employee_id': employee_id,
            'date': date,
            'text1': text,
            'text2': text2,
            'category': category,
        }

    def delete_note(self, note_id: int) -> int:
        """Delete a note by ID from 5NOTE."""
        filepath = self._table('NOTE')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('NOTE', note_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    def update_note(self, note_id: int, text1: Optional[str] = None,
                    text2: Optional[str] = None, employee_id: Optional[int] = None,
                    date: Optional[str] = None, category: Optional[str] = None) -> Optional[Dict]:
        """Update fields of an existing note."""
        filepath = self._table('NOTE')
        fields = get_table_fields(filepath)
        raw_idx, record = self._find_record('NOTE', note_id)
        if raw_idx is None:
            return None
        update_data = {}
        if text1 is not None:
            update_data['TEXT1'] = text1[:252]
        if text2 is not None:
            update_data['TEXT2'] = text2[:252]
        if employee_id is not None:
            update_data['EMPLOYEEID'] = employee_id
        if date is not None:
            update_data['DATE'] = date
        if category is not None:
            update_data['RESERVED'] = category[:20]
        update_record(filepath, fields, raw_idx, update_data)
        # Return updated record
        _, updated = self._find_record('NOTE', note_id)
        if updated:
            return {
                'id': updated.get('ID'),
                'employee_id': updated.get('EMPLOYEEID'),
                'date': updated.get('DATE', ''),
                'text1': updated.get('TEXT1', ''),
                'text2': updated.get('TEXT2', ''),
                'category': (updated.get('RESERVED') or '').strip(),
            }
        return None

    # ── Periods ───────────────────────────────────────────────
    def get_periods(self, group_id: Optional[int] = None) -> List[Dict]:
        """Return accounting periods from PERIO."""
        rows = self._read('PERIO')
        result = []
        for r in rows:
            if group_id is not None and r.get('GROUPID') != group_id:
                continue
            result.append({
                'id': r.get('ID'),
                'group_id': r.get('GROUPID'),
                'start': r.get('START', ''),
                'end': r.get('END', ''),
                'color': bgr_to_hex(r.get('COLOR', 16777215)) if r.get('COLOR') else None,
                'description': r.get('DESCRIPT', ''),
            })
        result.sort(key=lambda x: x.get('start', ''))
        return result

    # ── Write: Employees ──────────────────────────────────────
    def _next_id(self, table_name: str) -> int:
        """Return max(ID)+1 for a table."""
        rows = self._read(table_name)
        return max((r.get('ID', 0) or 0 for r in rows), default=0) + 1

    def _find_record(self, table_name: str, record_id: int):
        """Return (raw_index, record) for the record with given ID, or (None, None)."""
        filepath = self._table(table_name)
        fields = get_table_fields(filepath)
        matches = find_all_records(filepath, fields, ID=record_id)
        if matches:
            return matches[0]
        return None, None

    def create_employee(self, data: dict) -> dict:
        filepath = self._table('EMPL')
        fields = get_table_fields(filepath)
        new_id = self._next_id('EMPL')
        rows = self._read('EMPL')
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1
        # Uniqueness check: SHORTNAME must be unique among active employees (if provided)
        shortname = (data.get('SHORTNAME') or '').strip().upper()
        if shortname:
            for row in rows:
                if row.get('HIDE'):
                    continue
                if (row.get('SHORTNAME') or '').strip().upper() == shortname:
                    raise ValueError(f"DUPLICATE:SHORTNAME:{shortname}")
        field_names = {f['name'] for f in fields}
        record = {
            'ID': new_id,
            'POSITION': data.get('POSITION', max_pos),
            'NUMBER': data.get('NUMBER', ''),
            'NAME': data.get('NAME', ''),
            'FIRSTNAME': data.get('FIRSTNAME', ''),
            'SHORTNAME': data.get('SHORTNAME', ''),
            'SEX': data.get('SEX', 0),
            'HRSDAY': data.get('HRSDAY', 0.0),
            'HRSWEEK': data.get('HRSWEEK', 0.0),
            'HRSMONTH': data.get('HRSMONTH', 0.0),
            'WORKDAYS': data.get('WORKDAYS', '1 1 1 1 1 0 0 0'),
            'HIDE': 1 if data.get('HIDE') else 0,
            'RESERVED': '',
        }
        # Add extended fields if present in DBF schema
        for key in ('HRSTOTAL', 'BOLD', 'CALCBASE', 'DEDUCTHOL',
                    'SALUTATION', 'STREET', 'ZIP', 'TOWN', 'PHONE', 'EMAIL', 'FUNCTION',
                    'BIRTHDAY', 'EMPSTART', 'EMPEND',
                    'NOTE1', 'NOTE2', 'NOTE3', 'NOTE4',
                    'ARBITR1', 'ARBITR2', 'ARBITR3',
                    'CFGLABEL', 'CBKLABEL', 'CBKSCHED'):
            if key in field_names and key in data and data[key] is not None:
                record[key] = data[key]
        append_record(filepath, fields, record)
        self._invalidate_cache('EMPL')
        return {**record, 'id': new_id}

    def update_employee(self, emp_id: int, data: dict) -> dict:
        filepath = self._table('EMPL')
        fields = get_table_fields(filepath)
        field_names = {f['name'] for f in fields}
        raw_idx, existing = self._find_record('EMPL', emp_id)
        if raw_idx is None:
            raise ValueError(f"Employee {emp_id} not found")
        update_data = {}
        all_updatable = (
            'NAME', 'FIRSTNAME', 'SHORTNAME', 'NUMBER', 'SEX',
            'HRSDAY', 'HRSWEEK', 'HRSMONTH', 'HRSTOTAL',
            'WORKDAYS', 'HIDE', 'BOLD', 'POSITION',
            'SALUTATION', 'STREET', 'ZIP', 'TOWN', 'PHONE', 'EMAIL', 'FUNCTION',
            'BIRTHDAY', 'EMPSTART', 'EMPEND',
            'CALCBASE', 'DEDUCTHOL',
            'NOTE1', 'NOTE2', 'NOTE3', 'NOTE4',
            'ARBITR1', 'ARBITR2', 'ARBITR3',
            'CFGLABEL', 'CBKLABEL', 'CBKSCHED',
            'PHOTO',
        )
        for key in all_updatable:
            if key in data and data[key] is not None and key in field_names:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': emp_id, **update_data}

    def change_password(self, user_id: int, new_password_plain: str) -> bool:
        """Change a user's password. Returns True if successful, False if user not found."""
        filepath = self._table('USER')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('USER', user_id)
        if raw_idx is None:
            return False
        digest = self._hash_password(new_password_plain)
        update_record(filepath, fields, raw_idx, {'DIGEST': digest})
        return True

    def delete_employee(self, emp_id: int) -> int:
        """Soft-delete (hide) an employee and cascade-delete their related records.

        Cascades:
        - MASHI, SPSHI, ABSEN: all entries for this employee are hard-deleted
        - BOOK: all bookings for this employee are hard-deleted
        - Wishes (JSON): all wishes for this employee are removed
        - CYASS: cycle assignments for this employee are removed
        """
        filepath = self._table('EMPL')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('EMPL', emp_id)
        if raw_idx is None:
            return 0
        update_record(filepath, fields, raw_idx, {'HIDE': 1})
        # Cascade: remove schedule, absence, booking, cycle-assignment records
        for table in ('MASHI', 'SPSHI', 'ABSEN'):
            t_path = self._table(table)
            t_fields = get_table_fields(t_path)
            matches = find_all_records(t_path, t_fields, EMPLOYEEID=emp_id)
            for idx, _ in reversed(matches):  # reverse to keep indices stable
                delete_record(t_path, t_fields, idx)
        # BOOK table uses EMPLOYEEID too
        book_path = self._table('BOOK')
        book_fields = get_table_fields(book_path)
        book_matches = find_all_records(book_path, book_fields, EMPLOYEEID=emp_id)
        for idx, _ in reversed(book_matches):
            delete_record(book_path, book_fields, idx)
        # CYASS: cycle assignments
        cyass_path = self._table('CYASS')
        cyass_fields = get_table_fields(cyass_path)
        cyass_matches = find_all_records(cyass_path, cyass_fields, EMPLOYEEID=emp_id)
        for idx, _ in reversed(cyass_matches):
            delete_record(cyass_path, cyass_fields, idx)
        # Wishes (JSON file)
        wishes_path = self._wishes_path()
        if os.path.exists(wishes_path):
            try:
                import json as _json
                with open(wishes_path, 'r', encoding='utf-8') as f:
                    wishes = _json.load(f)
                wishes = [w for w in wishes if w.get('employee_id') != emp_id]
                with open(wishes_path, 'w', encoding='utf-8') as f:
                    _json.dump(wishes, f, ensure_ascii=False)
            except Exception:
                pass  # non-fatal: wishes file might not exist or be malformed
        self._invalidate_cache('EMPL')
        return 1

    # ── Write: Groups ─────────────────────────────────────────
    def create_group(self, data: dict) -> dict:
        filepath = self._table('GROUP')
        fields = get_table_fields(filepath)
        field_names = {f['name'] for f in fields}
        new_id = self._next_id('GROUP')
        rows = self._read('GROUP')
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1
        record = {
            'ID': new_id,
            'NAME': data.get('NAME', ''),
            'SHORTNAME': data.get('SHORTNAME', ''),
            'SUPERID': data.get('SUPERID', 0),
            'POSITION': data.get('POSITION', max_pos),
            'HIDE': 1 if data.get('HIDE') else 0,
            'RESERVED': '',
        }
        for key in ('BOLD', 'DAILYDEM', 'ARBITR', 'CFGLABEL', 'CBKLABEL', 'CBKSCHED'):
            if key in field_names and key in data and data[key] is not None:
                record[key] = data[key]
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_group(self, group_id: int, data: dict) -> dict:
        filepath = self._table('GROUP')
        fields = get_table_fields(filepath)
        field_names = {f['name'] for f in fields}
        raw_idx, _ = self._find_record('GROUP', group_id)
        if raw_idx is None:
            raise ValueError(f"Group {group_id} not found")
        update_data = {}
        for key in ('NAME', 'SHORTNAME', 'SUPERID', 'POSITION', 'HIDE',
                    'BOLD', 'DAILYDEM', 'ARBITR', 'CFGLABEL', 'CBKLABEL', 'CBKSCHED'):
            if key in data and data[key] is not None and key in field_names:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': group_id, **update_data}

    def delete_group(self, group_id: int) -> int:
        filepath = self._table('GROUP')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('GROUP', group_id)
        if raw_idx is None:
            return 0
        update_record(filepath, fields, raw_idx, {'HIDE': 1})
        return 1

    def add_group_member(self, group_id: int, employee_id: int) -> dict:
        filepath = self._table('GRASG')
        fields = get_table_fields(filepath)
        # Check not already a member
        matches = find_all_records(filepath, fields, GROUPID=group_id, EMPLOYEEID=employee_id)
        if matches:
            return {'id': matches[0][1].get('ID'), 'group_id': group_id, 'employee_id': employee_id}
        new_id = self._next_id('GRASG')
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'GROUPID': group_id,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return record

    def remove_group_member(self, group_id: int, employee_id: int) -> int:
        filepath = self._table('GRASG')
        fields = get_table_fields(filepath)
        matches = find_all_records(filepath, fields, GROUPID=group_id, EMPLOYEEID=employee_id)
        count = 0
        for raw_idx, _ in matches:
            delete_record(filepath, fields, raw_idx)
            count += 1
        return count

    # ── Write: Shifts ─────────────────────────────────────────
    def create_shift(self, data: dict) -> dict:
        filepath = self._table('SHIFT')
        fields = get_table_fields(filepath)
        field_names = {f['name'] for f in fields}
        new_id = self._next_id('SHIFT')
        rows = self._read('SHIFT')
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1
        # Uniqueness check: NAME must be unique among active shifts
        name_lower = (data.get('NAME') or '').strip().lower()
        for row in rows:
            if row.get('HIDE'):
                continue
            if (row.get('NAME') or '').strip().lower() == name_lower:
                raise ValueError(f"DUPLICATE:SHIFTNAME:{data.get('NAME')}")
        record = {
            'ID': new_id,
            'NAME': data.get('NAME', ''),
            'SHORTNAME': data.get('SHORTNAME', ''),
            'POSITION': data.get('POSITION', max_pos),
            'COLORTEXT': data.get('COLORTEXT', 0),
            'COLORBAR': data.get('COLORBAR', 0),
            'COLORBK': data.get('COLORBK', 16777215),
            'DURATION0': data.get('DURATION0', 0.0),
            'HIDE': 1 if data.get('HIDE') else 0,
            'RESERVED': '',
        }
        # Add per-weekday duration/startend fields if present in schema
        for i in range(1, 8):
            dk = f'DURATION{i}'
            sk = f'STARTEND{i}'
            if dk in field_names and dk in data and data[dk] is not None:
                record[dk] = data[dk]
            if sk in field_names and sk in data and data[sk] is not None:
                record[sk] = data[sk]
        if 'STARTEND0' in field_names and 'STARTEND0' in data and data['STARTEND0'] is not None:
            record['STARTEND0'] = data['STARTEND0']
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_shift(self, shift_id: int, data: dict) -> dict:
        filepath = self._table('SHIFT')
        fields = get_table_fields(filepath)
        field_names = {f['name'] for f in fields}
        raw_idx, _ = self._find_record('SHIFT', shift_id)
        if raw_idx is None:
            raise ValueError(f"Shift {shift_id} not found")
        update_data = {}
        base_keys = ('NAME', 'SHORTNAME', 'POSITION', 'COLORTEXT', 'COLORBAR', 'COLORBK', 'DURATION0', 'HIDE')
        for key in base_keys:
            if key in data:
                update_data[key] = data[key]
        # Per-weekday duration/startend fields
        for i in range(8):
            dk = f'DURATION{i}'
            sk = f'STARTEND{i}'
            if dk in data and dk in field_names:
                update_data[dk] = data[dk]
            if sk in data and sk in field_names:
                update_data[sk] = data[sk]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': shift_id, **update_data}

    def hide_shift(self, shift_id: int) -> int:
        filepath = self._table('SHIFT')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('SHIFT', shift_id)
        if raw_idx is None:
            return 0
        update_record(filepath, fields, raw_idx, {'HIDE': 1})
        return 1

    # ── Write: Leave Types ────────────────────────────────────
    def create_leave_type(self, data: dict) -> dict:
        filepath = self._table('LEAVT')
        fields = get_table_fields(filepath)
        new_id = self._next_id('LEAVT')
        rows = self._read('LEAVT')
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1
        record = {
            'ID': new_id,
            'NAME': data.get('NAME', ''),
            'SHORTNAME': data.get('SHORTNAME', ''),
            'POSITION': data.get('POSITION', max_pos),
            'COLORTEXT': data.get('COLORTEXT', 0),
            'COLORBAR': data.get('COLORBAR', 0),
            'COLORBK': data.get('COLORBK', 16777215),
            'ENTITLED': 1 if data.get('ENTITLED') else 0,
            'STDENTIT': data.get('STDENTIT', 0.0),
            'HIDE': 1 if data.get('HIDE') else 0,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_leave_type(self, lt_id: int, data: dict) -> dict:
        filepath = self._table('LEAVT')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('LEAVT', lt_id)
        if raw_idx is None:
            raise ValueError(f"LeaveType {lt_id} not found")
        update_data = {}
        for key in ('NAME', 'SHORTNAME', 'POSITION', 'COLORTEXT', 'COLORBAR', 'COLORBK', 'ENTITLED', 'STDENTIT', 'HIDE'):
            if key in data:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': lt_id, **update_data}

    def hide_leave_type(self, lt_id: int) -> int:
        filepath = self._table('LEAVT')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('LEAVT', lt_id)
        if raw_idx is None:
            return 0
        update_record(filepath, fields, raw_idx, {'HIDE': 1})
        return 1

    # ── Write: Holidays ───────────────────────────────────────
    def create_holiday(self, data: dict) -> dict:
        filepath = self._table('HOLID')
        fields = get_table_fields(filepath)
        new_id = self._next_id('HOLID')
        record = {
            'ID': new_id,
            'DATE': data.get('DATE', ''),
            'NAME': data.get('NAME', ''),
            'INTERVAL': data.get('INTERVAL', 0),
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_holiday(self, holiday_id: int, data: dict) -> dict:
        filepath = self._table('HOLID')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('HOLID', holiday_id)
        if raw_idx is None:
            raise ValueError(f"Holiday {holiday_id} not found")
        update_data = {}
        for key in ('DATE', 'NAME', 'INTERVAL'):
            if key in data:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': holiday_id, **update_data}

    def delete_holiday(self, holiday_id: int) -> int:
        filepath = self._table('HOLID')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('HOLID', holiday_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    # ── Write: Workplaces ─────────────────────────────────────
    def create_workplace(self, data: dict) -> dict:
        filepath = self._table('WOPL')
        fields = get_table_fields(filepath)
        new_id = self._next_id('WOPL')
        rows = self._read('WOPL')
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1
        record = {
            'ID': new_id,
            'NAME': data.get('NAME', ''),
            'SHORTNAME': data.get('SHORTNAME', ''),
            'POSITION': data.get('POSITION', max_pos),
            'COLORTEXT': data.get('COLORTEXT', 0),
            'COLORBAR': data.get('COLORBAR', 0),
            'COLORBK': data.get('COLORBK', 16777215),
            'HIDE': 1 if data.get('HIDE') else 0,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_workplace(self, wp_id: int, data: dict) -> dict:
        filepath = self._table('WOPL')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('WOPL', wp_id)
        if raw_idx is None:
            raise ValueError(f"Workplace {wp_id} not found")
        update_data = {}
        for key in ('NAME', 'SHORTNAME', 'POSITION', 'COLORTEXT', 'COLORBAR', 'COLORBK', 'HIDE'):
            if key in data:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': wp_id, **update_data}

    def hide_workplace(self, wp_id: int) -> int:
        filepath = self._table('WOPL')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('WOPL', wp_id)
        if raw_idx is None:
            return 0
        update_record(filepath, fields, raw_idx, {'HIDE': 1})
        return 1

    # ── Workplace ↔ Employee Assignments (sidecar JSON) ────────
    def _assignments_path(self) -> str:
        return os.path.join(self.db_path, 'workplace_assignments.json')

    def _load_assignments(self) -> Dict[str, List[int]]:
        """Load {workplace_id_str: [employee_id, ...]} from sidecar JSON."""
        import json
        path = self._assignments_path()
        if not os.path.exists(path):
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_assignments(self, data: Dict[str, List[int]]) -> None:
        import json
        path = self._assignments_path()
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

    def get_workplace_employees(self, wp_id: int) -> List[Dict]:
        """Return employees explicitly assigned to a workplace (sidecar JSON)."""
        assignments = self._load_assignments()
        emp_ids = assignments.get(str(wp_id), [])
        if not emp_ids:
            return []
        emps = {e['ID']: e for e in self.get_employees(include_hidden=True)}
        result = []
        for eid in emp_ids:
            e = emps.get(eid)
            if e:
                result.append({
                    'ID': e.get('ID'),
                    'NAME': e.get('NAME', ''),
                    'FIRSTNAME': e.get('FIRSTNAME', ''),
                    'SHORTNAME': e.get('SHORTNAME', ''),
                    'NUMBER': e.get('NUMBER', ''),
                })
        return result

    def assign_employee_to_workplace(self, employee_id: int, workplace_id: int) -> bool:
        """Assign an employee to a workplace (idempotent)."""
        assignments = self._load_assignments()
        key = str(workplace_id)
        if key not in assignments:
            assignments[key] = []
        if employee_id not in assignments[key]:
            assignments[key].append(employee_id)
            self._save_assignments(assignments)
            return True
        return False  # already assigned

    def remove_employee_from_workplace(self, employee_id: int, workplace_id: int) -> bool:
        """Remove an employee from a workplace."""
        assignments = self._load_assignments()
        key = str(workplace_id)
        lst = assignments.get(key, [])
        if employee_id in lst:
            lst.remove(employee_id)
            assignments[key] = lst
            self._save_assignments(assignments)
            return True
        return False  # not assigned

    # ── Extra Charges (XCHAR) ─────────────────────────────────
    def get_extracharges(self, include_hidden: bool = False) -> List[Dict]:
        rows = self._read('XCHAR')
        if not include_hidden:
            rows = [r for r in rows if not r.get('HIDE')]
        result = []
        for r in rows:
            result.append({
                'ID': r.get('ID'),
                'NAME': r.get('NAME', ''),
                'POSITION': r.get('POSITION', 0),
                'START': r.get('START', 0),   # minutes from midnight
                'END': r.get('END', 0),
                'VALIDITY': r.get('VALIDITY', 0),
                'VALIDDAYS': r.get('VALIDDAYS', ''),
                'HOLRULE': r.get('HOLRULE', 0),
                'HIDE': r.get('HIDE', 0),
            })
        result.sort(key=lambda x: x.get('POSITION', 0))
        return result

    def create_extracharge(self, data: dict) -> dict:
        filepath = self._table('XCHAR')
        fields = get_table_fields(filepath)
        new_id = self._next_id('XCHAR')
        rows = self._read('XCHAR')
        max_pos = max((r.get('POSITION', 0) or 0 for r in rows), default=0) + 1
        record = {
            'ID': new_id,
            'NAME': data.get('NAME', ''),
            'POSITION': data.get('POSITION', max_pos),
            'START': data.get('START', 0),
            'END': data.get('END', 0),
            'VALIDITY': data.get('VALIDITY', 0),
            'VALIDDAYS': data.get('VALIDDAYS', '0000000'),
            'HOLRULE': data.get('HOLRULE', 0),
            'HIDE': 1 if data.get('HIDE') else 0,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_extracharge(self, xc_id: int, data: dict) -> dict:
        filepath = self._table('XCHAR')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('XCHAR', xc_id)
        if raw_idx is None:
            raise ValueError(f"ExtraCharge {xc_id} not found")
        update_data = {}
        for key in ('NAME', 'POSITION', 'START', 'END', 'VALIDITY', 'VALIDDAYS', 'HOLRULE', 'HIDE'):
            if key in data:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': xc_id, **update_data}

    def delete_extracharge(self, xc_id: int) -> int:
        filepath = self._table('XCHAR')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('XCHAR', xc_id)
        if raw_idx is None:
            return 0
        update_record(filepath, fields, raw_idx, {'HIDE': 1})
        return 1

    # ── Extra Charge Calculation helpers ─────────────────────
    @staticmethod
    def _decode_startend(raw: str) -> str:
        """Decode a UTF-16LE-encoded STARTEND string back to ASCII (e.g. '06:00-14:00')."""
        if not raw:
            return ''
        try:
            decoded = raw.encode('utf-16-le').decode('latin1', errors='replace').strip()
            return decoded.replace('\x00', '').replace('\t', '').strip()
        except Exception:
            return ''

    @staticmethod
    def _time_str_to_minutes(time_str: str) -> Optional[int]:
        """Convert 'HH:MM' to minutes from midnight, or None if invalid."""
        parts = time_str.strip().split(':')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            return int(parts[0]) * 60 + int(parts[1])
        return None

    @staticmethod
    def _is_validday_active(validdays: str, weekday: int) -> bool:
        """Check if weekday (0=Mon, 6=Sun) is active in VALIDDAYS string."""
        if not validdays or weekday < 0 or weekday >= len(validdays):
            return True
        c = validdays[weekday]
        code = ord(c)
        return code == 0x31 or code == 0x2031 or c == '1'

    @staticmethod
    def _interval_overlap_minutes(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
        """Calculate overlap in minutes between two simple (non-wrapping) time intervals."""
        return max(0, min(a_end, b_end) - max(a_start, b_start))

    def _time_window_overlap_minutes(self, s1: int, e1: int, s2: int, e2: int) -> int:
        """Calculate overlap in minutes between two time windows (may wrap overnight)."""
        def intervals(start: int, end: int):
            return [(start, end)] if start <= end else [(start, 1440), (0, end)]
        total = 0
        for a_s, a_e in intervals(s1, e1):
            for b_s, b_e in intervals(s2, e2):
                total += self._interval_overlap_minutes(a_s, a_e, b_s, b_e)
        return total

    def _get_shift_time_range(self, shift: Dict, weekday: int):
        """Return (start_min, end_min, duration_h) for a shift on given weekday."""
        raw = shift.get(f'STARTEND{weekday}', '')
        decoded = self._decode_startend(raw)
        if decoded and '-' in decoded:
            parts = decoded.split('-', 1)
            start_m = self._time_str_to_minutes(parts[0].strip())
            end_m = self._time_str_to_minutes(parts[1].strip())
            if start_m is not None and end_m is not None:
                duration = float(shift.get(f'DURATION{weekday}', 0) or 0)
                return start_m, end_m, duration
        duration = float(shift.get(f'DURATION{weekday}', 0) or 0)
        return None, None, duration

    def calculate_extracharge_hours(
        self,
        year: int,
        month: int,
        employee_id: Optional[int] = None,
    ) -> List[Dict]:
        """
        Calculate surcharge hours per ExtraCharge rule for a given month.
        Returns a list of dicts: { charge_id, charge_name, hours, shift_count, ... }
        """
        prefix = f"{year:04d}-{month:02d}"
        charges = self.get_extracharges(include_hidden=False)
        if not charges:
            return []

        holiday_dates = self.get_holiday_dates(year)
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}

        entries = []
        for r in self._read('MASHI'):
            d = r.get('DATE', '')
            if not d or not d.startswith(prefix):
                continue
            eid = r.get('EMPLOYEEID')
            if employee_id is not None and eid != employee_id:
                continue
            entries.append({'employee_id': eid, 'date': d, 'shift_id': r.get('SHIFTID')})

        for r in self._read('SPSHI'):
            d = r.get('DATE', '')
            if not d or not d.startswith(prefix):
                continue
            eid = r.get('EMPLOYEEID')
            if employee_id is not None and eid != employee_id:
                continue
            entries.append({'employee_id': eid, 'date': d, 'shift_id': r.get('SHIFTID')})

        charge_acc: Dict[int, Dict] = {
            c['ID']: {'hours': 0.0, 'count': 0, 'charge': c}
            for c in charges
        }

        for entry in entries:
            date_str = entry['date']
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                continue

            weekday = dt.weekday()  # 0=Mon, 6=Sun
            is_holiday = date_str in holiday_dates
            shift = shifts_map.get(entry.get('shift_id'))
            if not shift:
                continue

            shift_start, shift_end, shift_duration = self._get_shift_time_range(shift, weekday)

            for c in charges:
                cid = c['ID']
                holrule = c.get('HOLRULE', 0)
                if holrule == 1 and not is_holiday:
                    continue
                if holrule == 2 and is_holiday:
                    continue
                if not self._is_validday_active(c.get('VALIDDAYS', ''), weekday):
                    continue

                c_start = c.get('START', 0)
                c_end = c.get('END', 0)

                if c_start == 0 and c_end == 0:
                    overlap_h = shift_duration
                elif shift_start is not None and shift_end is not None:
                    overlap_min = self._time_window_overlap_minutes(
                        shift_start, shift_end, c_start, c_end
                    )
                    overlap_h = overlap_min / 60.0
                else:
                    overlap_h = 0.0

                if overlap_h > 0:
                    charge_acc[cid]['hours'] += overlap_h
                    charge_acc[cid]['count'] += 1

        return [
            {
                'charge_id': cid,
                'charge_name': acc['charge']['NAME'],
                'hours': round(acc['hours'], 2),
                'shift_count': acc['count'],
                'start_time': acc['charge'].get('START', 0),
                'end_time': acc['charge'].get('END', 0),
                'validdays': acc['charge'].get('VALIDDAYS', ''),
                'holrule': acc['charge'].get('HOLRULE', 0),
            }
            for cid, acc in charge_acc.items()
        ]

    # ── Leave Entitlements ────────────────────────────────────
    def get_absences_list(self, year: Optional[int] = None, employee_id: Optional[int] = None,
                           leave_type_id: Optional[int] = None) -> List[Dict]:
        """Return all absences, optionally filtered by year/employee/leave type."""
        rows = self._read('ABSEN')
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}
        result = []
        for r in rows:
            date = r.get('DATE', '')
            if year is not None and not date.startswith(str(year)):
                continue
            if employee_id is not None and r.get('EMPLOYEEID') != employee_id:
                continue
            lt_id = r.get('LEAVETYPID', 0)
            if leave_type_id is not None and lt_id != leave_type_id:
                continue
            lt = lt_map.get(lt_id)
            result.append({
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'date': date,
                'leave_type_id': lt_id,
                'leave_type_name': lt.get('NAME', '') if lt else '',
                'leave_type_short': lt.get('SHORTNAME', '') if lt else '',
            })
        result.sort(key=lambda x: x.get('date', ''))
        return result

    def get_all_group_assignments(self) -> List[Dict]:
        """Return all group assignments (employee_id, group_id pairs)."""
        rows = self._read('GRASG')
        return [{'employee_id': r.get('EMPLOYEEID'), 'group_id': r.get('GROUPID')} for r in rows]

    def get_leave_entitlements(self, year: int = None, employee_id: int = None) -> List[Dict]:
        """Read leave entitlements from 5LEAEN.DBF."""
        rows = self._read('LEAEN')
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}

        result = []
        for r in rows:
            if year is not None and r.get('YEAR') != year:
                continue
            if employee_id is not None and r.get('EMPLOYEEID') != employee_id:
                continue
            lt_id = r.get('LEAVETYPID', 0)
            lt = lt_map.get(lt_id)
            result.append({
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'year': r.get('YEAR'),
                'leave_type_id': lt_id,
                'leave_type_name': lt.get('NAME', '') if lt else '',
                'entitlement': float(r.get('ENTITLEMNT', 0) or 0),
                'carry_forward': float(r.get('REST', 0) or 0),
                'in_days': bool(r.get('INDAYS', 1)),
            })
        return result

    def set_leave_entitlement(self, employee_id: int, year: int, days: float,
                               carry_forward: float = 0, leave_type_id: int = 0) -> Dict:
        """Create or update a leave entitlement record."""
        filepath = self._table('LEAEN')
        fields = get_table_fields(filepath)

        # Read max ID before deletion
        all_records = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in all_records), default=0)

        # Remove existing entries for this employee+year(+leave_type)
        existing = find_all_records(filepath, fields, EMPLOYEEID=employee_id, YEAR=year)
        for idx, rec in existing:
            if leave_type_id == 0 or rec.get('LEAVETYPID', 0) == leave_type_id:
                delete_record(filepath, fields, idx)

        new_id = max_id + 1
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'YEAR': year,
            'LEAVETYPID': leave_type_id,
            'ENTITLEMNT': int(days),
            'REST': int(carry_forward),
            'INDAYS': 1,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'id': new_id,
            'employee_id': employee_id,
            'year': year,
            'leave_type_id': leave_type_id,
            'entitlement': days,
            'carry_forward': carry_forward,
            'in_days': True,
        }

    def _get_default_entitlement(self) -> float:
        """Return the STDENTIT from the first entitled leave type (e.g. 'Urlaub')."""
        for lt in self.get_leave_types(include_hidden=True):
            if lt.get('ENTITLED') and lt.get('STDENTIT', 0) > 0:
                return float(lt['STDENTIT'])
        return 25.0

    def get_leave_balance(self, employee_id: int, year: int) -> Dict:
        """Calculate leave balance: entitlement + carry_forward - used = remaining."""
        default_ent = self._get_default_entitlement()
        entitlements = self.get_leave_entitlements(year=year, employee_id=employee_id)

        if entitlements:
            total_entitlement = sum(e['entitlement'] for e in entitlements)
            total_carry = sum(e['carry_forward'] for e in entitlements)
        else:
            total_entitlement = default_ent
            total_carry = 0.0

        # Count vacation absences (ENTITLED leave types)
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}
        entitled_ids = {lt_id for lt_id, lt in lt_map.items() if lt.get('ENTITLED')}

        year_str = str(year)
        used = 0.0
        for r in self._read('ABSEN'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if not d or not d.startswith(year_str):
                continue
            if r.get('LEAVETYPID', 0) in entitled_ids:
                used += 1.0

        remaining = total_entitlement + total_carry - used
        forfeiture_date = f"{year}-12-31"

        return {
            'employee_id': employee_id,
            'year': year,
            'entitlement': total_entitlement,
            'carry_forward': total_carry,
            'total': total_entitlement + total_carry,
            'used': used,
            'remaining': remaining,
            'forfeiture_date': forfeiture_date,
            'has_custom_entitlement': len(entitlements) > 0,
        }

    def get_leave_balance_group(self, year: int, group_id: int) -> List[Dict]:
        """Get leave balance for all employees in a group."""
        member_ids = self.get_group_members(group_id)
        emp_map = {e['ID']: e for e in self.get_employees(include_hidden=True)}
        result = []
        for eid in member_ids:
            emp = emp_map.get(eid)
            if not emp:
                continue
            balance = self.get_leave_balance(eid, year)
            balance['employee_name'] = (
                f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', ')
            )
            balance['employee_number'] = emp.get('NUMBER', '')
            result.append(balance)
        result.sort(key=lambda x: x.get('employee_name', ''))
        return result

    # ── Holiday Bans ──────────────────────────────────────────
    def get_holiday_bans(self, group_id: int = None) -> List[Dict]:
        """Read holiday bans from 5HOBAN.DBF."""
        rows = self._read('HOBAN')
        groups_map = {g['ID']: g for g in self.get_groups(include_hidden=True)}
        result = []
        for r in rows:
            if group_id is not None and r.get('GROUPID') != group_id:
                continue
            gid = r.get('GROUPID', 0)
            grp = groups_map.get(gid)
            result.append({
                'id': r.get('ID'),
                'group_id': gid,
                'group_name': grp.get('NAME', '') if grp else '',
                'start_date': r.get('START', ''),
                'end_date': r.get('END', ''),
                'restrict': r.get('RESTRICT', 1),
                'reason': r.get('DESCRIPT', ''),
            })
        result.sort(key=lambda x: x.get('start_date', ''))
        return result

    def create_holiday_ban(self, group_id: int, start_date: str, end_date: str,
                            reason: str = '') -> Dict:
        """Create a holiday ban entry."""
        filepath = self._table('HOBAN')
        fields = get_table_fields(filepath)
        existing = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in existing), default=0)
        new_id = max_id + 1
        record = {
            'ID': new_id,
            'GROUPID': group_id,
            'START': start_date,
            'END': end_date,
            'RESTRICT': 1,
            'DESCRIPT': reason[:200] if reason else '',
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'id': new_id,
            'group_id': group_id,
            'start_date': start_date,
            'end_date': end_date,
            'restrict': 1,
            'reason': reason,
        }

    def delete_holiday_ban(self, ban_id: int) -> int:
        """Delete a holiday ban. Returns count of deleted records."""
        filepath = self._table('HOBAN')
        fields = get_table_fields(filepath)
        matches = find_all_records(filepath, fields, ID=ban_id)
        count = 0
        for idx, _ in matches:
            delete_record(filepath, fields, idx)
            count += 1
        return count

    # ── Annual Close ──────────────────────────────────────────
    def get_annual_close_preview(self, year: int, group_id: int = None,
                                  carry_forward_days: float = 10) -> Dict:
        """Preview annual close without saving changes."""
        employees = self.get_employees(include_hidden=False)
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            employees = [e for e in employees if e['ID'] in member_ids]

        details = []
        total_carry = 0.0
        total_forfeited = 0.0

        for emp in employees:
            eid = emp['ID']
            balance = self.get_leave_balance(eid, year)
            remaining = balance['remaining']
            carry = min(float(remaining), carry_forward_days) if remaining > 0 else 0.0
            forfeited = max(0.0, float(remaining) - carry_forward_days) if remaining > 0 else 0.0

            total_carry += carry
            total_forfeited += forfeited
            details.append({
                'employee_id': eid,
                'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
                'entitlement': balance['entitlement'],
                'carry_forward_in': balance['carry_forward'],
                'total': balance['total'],
                'used': balance['used'],
                'remaining': remaining,
                'proposed_carry_forward': carry,
                'forfeited': forfeited,
            })

        return {
            'year': year,
            'next_year': year + 1,
            'carry_forward_limit': carry_forward_days,
            'employee_count': len(employees),
            'total_carry_forward': total_carry,
            'total_forfeited': total_forfeited,
            'details': details,
        }

    def run_annual_close(self, year: int, group_id: int = None,
                          carry_forward_days: float = 10) -> Dict:
        """
        Run annual close: calculate remaining vacation, set carry_forward for year+1.
        Returns summary with { processed, total_carry_forward, total_forfeited }.
        """
        employees = self.get_employees(include_hidden=False)
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            employees = [e for e in employees if e['ID'] in member_ids]

        processed = 0
        total_carry = 0.0
        total_forfeited = 0.0
        details = []

        for emp in employees:
            eid = emp['ID']
            balance = self.get_leave_balance(eid, year)
            remaining = balance['remaining']
            carry = min(float(remaining), carry_forward_days) if remaining > 0 else 0.0
            forfeited = max(0.0, float(remaining) - carry_forward_days) if remaining > 0 else 0.0

            # Set entitlement for next year with carry_forward
            next_ent = balance['entitlement']  # keep same entitlement
            self.set_leave_entitlement(
                employee_id=eid,
                year=year + 1,
                days=next_ent,
                carry_forward=carry,
            )

            total_carry += carry
            total_forfeited += forfeited
            processed += 1
            details.append({
                'employee_id': eid,
                'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
                'remaining': remaining,
                'carry_forward': carry,
                'forfeited': forfeited,
            })

        return {
            'year': year,
            'next_year': year + 1,
            'processed': processed,
            'total_carry_forward': total_carry,
            'total_forfeited': total_forfeited,
            'details': details,
        }

    # ── Zeitkonto / Überstunden ───────────────────────────────

    def get_overtime_records(self, year: Optional[int] = None, employee_id: Optional[int] = None) -> List[Dict]:
        """Return records from 5OVER.DBF (manual overtime adjustments)."""
        rows = self._read('OVER')
        result = []
        year_str = str(year) if year else None
        for r in rows:
            if employee_id is not None and r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if year_str and (not d or not d.startswith(year_str)):
                continue
            result.append({
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'date': d,
                'hours': float(r.get('HOURS', 0) or 0),
            })
        return result

    def get_bookings(self, year: Optional[int] = None, month: Optional[int] = None,
                     employee_id: Optional[int] = None) -> List[Dict]:
        """Return manual time bookings from 5BOOK.DBF."""
        rows = self._read('BOOK')
        result = []
        year_str = str(year) if year else None
        prefix = f"{year:04d}-{month:02d}" if (year and month) else None
        for r in rows:
            if employee_id is not None and r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if prefix and (not d or not d.startswith(prefix)):
                continue
            elif year_str and not prefix and (not d or not d.startswith(year_str)):
                continue
            result.append({
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'date': d,
                'type': r.get('TYPE', 0),
                'value': float(r.get('VALUE', 0) or 0),
                'note': r.get('NOTE', ''),
            })
        result.sort(key=lambda x: x.get('date', ''))
        return result

    def create_booking(self, employee_id: int, date_str: str, booking_type: int,
                        value: float, note: str = '') -> Dict:
        """Append a new manual booking to 5BOOK.DBF."""
        filepath = self._table('BOOK')
        fields = get_table_fields(filepath)
        existing = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in existing), default=0)
        new_id = max_id + 1
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'DATE': date_str,
            'TYPE': booking_type,
            'VALUE': value,
            'NOTE': (note or '')[:200],
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'id': new_id,
            'employee_id': employee_id,
            'date': date_str,
            'type': booking_type,
            'value': value,
            'note': note,
        }

    def delete_booking(self, booking_id: int) -> int:
        """Delete a booking from 5BOOK.DBF by ID. Returns 1 if deleted, 0 if not found."""
        filepath = self._table('BOOK')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('BOOK', booking_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    # ── Carry Forward (Saldo-Übertrag) ────────────────────────
    def get_carry_forward(self, employee_id: int, year: int) -> Dict:
        """Read carry-forward booking (TYPE=2) for employee+year from 5BOOK.DBF."""
        year_str = str(year)
        for r in self._read('BOOK'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            if r.get('TYPE') != 2:
                continue
            d = r.get('DATE', '')
            if d and d.startswith(year_str):
                return {
                    'employee_id': employee_id,
                    'year': year,
                    'hours': float(r.get('VALUE', 0) or 0),
                    'booking_id': r.get('ID'),
                }
        return {'employee_id': employee_id, 'year': year, 'hours': 0.0, 'booking_id': None}

    def set_carry_forward(self, employee_id: int, year: int, hours: float) -> Dict:
        """Set carry-forward for employee+year. Replaces any existing TYPE=2 entry for that year."""
        filepath = self._table('BOOK')
        fields = get_table_fields(filepath)
        year_str = str(year)
        # Find and delete existing TYPE=2 entries for this employee+year using raw index
        existing = read_dbf(filepath)
        for r in existing:
            if r.get('EMPLOYEEID') == employee_id and r.get('TYPE') == 2:
                d = r.get('DATE', '')
                if d and d.startswith(year_str):
                    rec_id = r.get('ID')
                    if rec_id is not None:
                        raw_idx, _ = self._find_record('BOOK', rec_id)
                        if raw_idx is not None:
                            delete_record(filepath, fields, raw_idx)
        # Append new carry-forward
        existing2 = read_dbf(filepath)
        max_id = max((r.get('ID', 0) or 0 for r in existing2), default=0)
        new_id = max_id + 1
        date_str = f"{year}-01-01"
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'DATE': date_str,
            'TYPE': 2,
            'VALUE': hours,
            'NOTE': f'Jahresübertrag {year}',
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {'employee_id': employee_id, 'year': year, 'hours': hours, 'booking_id': new_id}

    def calculate_annual_statement(self, employee_id: int, year: int) -> Dict:
        """
        Calculate annual saldo and return carry-forward for next year.
        saldo = actual_hours - target_hours + booking_adjustments (excl. TYPE=2)
        """
        balance = self.calculate_time_balance(employee_id, year)
        if not balance:
            return {'saldo': 0.0, 'should_carry': False, 'employee_id': employee_id, 'year': year}
        # Exclude existing carry-forward (TYPE=2) from calculation
        cf = self.get_carry_forward(employee_id, year)
        carry_in = cf['hours']
        total_saldo = balance.get('total_saldo', 0.0)
        # Remove carry_in from saldo (it was already counted as booking_adjustment)
        net_saldo = round(total_saldo - carry_in, 2)
        return {
            'employee_id': employee_id,
            'year': year,
            'saldo': net_saldo,
            'carry_in': carry_in,
            'total_saldo': total_saldo,
            'should_carry': net_saldo != 0.0,
            'next_year': year + 1,
        }

    def calculate_time_balance(self, employee_id: int, year: int) -> Dict:
        """
        Calculate Soll vs Ist hours and overtime saldo for one employee for a full year.
        Uses schedule data (MASHI/SPSHI), employee's target hours, and 5OVER adjustments.
        Returns monthly breakdown + totals.
        """
        emp = self.get_employee(employee_id)
        if not emp:
            return {}

        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        year_str = f"{year:04d}"
        workdays_list = emp.get('WORKDAYS_LIST', [])

        monthly: Dict[int, Dict] = {}
        for m in range(1, 13):
            working_days = self._count_working_days(year, m, workdays_list)
            target = float(emp.get('HRSMONTH') or 0)
            if target == 0:
                target = float(emp.get('HRSDAY') or 0) * working_days
            monthly[m] = {
                'month': m,
                'target_hours': round(target, 2),
                'actual_hours': 0.0,
                'overtime_adjustment': 0.0,
                'booking_adjustment': 0.0,
                'absence_days': 0,
            }

        # Collect actual hours from MASHI
        for r in self._read('MASHI'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if not d or not d.startswith(year_str):
                continue
            m = int(d[5:7])
            sid = r.get('SHIFTID')
            if sid and sid in shifts_map:
                hrs = float(shifts_map[sid].get('DURATION0', 0) or 0)
                monthly[m]['actual_hours'] += hrs

        # Collect actual hours from SPSHI
        for r in self._read('SPSHI'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if not d or not d.startswith(year_str):
                continue
            m = int(d[5:7])
            monthly[m]['actual_hours'] += float(r.get('DURATION', 0) or 0)

        # Collect absence days
        for r in self._read('ABSEN'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if not d or not d.startswith(year_str):
                continue
            m = int(d[5:7])
            monthly[m]['absence_days'] += 1

        # Overtime adjustments from 5OVER
        for rec in self.get_overtime_records(year=year, employee_id=employee_id):
            d = rec['date']
            if d:
                m = int(d[5:7])
                monthly[m]['overtime_adjustment'] += rec['hours']

        # Manual bookings from 5BOOK
        for rec in self.get_bookings(year=year, employee_id=employee_id):
            d = rec['date']
            if d:
                m = int(d[5:7])
                monthly[m]['booking_adjustment'] += rec['value']

        # Round and compute differences
        total_target = 0.0
        total_actual = 0.0
        total_overtime_adj = 0.0
        total_booking_adj = 0.0
        months_list = []
        running_saldo = 0.0

        for m in range(1, 13):
            mo = monthly[m]
            mo['actual_hours'] = round(mo['actual_hours'], 2)
            mo['overtime_adjustment'] = round(mo['overtime_adjustment'], 2)
            mo['booking_adjustment'] = round(mo['booking_adjustment'], 2)
            diff = mo['actual_hours'] - mo['target_hours']
            adj = mo['overtime_adjustment'] + mo['booking_adjustment']
            saldo_month = round(diff + adj, 2)
            running_saldo += saldo_month
            mo['difference'] = round(diff, 2)
            mo['adjustment'] = round(adj, 2)
            mo['saldo'] = round(saldo_month, 2)
            mo['running_saldo'] = round(running_saldo, 2)
            total_target += mo['target_hours']
            total_actual += mo['actual_hours']
            total_overtime_adj += mo['overtime_adjustment']
            total_booking_adj += mo['booking_adjustment']
            months_list.append(mo)

        total_diff = round(total_actual - total_target, 2)
        total_saldo = round(total_diff + total_overtime_adj + total_booking_adj, 2)

        return {
            'employee_id': employee_id,
            'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
            'employee_short': emp.get('SHORTNAME', ''),
            'year': year,
            'total_target_hours': round(total_target, 2),
            'total_actual_hours': round(total_actual, 2),
            'total_difference': total_diff,
            'total_adjustment': round(total_overtime_adj + total_booking_adj, 2),
            'total_saldo': total_saldo,
            'months': months_list,
        }

    def get_zeitkonto(self, year: int, employee_id: Optional[int] = None,
                      group_id: Optional[int] = None) -> List[Dict]:
        """Return Zeitkonto summary for all employees (or filtered by group/employee)."""
        employees = self.get_employees(include_hidden=False)
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            employees = [e for e in employees if e['ID'] in member_ids]
        if employee_id is not None:
            employees = [e for e in employees if e['ID'] == employee_id]

        result = []
        for emp in employees:
            balance = self.calculate_time_balance(emp['ID'], year)
            if balance:
                result.append({
                    'employee_id': emp['ID'],
                    'employee_name': balance['employee_name'],
                    'employee_short': balance['employee_short'],
                    'total_target_hours': balance['total_target_hours'],
                    'total_actual_hours': balance['total_actual_hours'],
                    'total_difference': balance['total_difference'],
                    'total_adjustment': balance['total_adjustment'],
                    'total_saldo': balance['total_saldo'],
                })
        result.sort(key=lambda x: x.get('employee_name', ''))
        return result

    # ── Schedule Conflicts ────────────────────────────────────

    def get_schedule_conflicts(self, year: int, month: int, group_id: int = None) -> list:
        """
        Detect conflicts for a given month:
          - shift_and_absence: employee has both a MASHI/SPSHI entry AND an ABSEN entry on the same day
          - holiday_ban: employee has an ABSEN entry on a day within a HOBAN period for their group(s)
        Returns list of dicts with keys: employee_id, date, type, message
        """
        prefix = f"{year:04d}-{month:02d}"
        conflicts = []

        # Get relevant employees
        employees_list = self.get_employees(include_hidden=False)
        emp_map = {e['ID']: e for e in employees_list}

        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
        else:
            member_ids = set(emp_map.keys())

        # Build lookup maps for shift names and absence types
        shift_name_map: dict = {}  # {shift_id: shift_name}
        for r in self._read('SHIFT'):
            sid = r.get('ID')
            if sid:
                shift_name_map[sid] = r.get('NAME', r.get('SHORTNAME', str(sid)))
        leave_name_map: dict = {}  # {leave_type_id: leave_type_name}
        for r in self._read('LEAVT'):
            lid = r.get('ID')
            if lid:
                leave_name_map[lid] = r.get('NAME', r.get('SHORTNAME', str(lid)))

        # Build: employee_id -> set of dates with shifts, and date -> shift name
        shift_dates: dict = {}
        shift_detail: dict = {}  # {(eid, date): shift_name}
        for r in self._read('MASHI'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                eid = r.get('EMPLOYEEID')
                if eid in member_ids:
                    shift_dates.setdefault(eid, set()).add(d)
                    sid = r.get('SHIFTID')
                    sname = shift_name_map.get(sid, '') if sid else ''
                    if (eid, d) not in shift_detail:
                        shift_detail[(eid, d)] = sname
        for r in self._read('SPSHI'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                eid = r.get('EMPLOYEEID')
                if eid in member_ids:
                    shift_dates.setdefault(eid, set()).add(d)
                    if (eid, d) not in shift_detail:
                        shift_detail[(eid, d)] = r.get('NAME', 'Sonderschicht')

        # Build: employee_id -> list of absence date strings, and date -> leave type name
        absence_dates: dict = {}
        absence_detail: dict = {}  # {(eid, date): leave_type_name}
        for r in self._read('ABSEN'):
            d = r.get('DATE', '')
            if d and d.startswith(prefix):
                eid = r.get('EMPLOYEEID')
                if eid in member_ids:
                    absence_dates.setdefault(eid, set()).add(d)
                    lid = r.get('LEAVETYPID')
                    lname = leave_name_map.get(lid, '') if lid else ''
                    absence_detail[(eid, d)] = lname

        # Conflict type 1: shift_and_absence
        for eid in member_ids:
            shifts = shift_dates.get(eid, set())
            absences = absence_dates.get(eid, set())
            overlap = shifts & absences
            emp = emp_map.get(eid)
            emp_name = f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', ') if emp else f"MA#{eid}"
            for date_str in sorted(overlap):
                day_display = date_str[8:10] + '.' + date_str[5:7] + '.' + date_str[0:4]
                conflicts.append({
                    'employee_id': eid,
                    'employee_name': emp_name,
                    'date': date_str,
                    'type': 'shift_and_absence',
                    'shift_name': shift_detail.get((eid, date_str), ''),
                    'absence_name': absence_detail.get((eid, date_str), ''),
                    'message': f"{emp_name}: Schicht + Abwesenheit am {day_display}",
                })

        # Conflict type 2: holiday_ban
        # Get all holiday bans (all groups) and filter by employee's groups
        all_bans = self.get_holiday_bans()

        # Build employee -> groups map
        emp_groups: dict = {}
        for r in self._read('GRASG'):
            eid = r.get('EMPLOYEEID')
            gid = r.get('GROUPID')
            if eid in member_ids:
                emp_groups.setdefault(eid, set()).add(gid)

        # For each employee, check if any absence falls in a ban period for their group
        for eid, abs_dates in absence_dates.items():
            emp = emp_map.get(eid)
            emp_name = f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', ') if emp else f"MA#{eid}"
            emp_group_ids = emp_groups.get(eid, set())
            for date_str in sorted(abs_dates):
                for ban in all_bans:
                    ban_gid = ban.get('group_id')
                    # Ban applies if no group (global) or employee is in the banned group
                    if ban_gid and ban_gid not in emp_group_ids:
                        continue
                    start = ban.get('start_date', '')
                    end = ban.get('end_date', '')
                    if start and end and start <= date_str <= end:
                        day_display = date_str[8:10] + '.' + date_str[5:7] + '.' + date_str[0:4]
                        reason = ban.get('reason', '')
                        msg = f"{emp_name}: Abwesenheit in gesperrtem Zeitraum am {day_display}"
                        if reason:
                            msg += f" ({reason})"
                        conflicts.append({
                            'employee_id': eid,
                            'date': date_str,
                            'type': 'holiday_ban',
                            'message': msg,
                        })
                        break  # one conflict per employee per date

        # Sort by date then employee_id
        conflicts.sort(key=lambda c: (c['date'], c['employee_id']))
        return conflicts

    # ── Restrictions ───────────────────────────────────────────

    def get_restrictions(self, employee_id: Optional[int] = None) -> List[Dict]:
        """Return all shift restrictions, optionally filtered by employee."""
        rows = self._read('RESTR')
        if employee_id is not None:
            rows = [r for r in rows if r.get('EMPLOYEEID') == employee_id]
        shifts = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        result = []
        for r in rows:
            shift = shifts.get(r.get('SHIFTID', 0))
            entry = {
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'shift_id': r.get('SHIFTID'),
                'weekday': r.get('WEEKDAY', 0),
                'restrict': r.get('RESTRICT', 1),
                'reason': (r.get('RESERVED') or '').strip(),
                'shift_name': shift.get('NAME', '') if shift else '',
                'shift_short': shift.get('SHORTNAME', '') if shift else '',
            }
            result.append(entry)
        return result

    def set_restriction(
        self,
        employee_id: int,
        shift_id: int,
        reason: str = '',
        weekday: int = 0,
    ) -> Dict:
        """Create a restriction (or return existing one)."""
        filepath = self._table('RESTR')
        fields = get_table_fields(filepath)
        existing = find_all_records(
            filepath, fields, EMPLOYEEID=employee_id, SHIFTID=shift_id, WEEKDAY=weekday
        )
        if existing:
            rec = existing[0][1]
            return {'id': rec.get('ID'), 'employee_id': employee_id, 'shift_id': shift_id,
                    'weekday': weekday, 'reason': reason, 'exists': True}
        new_id = self._next_id('RESTR')
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'WEEKDAY': weekday,
            'SHIFTID': shift_id,
            'RESTRICT': 1,
            'RESERVED': (reason or '')[:20],
        }
        append_record(filepath, fields, record)
        return {**record, 'exists': False}

    def remove_restriction(self, employee_id: int, shift_id: int, weekday: int = 0) -> int:
        """Delete matching restriction records; returns count deleted."""
        filepath = self._table('RESTR')
        fields = get_table_fields(filepath)
        matches = find_all_records(
            filepath, fields, EMPLOYEEID=employee_id, SHIFTID=shift_id, WEEKDAY=weekday
        )
        count = 0
        for raw_idx, _ in matches:
            delete_record(filepath, fields, raw_idx)
            count += 1
        return count

    # ── Write: Periods ────────────────────────────────────────
    def create_period(self, data: dict) -> dict:
        """Append a new accounting period to 5PERIO."""
        filepath = self._table('PERIO')
        fields = get_table_fields(filepath)
        new_id = self._next_id('PERIO')
        record = {
            'ID': new_id,
            'GROUPID': data.get('group_id', 0),
            'START': data.get('start', ''),
            'END': data.get('end', ''),
            'COLOR': data.get('color', 16777215),
            'DESCRIPT': (data.get('description', '') or '')[:200],
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'id': new_id,
            'group_id': record['GROUPID'],
            'start': record['START'],
            'end': record['END'],
            'color': None,
            'description': record['DESCRIPT'],
        }

    def delete_period(self, period_id: int) -> int:
        """Delete an accounting period from 5PERIO by ID."""
        filepath = self._table('PERIO')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('PERIO', period_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    # ── Write: Staffing Requirements (SHDEM) ──────────────────
    def set_staffing_requirement(
        self,
        shift_id: int,
        weekday: int,
        min_staff: int,
        max_staff: int,
        group_id: int,
    ) -> dict:
        """Create or update a SHDEM staffing requirement."""
        filepath = self._table('SHDEM')
        fields = get_table_fields(filepath)
        # Try to find existing record
        matches = find_all_records(filepath, fields, SHIFTID=shift_id, WEEKDAY=weekday, GROUPID=group_id)
        if matches:
            raw_idx, existing = matches[0]
            update_record(filepath, fields, raw_idx, {'MIN': min_staff, 'MAX': max_staff})
            return {
                'id': existing.get('ID'),
                'group_id': group_id,
                'weekday': weekday,
                'shift_id': shift_id,
                'min': min_staff,
                'max': max_staff,
            }
        # Create new
        new_id = self._next_id('SHDEM')
        record = {
            'ID': new_id,
            'GROUPID': group_id,
            'WEEKDAY': weekday,
            'SHIFTID': shift_id,
            'WORKPLACID': 0,
            'MIN': min_staff,
            'MAX': max_staff,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {
            'id': new_id,
            'group_id': group_id,
            'weekday': weekday,
            'shift_id': shift_id,
            'min': min_staff,
            'max': max_staff,
        }

    # ── Settings (USETT) ──────────────────────────────────────
    def get_usett(self) -> Dict:
        """Read global settings from 5USETT.DBF (record 0)."""
        rows = self._read('USETT')
        if not rows:
            return {}
        r = rows[0]
        return {
            'ID': r.get('ID', 0),
            'LOGIN': r.get('LOGIN', 0),
            'SPSHCAT': r.get('SPSHCAT', 0),
            'OVERTCAT': r.get('OVERTCAT', 0),
            'ANOANAME': r.get('ANOANAME', 'Abwesend'),
            'ANOASHORT': r.get('ANOASHORT', 'X'),
            'ANOACRTXT': r.get('ANOACRTXT', 0),
            'ANOACRBAR': r.get('ANOACRBAR', 16711680),
            'ANOACRBK': r.get('ANOACRBK', 16777215),
            'ANOABOLD': r.get('ANOABOLD', 0),
            'BACKUPFR': r.get('BACKUPFR', 0),
        }

    def update_usett(self, data: Dict) -> Dict:
        """Update global settings in 5USETT.DBF (record 0)."""
        filepath = self._table('USETT')
        fields = get_table_fields(filepath)
        rows = read_dbf(filepath)
        if not rows:
            raise ValueError("5USETT.DBF is empty — cannot update settings")
        # Find raw index of record with ID=0 (global settings row)
        matches = find_all_records(filepath, fields, ID=0)
        if matches:
            raw_idx = matches[0][0]
        else:
            # Fallback: use first record regardless of ID
            raw_idx = 0
        update_data: Dict[str, Any] = {}
        allowed = ('ANOANAME', 'ANOASHORT', 'ANOACRTXT', 'ANOACRBAR', 'ANOACRBK', 'ANOABOLD', 'BACKUPFR', 'LOGIN', 'SPSHCAT', 'OVERTCAT')
        for key in allowed:
            if key in data:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        self._invalidate_cache('USETT')
        return self.get_usett()

    # ── Special Staffing Requirements (SPDEM) ─────────────────
    def get_special_staffing(self, date: Optional[str] = None, group_id: Optional[int] = None) -> List[Dict]:
        """Return date-specific staffing requirements from 5SPDEM.DBF."""
        rows = self._read('SPDEM')
        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        workplaces_map = {w['ID']: w for w in self.get_workplaces(include_hidden=True)}
        result = []
        for r in rows:
            if date is not None and r.get('DATE', '') != date:
                continue
            if group_id is not None and r.get('GROUPID') != group_id:
                continue
            sid = r.get('SHIFTID')
            shift = shifts_map.get(sid) if sid else None
            wpid = r.get('WORKPLACID')
            wp = workplaces_map.get(wpid) if wpid else None
            result.append({
                'id': r.get('ID'),
                'group_id': r.get('GROUPID'),
                'date': r.get('DATE', ''),
                'shift_id': sid,
                'shift_name': shift.get('NAME', '') if shift else '',
                'shift_short': shift.get('SHORTNAME', '') if shift else '',
                'color_bk': bgr_to_hex(shift.get('COLORBK', 16777215)) if shift else '#FFFFFF',
                'color_text': bgr_to_hex(shift.get('COLORTEXT', 0)) if shift else '#000000',
                'workplace_id': wpid,
                'workplace_name': wp.get('NAME', '') if wp else '',
                'min': r.get('MIN', 0),
                'max': r.get('MAX', 0),
            })
        result.sort(key=lambda x: (x.get('date', ''), x.get('group_id') or 0))
        return result

    def create_special_staffing(self, groupid: int, date: str, shiftid: int, workplacid: int, min_staff: int, max_staff: int) -> Dict:
        """Create a new date-specific staffing requirement in 5SPDEM.DBF."""
        filepath = self._table('SPDEM')
        fields = get_table_fields(filepath)
        new_id = self._next_id('SPDEM')
        record = {
            'ID': new_id,
            'GROUPID': groupid,
            'DATE': date,
            'SHIFTID': shiftid,
            'WORKPLACID': workplacid,
            'MIN': min_staff,
            'MAX': max_staff,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {**record, 'id': new_id}

    def update_special_staffing(self, record_id: int, data: Dict) -> Dict:
        """Update a date-specific staffing requirement."""
        filepath = self._table('SPDEM')
        fields = get_table_fields(filepath)
        raw_idx, existing = self._find_record('SPDEM', record_id)
        if raw_idx is None:
            raise ValueError(f"SpecialStaffing {record_id} not found")
        update_data: Dict[str, Any] = {}
        for key in ('GROUPID', 'DATE', 'SHIFTID', 'WORKPLACID', 'MIN', 'MAX'):
            if key in data:
                update_data[key] = data[key]
        update_record(filepath, fields, raw_idx, update_data)
        return {'id': record_id, **update_data}

    def delete_special_staffing(self, record_id: int) -> int:
        """Delete a date-specific staffing requirement."""
        filepath = self._table('SPDEM')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('SPDEM', record_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    # ── Employee Detailed Statistics ──────────────────────────

    def _is_night_shift(self, shift: Dict, weekday: int) -> bool:
        """Return True if the shift on given weekday qualifies as a night shift.
        Night = start >= 22:00 OR end <= 06:00 (and shift has a defined time window).
        """
        raw = shift.get(f'STARTEND{weekday}', '') or shift.get('STARTEND0', '')
        decoded = self._decode_startend(raw)
        if decoded and '-' in decoded:
            parts = decoded.split('-', 1)
            start_m = self._time_str_to_minutes(parts[0].strip())
            end_m = self._time_str_to_minutes(parts[1].strip())
            if start_m is not None and end_m is not None:
                # Night: starts at 22:00 (1320 min) or later, OR ends at 06:00 (360) or earlier
                if start_m >= 1320 or end_m <= 360:
                    return True
        # Fallback: check STARTEND0
        if weekday != 0:
            raw0 = shift.get('STARTEND0', '')
            decoded0 = self._decode_startend(raw0)
            if decoded0 and '-' in decoded0:
                parts0 = decoded0.split('-', 1)
                s0 = self._time_str_to_minutes(parts0[0].strip())
                e0 = self._time_str_to_minutes(parts0[1].strip())
                if s0 is not None and e0 is not None:
                    if s0 >= 1320 or e0 <= 360:
                        return True
        return False

    def get_employee_stats_year(self, employee_id: int, year: int) -> Dict:
        """
        Return detailed per-employee statistics for all 12 months of a year.
        Includes: soll_stunden, ist_stunden, weekend_shifts, night_shifts, vacation_days, absence_days.
        """
        from datetime import date as _date

        emp = self.get_employee(employee_id)
        if not emp:
            return {}

        shifts_map = {s['ID']: s for s in self.get_shifts(include_hidden=True)}
        lt_map = {lt['ID']: lt for lt in self.get_leave_types(include_hidden=True)}
        entitled_ids = {lt_id for lt_id, lt in lt_map.items() if lt.get('ENTITLED')}
        year_str = f"{year:04d}"
        workdays_list = emp.get('WORKDAYS_LIST', [])

        # Initialize monthly buckets
        monthly: Dict[int, Dict] = {}
        for m in range(1, 13):
            working_days = self._count_working_days(year, m, workdays_list)
            target = float(emp.get('HRSMONTH') or 0)
            if target == 0:
                target = float(emp.get('HRSDAY') or 0) * working_days
            monthly[m] = {
                'month': m,
                'target_hours': round(target, 2),
                'actual_hours': 0.0,
                'weekend_shifts': 0,
                'night_shifts': 0,
                'vacation_days': 0,
                'absence_days': 0,
                'shifts_count': 0,
            }

        # Scan MASHI (regular schedule)
        for r in self._read('MASHI'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if not d or not d.startswith(year_str):
                continue
            m = int(d[5:7])
            try:
                dt = _date.fromisoformat(d)
                weekday = dt.weekday()
            except ValueError:
                continue

            sid = r.get('SHIFTID')
            shift = shifts_map.get(sid) if sid else None
            if shift:
                hrs = float(shift.get('DURATION0', 0) or 0)
                monthly[m]['actual_hours'] += hrs
                monthly[m]['shifts_count'] += 1
                if weekday >= 5:  # Saturday=5, Sunday=6
                    monthly[m]['weekend_shifts'] += 1
                if self._is_night_shift(shift, weekday):
                    monthly[m]['night_shifts'] += 1

        # Scan SPSHI (special shifts)
        for r in self._read('SPSHI'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if not d or not d.startswith(year_str):
                continue
            m = int(d[5:7])
            try:
                dt = _date.fromisoformat(d)
                weekday = dt.weekday()
            except ValueError:
                continue

            hrs = float(r.get('DURATION', 0) or 0)
            monthly[m]['actual_hours'] += hrs
            monthly[m]['shifts_count'] += 1
            if weekday >= 5:
                monthly[m]['weekend_shifts'] += 1
            # For special shifts, check referenced shift if available
            sid = r.get('SHIFTID')
            if sid and sid in shifts_map:
                if self._is_night_shift(shifts_map[sid], weekday):
                    monthly[m]['night_shifts'] += 1

        # Scan ABSEN (absences)
        for r in self._read('ABSEN'):
            if r.get('EMPLOYEEID') != employee_id:
                continue
            d = r.get('DATE', '')
            if not d or not d.startswith(year_str):
                continue
            m = int(d[5:7])
            monthly[m]['absence_days'] += 1
            lt_id = r.get('LEAVETYPID', 0)
            if lt_id in entitled_ids:
                monthly[m]['vacation_days'] += 1

        # Build result list with computed difference
        months_list = []
        total_target = 0.0
        total_actual = 0.0
        total_weekend = 0
        total_night = 0
        total_vacation = 0
        total_absence = 0
        total_shifts = 0

        for m in range(1, 13):
            mo = monthly[m]
            mo['actual_hours'] = round(mo['actual_hours'], 2)
            mo['difference'] = round(mo['actual_hours'] - mo['target_hours'], 2)
            total_target += mo['target_hours']
            total_actual += mo['actual_hours']
            total_weekend += mo['weekend_shifts']
            total_night += mo['night_shifts']
            total_vacation += mo['vacation_days']
            total_absence += mo['absence_days']
            total_shifts += mo['shifts_count']
            months_list.append(mo)

        return {
            'employee_id': employee_id,
            'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
            'employee_short': emp.get('SHORTNAME', ''),
            'employee_number': emp.get('NUMBER', ''),
            'year': year,
            'months': months_list,
            'totals': {
                'target_hours': round(total_target, 2),
                'actual_hours': round(total_actual, 2),
                'difference': round(total_actual - total_target, 2),
                'weekend_shifts': total_weekend,
                'night_shifts': total_night,
                'vacation_days': total_vacation,
                'absence_days': total_absence,
                'shifts_count': total_shifts,
            },
        }

    def get_employee_stats_month(self, employee_id: int, year: int, month: int) -> Dict:
        """
        Return detailed stats for a single employee for a specific month.
        Subset of get_employee_stats_year for one month.
        """
        result = self.get_employee_stats_year(employee_id, year)
        if not result:
            return {}
        mo = result['months'][month - 1]
        return {
            'employee_id': employee_id,
            'employee_name': result['employee_name'],
            'employee_short': result['employee_short'],
            'year': year,
            'month': month,
            'target_hours': mo['target_hours'],
            'actual_hours': mo['actual_hours'],
            'difference': mo['difference'],
            'weekend_shifts': mo['weekend_shifts'],
            'night_shifts': mo['night_shifts'],
            'vacation_days': mo['vacation_days'],
            'absence_days': mo['absence_days'],
            'shifts_count': mo['shifts_count'],
        }

    # ── Stats ──────────────────────────────────────────────────
    def get_stats(self) -> Dict:
        employees = self.get_employees()
        groups = self.get_groups()
        shifts = self.get_shifts()
        leave_types = self.get_leave_types()
        workplaces = self.get_workplaces()
        holidays = self.get_holidays()
        users = self.get_users()

        return {
            'employees': len(employees),
            'groups': len(groups),
            'shifts': len(shifts),
            'leave_types': len(leave_types),
            'workplaces': len(workplaces),
            'holidays': len(holidays),
            'users': len(users),
        }

    # ── Cycle Exceptions (5CYEXC) ─────────────────────────────
    def get_cycle_exceptions(self, employee_id: Optional[int] = None,
                              cycle_assignment_id: Optional[int] = None) -> List[Dict]:
        """Get cycle exceptions (overrides for specific dates in assigned cycles)."""
        rows = self._read('CYEXC')
        result = []
        for r in rows:
            if employee_id is not None and r.get('EMPLOYEEID') != employee_id:
                continue
            if cycle_assignment_id is not None and r.get('CYCLEASSID') != cycle_assignment_id:
                continue
            result.append({
                'id': r.get('ID'),
                'employee_id': r.get('EMPLOYEEID'),
                'cycle_assignment_id': r.get('CYCLEASSID'),
                'date': r.get('DATE', ''),
                'type': r.get('TYPE', 0),
            })
        return result

    def set_cycle_exception(self, employee_id: int, cycle_assignment_id: int,
                             date_str: str, exc_type: int = 1) -> Dict:
        """Set a cycle exception for a specific date (type 1=skip, 0=normal)."""
        filepath = self._table('CYEXC')
        fields = get_table_fields(filepath)
        # Check if exception already exists for this date/employee
        existing = find_all_records(filepath, fields, EMPLOYEEID=employee_id, CYCLEASSID=cycle_assignment_id)
        for raw_idx, r in existing:
            if r.get('DATE', '') == date_str:
                update_record(filepath, fields, raw_idx, {'TYPE': exc_type})
                return {'id': r.get('ID'), 'employee_id': employee_id,
                        'cycle_assignment_id': cycle_assignment_id,
                        'date': date_str, 'type': exc_type}
        new_id = self._next_id('CYEXC')
        record = {
            'ID': new_id,
            'EMPLOYEEID': employee_id,
            'CYCLEASSID': cycle_assignment_id,
            'DATE': date_str,
            'TYPE': exc_type,
            'RESERVED': '',
        }
        append_record(filepath, fields, record)
        return {'id': new_id, 'employee_id': employee_id,
                'cycle_assignment_id': cycle_assignment_id,
                'date': date_str, 'type': exc_type}

    def delete_cycle_exception(self, exception_id: int) -> int:
        """Delete a cycle exception by ID."""
        filepath = self._table('CYEXC')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('CYEXC', exception_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    # ── Employee Access (5EMACC) ──────────────────────────────
    def get_employee_access(self, user_id: Optional[int] = None) -> List[Dict]:
        """Get employee-level access restrictions for users."""
        rows = self._read('EMACC')
        result = []
        for r in rows:
            if user_id is not None and r.get('USERID') != user_id:
                continue
            result.append({
                'id': r.get('ID'),
                'user_id': r.get('USERID'),
                'employee_id': r.get('EMPLOYEEID'),
                'rights': r.get('RIGHTS', 0),
            })
        return result

    def set_employee_access(self, user_id: int, employee_id: int, rights: int) -> Dict:
        """Set access rights for a user on a specific employee."""
        filepath = self._table('EMACC')
        fields = get_table_fields(filepath)
        existing = find_all_records(filepath, fields, USERID=user_id, EMPLOYEEID=employee_id)
        if existing:
            raw_idx, r = existing[0]
            update_record(filepath, fields, raw_idx, {'RIGHTS': rights})
            return {'id': r.get('ID'), 'user_id': user_id, 'employee_id': employee_id, 'rights': rights}
        new_id = self._next_id('EMACC')
        record = {'ID': new_id, 'USERID': user_id, 'EMPLOYEEID': employee_id,
                  'RIGHTS': rights, 'RESERVED': ''}
        append_record(filepath, fields, record)
        return {'id': new_id, 'user_id': user_id, 'employee_id': employee_id, 'rights': rights}

    def delete_employee_access(self, access_id: int) -> int:
        """Delete employee access record."""
        filepath = self._table('EMACC')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('EMACC', access_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    # ── Group Access (5GRACC) ─────────────────────────────────
    def get_group_access(self, user_id: Optional[int] = None) -> List[Dict]:
        """Get group-level access restrictions for users."""
        rows = self._read('GRACC')
        result = []
        for r in rows:
            if user_id is not None and r.get('USERID') != user_id:
                continue
            result.append({
                'id': r.get('ID'),
                'user_id': r.get('USERID'),
                'group_id': r.get('GROUPID'),
                'rights': r.get('RIGHTS', 0),
            })
        return result

    def set_group_access(self, user_id: int, group_id: int, rights: int) -> Dict:
        """Set access rights for a user on a specific group."""
        filepath = self._table('GRACC')
        fields = get_table_fields(filepath)
        existing = find_all_records(filepath, fields, USERID=user_id, GROUPID=group_id)
        if existing:
            raw_idx, r = existing[0]
            update_record(filepath, fields, raw_idx, {'RIGHTS': rights})
            return {'id': r.get('ID'), 'user_id': user_id, 'group_id': group_id, 'rights': rights}
        new_id = self._next_id('GRACC')
        record = {'ID': new_id, 'USERID': user_id, 'GROUPID': group_id,
                  'RIGHTS': rights, 'RESERVED': ''}
        append_record(filepath, fields, record)
        return {'id': new_id, 'user_id': user_id, 'group_id': group_id, 'rights': rights}

    def delete_group_access(self, access_id: int) -> int:
        """Delete group access record."""
        filepath = self._table('GRACC')
        fields = get_table_fields(filepath)
        raw_idx, _ = self._find_record('GRACC', access_id)
        if raw_idx is None:
            return 0
        delete_record(filepath, fields, raw_idx)
        return 1

    # ── Changelog ─────────────────────────────────────────────

    def _changelog_path(self) -> str:
        """Path to changelog.json file next to the database directory."""
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
        os.makedirs(data_dir, exist_ok=True)
        return os.path.join(data_dir, 'changelog.json')

    def get_changelog(self, limit: int = 100, user: Optional[str] = None,
                      date_from: Optional[str] = None, date_to: Optional[str] = None) -> List[Dict]:
        """Read changelog entries from backend/data/changelog.json."""
        path = self._changelog_path()
        if not os.path.exists(path):
            return []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                entries: List[Dict] = json.load(f)
        except Exception:
            return []
        # Filter
        if user:
            entries = [e for e in entries if e.get('user', '').lower() == user.lower()]
        if date_from:
            entries = [e for e in entries if e.get('timestamp', '') >= date_from]
        if date_to:
            entries = [e for e in entries if e.get('timestamp', '') <= date_to + 'T23:59:59']
        # Sort newest first, apply limit
        entries = sorted(entries, key=lambda e: e.get('timestamp', ''), reverse=True)
        return entries[:limit]

    def log_action(self, user: str, action: str, entity: str, entity_id: int,
                   details: str = '') -> Dict:
        """Append a log entry to backend/data/changelog.json. Keeps max 1000 entries."""
        import datetime as _dt
        path = self._changelog_path()
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    entries: List[Dict] = json.load(f)
            except Exception:
                entries = []
        else:
            entries = []
        entry = {
            'timestamp': _dt.datetime.now().isoformat(timespec='seconds'),
            'user': user,
            'action': action,          # CREATE / UPDATE / DELETE
            'entity': entity,          # employee / shift / group / schedule / absence / ...
            'entity_id': entity_id,
            'details': details,
        }
        entries.append(entry)
        # Keep newest 1000
        if len(entries) > 1000:
            entries = entries[-1000:]
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        return entry

    # ── Schicht-Wünsche & Sperrtage ──────────────────────────

    def _wishes_path(self) -> str:
        """Path to wishes.json stored in the backend data directory."""
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
        os.makedirs(data_dir, exist_ok=True)
        return os.path.join(data_dir, 'wishes.json')

    def get_wishes(self, employee_id: Optional[int] = None,
                   year: Optional[int] = None, month: Optional[int] = None) -> List[Dict]:
        """Return shift wishes/blocked days, optionally filtered."""
        path = self._wishes_path()
        if not os.path.exists(path):
            return []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                entries: List[Dict] = json.load(f)
        except Exception:
            return []
        if employee_id is not None:
            entries = [e for e in entries if e.get('employee_id') == employee_id]
        if year is not None:
            entries = [e for e in entries if str(e.get('date', ''))[:4] == str(year)]
        if month is not None:
            entries = [e for e in entries if str(e.get('date', ''))[5:7] == f'{month:02d}']
        return entries

    def add_wish(self, employee_id: int, date: str, wish_type: str,
                 shift_id: Optional[int] = None, note: str = '') -> Dict:
        """Add a wish (WUNSCH) or blocked day (SPERRUNG).

        wish_type: 'WUNSCH' | 'SPERRUNG'
        """
        import datetime as _dt
        path = self._wishes_path()
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    entries: List[Dict] = json.load(f)
            except Exception:
                entries = []
        else:
            entries = []
        max_id = max((e.get('id', 0) for e in entries), default=0)
        entry = {
            'id': max_id + 1,
            'employee_id': employee_id,
            'date': date,
            'wish_type': wish_type,  # WUNSCH | SPERRUNG
            'shift_id': shift_id,
            'note': note,
            'created_at': _dt.datetime.now().isoformat(timespec='seconds'),
        }
        entries.append(entry)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        return entry

    def delete_wish(self, wish_id: int) -> int:
        """Delete a wish by ID. Returns 1 if deleted, 0 if not found."""
        path = self._wishes_path()
        if not os.path.exists(path):
            return 0
        try:
            with open(path, 'r', encoding='utf-8') as f:
                entries: List[Dict] = json.load(f)
        except Exception:
            return 0
        new_entries = [e for e in entries if e.get('id') != wish_id]
        if len(new_entries) == len(entries):
            return 0
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(new_entries, f, ensure_ascii=False, indent=2)
        return 1

    # ── Overtime Summary ──────────────────────────────────────

    def get_overtime_summary(self, year: int, group_id: Optional[int] = None) -> List[Dict]:
        """Calculate overtime (Überstunden) per employee for a given year.

        Returns:
            List of dicts with keys:
              employee_id, name, shortname, soll, ist, delta
            where delta = ist - soll (positive = Plusstunden, negative = Minusstunden).
        """
        employees = self.get_employees(include_hidden=False)
        if group_id is not None:
            member_ids = set(self.get_group_members(group_id))
            employees = [e for e in employees if e['ID'] in member_ids]

        result = []
        for emp in employees:
            balance = self.calculate_time_balance(emp['ID'], year)
            if not balance:
                continue
            soll = balance.get('total_target_hours', 0.0)
            ist = balance.get('total_actual_hours', 0.0)
            delta = round(ist - soll, 2)
            result.append({
                'employee_id': emp['ID'],
                'name': f"{emp.get('NAME', '')} {emp.get('FIRSTNAME', '')}".strip(),
                'shortname': emp.get('SHORTNAME', ''),
                'number': emp.get('NUMBER', ''),
                'soll': round(soll, 2),
                'ist': round(ist, 2),
                'delta': delta,
                'saldo': round(balance.get('total_saldo', delta), 2),
            })
        result.sort(key=lambda x: x.get('name', ''))
        return result

    # ── Sickness Statistics ───────────────────────────────────

    def get_sickness_statistics(self, year: int) -> Dict:
        """Return sickness/Krankenstand statistics for a given year.

        Returns:
            {
                year, sick_type_ids,
                total_sick_days, affected_employees,
                per_employee: [{employee_id, employee_name, employee_short, group_name,
                                sick_days, sick_episodes, bradford_factor}],
                per_month: [{month, sick_days}],
                per_weekday: [{weekday, weekday_name, sick_days}],
            }
        """
        # Identify sick leave type IDs
        leavt = self.get_leave_types(include_hidden=True)
        sick_ids: set = set()
        for lt in leavt:
            name = (lt.get('NAME', '') or '').lower()
            short = (lt.get('SHORTNAME', '') or '').lower()
            if any(kw in name or kw in short for kw in ['krank', 'sick', 'ku']):
                sick_ids.add(lt['ID'])

        year_str = str(year)

        # Collect sick absences for this year
        sick_abs = []
        for r in self._read('ABSEN'):
            date_val = r.get('DATE', '') or ''
            if date_val.startswith(year_str) and r.get('LEAVETYPID') in sick_ids:
                sick_abs.append(r)

        # Build employee + group info
        employees = self.get_employees(include_hidden=False)
        {e['ID']: e for e in employees}

        # Group membership per employee
        emp_group: Dict[int, str] = {}
        emp_group_id: Dict[int, int] = {}
        for grp in self.get_groups(include_hidden=False):
            gid = grp.get('ID')
            gname = grp.get('NAME', '')
            for mid in self.get_group_members(gid):
                emp_group[mid] = gname
                emp_group_id[mid] = gid

        # Per-employee: collect sick day dates
        emp_dates: Dict[int, list] = {}
        for ab in sick_abs:
            eid = ab.get('EMPLOYEEID')
            date_val = ab.get('DATE', '')
            if eid and date_val:
                if eid not in emp_dates:
                    emp_dates[eid] = []
                emp_dates[eid].append(date_val)

        # Per-month and per-weekday counts
        weekday_names = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
        per_month = [0] * 13   # index 1-12
        per_weekday = [0] * 7  # index 0=Mon … 6=Sun

        for ab in sick_abs:
            date_str = ab.get('DATE', '') or ''
            try:
                d = datetime.strptime(date_str, '%Y-%m-%d')
                per_month[d.month] += 1
                per_weekday[d.weekday()] += 1
            except ValueError:
                pass

        # Build per-employee stats with Bradford Factor
        per_employee = []
        for emp in employees:
            eid = emp['ID']
            dates_sorted = sorted(emp_dates.get(eid, []))
            sick_days = len(dates_sorted)

            # Count distinct episodes (consecutive days → same episode)
            episodes = 0
            if dates_sorted:
                episodes = 1
                prev_d = datetime.strptime(dates_sorted[0], '%Y-%m-%d')
                for d_str in dates_sorted[1:]:
                    d = datetime.strptime(d_str, '%Y-%m-%d')
                    if (d - prev_d).days > 3:  # gap > 3 calendar days = new episode
                        episodes += 1
                    prev_d = d

            bradford = (episodes ** 2) * sick_days  # Bradford Factor = S² × D

            per_employee.append({
                'employee_id': eid,
                'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}".strip(', '),
                'employee_short': emp.get('SHORTNAME', ''),
                'group_name': emp_group.get(eid, ''),
                'group_id': emp_group_id.get(eid, None),
                'sick_days': sick_days,
                'sick_episodes': episodes,
                'bradford_factor': bradford,
            })

        # Sort by sick_days descending, then name
        per_employee.sort(key=lambda x: (-x['sick_days'], x['employee_name']))

        return {
            'year': year,
            'sick_type_ids': sorted(sick_ids),
            'total_sick_days': sum(per_month[1:]),
            'affected_employees': len(emp_dates),
            'total_employees': len(employees),
            'per_employee': per_employee,
            'per_month': [
                {'month': i, 'sick_days': per_month[i]}
                for i in range(1, 13)
            ],
            'per_weekday': [
                {'weekday': i, 'weekday_name': weekday_names[i], 'sick_days': per_weekday[i]}
                for i in range(7)
            ],
        }

    # ── Schedule Templates (Schicht-Vorlagen) ─────────────────

    def _templates_path(self) -> str:
        """Path to schedule_templates.json stored in the DB data directory."""
        return os.path.join(self.db_path, 'schedule_templates.json')

    def _load_templates(self) -> List[Dict]:
        path = self._templates_path()
        if not os.path.exists(path):
            return []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []

    def _save_templates(self, templates: List[Dict]) -> None:
        path = self._templates_path()
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(templates, f, ensure_ascii=False, indent=2)

    def get_schedule_templates(self) -> List[Dict]:
        """Return all saved schedule templates."""
        return self._load_templates()

    def create_schedule_template(self, name: str, description: str,
                                  assignments: List[Dict]) -> Dict:
        """Save a new schedule template.

        Each assignment: {employee_id, weekday_offset (0=Mon..6=Sun), shift_id}.
        """
        import datetime as _dt
        templates = self._load_templates()
        new_id = max((t.get('id', 0) for t in templates), default=0) + 1
        template = {
            'id': new_id,
            'name': name,
            'description': description,
            'assignments': assignments,
            'created_at': _dt.datetime.now().isoformat(timespec='seconds'),
        }
        templates.append(template)
        self._save_templates(templates)
        return template

    def delete_schedule_template(self, template_id: int) -> bool:
        """Delete a template by ID. Returns True if found and deleted."""
        templates = self._load_templates()
        new_list = [t for t in templates if t.get('id') != template_id]
        if len(new_list) == len(templates):
            return False
        self._save_templates(new_list)
        return True

    def apply_schedule_template(self, template_id: int, target_date: str,
                                  force: bool = False) -> Dict:
        """Apply a template to the week starting on target_date (any day = week anchor).

        Each assignment's weekday_offset (0=Mon..6=Sun) is added to target_date.
        Returns { created, updated, skipped } counts.
        """
        from datetime import datetime as _dtd, timedelta as _td

        templates = self._load_templates()
        tmpl = next((t for t in templates if t.get('id') == template_id), None)
        if tmpl is None:
            raise ValueError(f"Template {template_id} not found")

        try:
            week_start = _dtd.fromisoformat(target_date).date()
        except ValueError:
            raise ValueError(f"Invalid target_date: {target_date}")

        created = updated = skipped = 0
        for asgn in tmpl.get('assignments', []):
            employee_id = asgn.get('employee_id')
            shift_id = asgn.get('shift_id')
            weekday_offset = int(asgn.get('weekday_offset', 0))
            if employee_id is None or shift_id is None:
                continue

            target_day = week_start + _td(days=weekday_offset)
            date_str = target_day.isoformat()

            existing = self.get_schedule_day(date_str)
            already_exists = any(e.get('employee_id') == employee_id for e in existing)

            if already_exists and not force:
                skipped += 1
                continue

            if already_exists and force:
                self.delete_schedule_entry(employee_id, date_str)
                updated += 1
            else:
                created += 1

            self.add_schedule_entry(employee_id, date_str, shift_id)

        return {
            'created': created,
            'updated': updated,
            'skipped': skipped,
            'template_name': tmpl.get('name', ''),
        }

    def get_week_entries_for_template(self, year: int, month: int,
                                      week_start_day: int,
                                      group_id: Optional[int] = None) -> List[Dict]:
        """Return all shift schedule entries for a 7-day window starting on week_start_day.

        Adds a 'weekday_offset' (0=Mon..6=Sun) and 'source_date' field to each entry.
        """
        from datetime import date as _date
        from calendar import monthrange as _mr

        days_in_month = _mr(year, month)[1]
        result = []
        for offset in range(7):
            d = week_start_day + offset
            if d > days_in_month:
                break
            date_obj = _date(year, month, d)
            weekday_offset = date_obj.weekday()  # 0=Mon..6=Sun
            date_str = date_obj.isoformat()
            day_entries = self.get_schedule_day(date_str, group_id=group_id)
            for entry in day_entries:
                if entry.get('kind') == 'shift':  # only regular shifts, not absences
                    entry['weekday_offset'] = weekday_offset
                    entry['source_date'] = date_str
                    result.append(entry)
        return result

    # ── Burnout-Radar ─────────────────────────────────────────
    def get_burnout_radar(self, year: int, month: int,
                          streak_threshold: int = 6,
                          overtime_threshold_pct: float = 20.0,
                          group_id: Optional[int] = None) -> List[Dict]:
        """
        Analyse schedule to detect at-risk employees:
        1. Long consecutive work streaks (>= streak_threshold days)
        2. Significant overtime (actual_hours > target_hours * (1 + overtime_threshold_pct/100))
        Returns list of {employee_id, employee_name, risk_level, reasons, streak, overtime_pct}
        """
        from datetime import date as _date, timedelta as _td
        import calendar as _cal

        employees = self.get_employees()
        if group_id:
            group_member_ids = {r.get('EMPLOYEEID') for r in self._read('MAGRP') if r.get('GROUPID') == group_id}
            employees = [e for e in employees if e['ID'] in group_member_ids]

        # Build set of working dates per employee for a 6-week window (3 weeks before + current month)
        days_in_month = _cal.monthrange(year, month)[1]
        month_start = _date(year, month, 1)
        window_start = month_start - _td(days=21)  # 3 weeks before
        window_end = _date(year, month, days_in_month)

        # Collect all working days from MASHI + SPSHI in window
        emp_work_dates: dict[int, set] = {}
        for table in ('MASHI', 'SPSHI'):
            for r in self._read(table):
                d_str = r.get('DATE', '')
                if not d_str or len(d_str) < 10:
                    continue
                try:
                    d = _date.fromisoformat(d_str[:10])
                except ValueError:
                    continue
                if window_start <= d <= window_end:
                    eid = r.get('EMPLOYEEID')
                    if eid:
                        emp_work_dates.setdefault(eid, set()).add(d)

        results = []
        for emp in employees:
            eid = emp['ID']
            worked = sorted(emp_work_dates.get(eid, set()))
            if not worked:
                continue

            # Find longest streak ending in current month
            max_streak = 0
            streak_end_date = None
            i = 0
            while i < len(worked):
                streak = 1
                j = i + 1
                while j < len(worked) and (worked[j] - worked[j - 1]).days == 1:
                    streak += 1
                    j += 1
                if streak > max_streak:
                    max_streak = streak
                    streak_end_date = worked[j - 1]
                i = j

            # Only consider streaks that extend into the current month
            streak_in_month = streak_end_date and streak_end_date >= month_start if streak_end_date else False

            # Get overtime for current month
            try:
                stats = self.get_employee_stats_month(eid, year, month)
                target_h = stats.get('target_hours', 0)
                actual_h = stats.get('actual_hours', 0)
                if target_h > 0:
                    ot_pct = ((actual_h - target_h) / target_h) * 100
                else:
                    ot_pct = 0
                ot_hours = actual_h - target_h
            except Exception:
                ot_pct = 0
                ot_hours = 0
                target_h = 0
                actual_h = 0

            reasons = []
            if streak_in_month and max_streak >= streak_threshold:
                reasons.append(f'{max_streak} Tage am Stück')
            if ot_pct >= overtime_threshold_pct:
                reasons.append(f'+{ot_pct:.0f}% Überstunden ({ot_hours:+.1f}h)')

            if not reasons:
                continue

            # Risk level
            risk_level = 'high' if (
                (streak_in_month and max_streak >= streak_threshold + 2) or ot_pct >= 30
            ) else 'medium'

            results.append({
                'employee_id': eid,
                'employee_name': f"{emp.get('NAME', '')}, {emp.get('FIRSTNAME', '')}",
                'employee_short': emp.get('SHORTNAME', ''),
                'risk_level': risk_level,
                'reasons': reasons,
                'streak': max_streak if streak_in_month else 0,
                'overtime_pct': round(ot_pct, 1),
                'overtime_hours': round(ot_hours, 1),
                'actual_hours': round(actual_h, 1),
                'target_hours': round(target_h, 1),
            })

        # Sort: high risk first, then by streak desc
        results.sort(key=lambda x: (0 if x['risk_level'] == 'high' else 1, -x['streak'], -x['overtime_pct']))
        return results

    # ── Schicht-Tauschbörse ───────────────────────────────────

    def _swap_requests_path(self) -> str:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
        os.makedirs(data_dir, exist_ok=True)
        return os.path.join(data_dir, 'swap_requests.json')

    def _load_swap_requests(self) -> List[Dict]:
        path = self._swap_requests_path()
        if not os.path.exists(path):
            return []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []

    def _save_swap_requests(self, entries: List[Dict]):
        path = self._swap_requests_path()
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)

    def get_swap_requests(self, status: Optional[str] = None,
                          employee_id: Optional[int] = None) -> List[Dict]:
        entries = self._load_swap_requests()
        if status:
            entries = [e for e in entries if e.get('status') == status]
        if employee_id is not None:
            entries = [e for e in entries if (
                e.get('requester_id') == employee_id or
                e.get('partner_id') == employee_id
            )]
        return sorted(entries, key=lambda e: e.get('created_at', ''), reverse=True)

    def create_swap_request(self, requester_id: int, requester_date: str,
                            partner_id: int, partner_date: str,
                            note: str = '') -> Dict:
        import datetime as _dt
        entries = self._load_swap_requests()
        new_id = max((e.get('id', 0) for e in entries), default=0) + 1
        entry = {
            'id': new_id,
            'requester_id': requester_id,
            'requester_date': requester_date,
            'partner_id': partner_id,
            'partner_date': partner_date,
            'note': note,
            'status': 'pending',      # pending | approved | rejected | cancelled
            'created_at': _dt.datetime.now().isoformat(timespec='seconds'),
            'resolved_at': None,
            'resolved_by': None,
            'reject_reason': '',
        }
        entries.append(entry)
        self._save_swap_requests(entries)
        return entry

    def resolve_swap_request(self, swap_id: int, action: str,
                              resolved_by: str = 'planner',
                              reject_reason: str = '') -> Optional[Dict]:
        """action: 'approve' or 'reject'"""
        import datetime as _dt
        entries = self._load_swap_requests()
        for entry in entries:
            if entry.get('id') == swap_id:
                if entry['status'] != 'pending':
                    return None  # already resolved
                entry['status'] = 'approved' if action == 'approve' else 'rejected'
                entry['resolved_at'] = _dt.datetime.now().isoformat(timespec='seconds')
                entry['resolved_by'] = resolved_by
                entry['reject_reason'] = reject_reason
                self._save_swap_requests(entries)
                return entry
        return None

    def cancel_swap_request(self, swap_id: int) -> int:
        entries = self._load_swap_requests()
        for entry in entries:
            if entry.get('id') == swap_id:
                entry['status'] = 'cancelled'
                self._save_swap_requests(entries)
                return 1
        return 0

    def delete_swap_request(self, swap_id: int) -> int:
        entries = self._load_swap_requests()
        new_entries = [e for e in entries if e.get('id') != swap_id]
        if len(new_entries) == len(entries):
            return 0
        self._save_swap_requests(new_entries)
        return 1
