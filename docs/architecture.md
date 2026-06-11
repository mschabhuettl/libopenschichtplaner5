# Architektur: libopenschichtplaner5 (`sp5lib`)

> Stand: 2026-06-12, Version 1.7.0 (pyproject.toml) zuzüglich des noch
> unveröffentlichten `sp5lib`-CLI (siehe `[Unreleased]` im CHANGELOG).
> Analysebasis: vollständiger Quellbaum dieses Repos plus Abgleich mit den
> Schwester-Repos `../openschichtplaner5-api` und `../openschichtplaner5`.

## 1. Zweck & Architektur

### 1.1 Zweck

`libopenschichtplaner5` ist die Kernbibliothek von OpenSchichtplaner5. Sie liest
**und schreibt** die originalen *Schichtplaner5*-Datenbankdateien (FoxPro/dBASE
`.DBF`, native Win32-Anwendung mit UTF-16-LE-Strings), sodass die offene
Web-Anwendung auf exakt denselben Daten arbeitet wie das proprietäre
Windows-Tool — ohne Migration. Seit 1.7.0 sind Schreibzugriffe mit einem
Original-Client interoperabel (Änderungsjournal in den `-L`-Begleittabellen,
CDX-Index-Invalidierung, byte-genaues Encoding), und eine zentrale
Berechnungsschicht (`sp5lib.calculations`) bildet die Rechenregeln des
Originals nach (Soll-/Ist-Stunden, Abwesenheits-Anrechnung, Urlaubskonten,
Zuschläge, Bedarf/Auslastung). Zusätzlich enthält die Bibliothek einen
SQLAlchemy-ORM-Layer (SQLite/PostgreSQL) als Migrationspfad weg von DBF, einen
DBF→ORM-Sync, automatische Alembic-Migrationen, Querschnittsdienste (E-Mail,
Farbe, Auth-Helfer) und ein schlankes CLI (`sp5lib info|dump|validate|sync`).

Distributionsname ist `libopenschichtplaner5`, der **Import-Name bleibt
historisch `sp5lib`** (analog `PyYAML` → `import yaml`); die Bibliothek wurde
mit voller Git-Historie aus `backend/sp5lib/` des App-Repos extrahiert.

- Lizenz: MIT; Python ≥ 3.10; `py.typed` (PEP 561).
- Laufzeit-Abhängigkeiten: `SQLAlchemy>=2.0`, `alembic`, `bcrypt`, `pyotp`,
  `packaging`; Extra `postgres` → `psycopg2-binary`.
- **Keine** HTTP-Endpoints — die REST-Schicht lebt im Schwester-Repo
  `openschichtplaner5-api`. Einziger Entry-Point ist das `sp5lib`-CLI
  (`[project.scripts]`, s. Abschnitt 2.7).

### 1.2 Modulübersicht

| Modul | LoC | Aufgabe |
|---|---:|---|
| `sp5lib/dbf_reader.py` | 269 | Pure-Python-DBF-Leser: Header/Felddeskriptoren, UTF-16-LE-Heuristik (inkl. nicht-lateinischer Schriften), Datums-/Zahlen-Decoding, Binärfelder (`DIGEST`/`CREATIME`/`UUID` als `bytes`), positionsbasierte De-Duplizierung doppelter Feldnamen (5DADEM: `START`/`START2`) |
| `sp5lib/dbf_writer.py` | 692 | Sicherer DBF-Schreiber: `append_record`, `update_record`, `delete_record`, `find_all_records`, `pack_table` (PACK); `flock`, Rollback, EOF-Marker, Record-Size-Guard; Interop mit Original-Clients via `-L`-Änderungsjournal und CDX-Invalidierung |
| `sp5lib/calculations.py` | 1241 | Zentrale, seiteneffektfreie Berechnungsschicht: Soll-/Ist-Stunden (CALCBASE-Dispatcher), Schichtdauern je Tagesindex, Abwesenheits-Anrechnung, Saldo/Überstunden, Urlaubskonten + Verfall + Jahresabschluss, Zuschlagsfenster, Personaltabelle, Bedarf/Auslastung, Zyklen-Expansion |
| `sp5lib/database.py` | 7073 | `SP5Database` — High-Level-Fassade über die DBF-Tabellen (171 öffentliche Methoden) inkl. globalem mtime-Cache, Auth/2FA, Statistiken, JSON-Sidecars; Auswertungen laufen über `sp5lib.calculations` |
| `sp5lib/cli.py` | 209 | `sp5lib`-Konsolenkommando: `info`, `dump` (JSON/CSV), `validate`, `sync` (DBF → SQLite/PostgreSQL) |
| `sp5lib/db_config.py` | 39 | Backend-Wahl via Env (`DB_BACKEND=dbf\|postgresql`, `DATABASE_URL`, `SP5_DB_PATH`) |
| `sp5lib/db_factory.py` | 55 | `get_database()` — liefert `SP5Database` oder (Singleton) `SP5PostgresDatabase` |
| `sp5lib/pg_database.py` | 1904 | `SP5PostgresDatabase` — Teilmenge der `SP5Database`-API auf SQLAlchemy-ORM (76 öffentliche Methoden, davon 7 als geteilte Funktionsobjekte der DBF-Fassade) |
| `sp5lib/sqlite_adapter.py` | 289 | Proof-of-Concept: SQLite-Spiegel der drei Kerntabellen EMPL/GROUP/BOOK (raw `sqlite3`, kein ORM) |
| `sp5lib/orm/` | ~2600 | SQLAlchemy-2.0-Layer: `base` (Engine/Session), `models` (19 kanonische Modelle), `models_pg` (7 PG-Zusatzmodelle + Re-Exporte), `repository` (18 Repositories), `sync` (DBF→ORM-Upsert) |
| `sp5lib/auto_migrate.py` | 435 | Startup-Migration: Alembic-Upgrade (PG) bzw. DBF-Schema-Extensions inkl. Backup (`pg_dump`/`copytree`) |
| `sp5lib/email_service.py` | 273 | SMTP-Benachrichtigungsmails (HTML-escaped, async via Thread), Konfiguration über `SP5_SMTP_*` |
| `sp5lib/color_utils.py` | 26 | Windows-BGR-Integer ↔ Hex/RGB, Luminanz-Check |
| `sp5lib/_resource_paths.py` | 40 | Auflösung der Host-App-Verzeichnisse über `SP5_BACKEND_DIR` (Fallback: In-Tree-Layout) |

### 1.3 Schichten

```
            openschichtplaner5-api (FastAPI-Router)          ← Konsument
 ───────────────────────────────────────────────────────────────────────
   db_factory.get_database()    sp5lib-CLI    auto_migrate / email_service
        │            │                            (Querschnitt)
        ▼            ▼
  SP5Database   SP5PostgresDatabase     sp5lib.orm.repository  ← Fassaden
  (database.py)  (pg_database.py)       (Repository-Pattern)
        │    \        /    │                    │
        │     ▼      ▼     │                    │
        │  sp5lib.calculations                  │
        │  (gemeinsame Rechenregeln)            │
        ▼                  └──────┬─────────────┘
  dbf_reader / dbf_writer         ▼
  (.DBF + -L-Journal, flock)  sp5lib.orm.base (Engine/Session)
        ▲                         │  SQLite (WAL) / PostgreSQL
        │                         ▼
        └───────  sp5lib.orm.sync (DBF → ORM-Upsert)  ────────
```

- **Legacy-Pfad (Default):** `SP5Database` liest die `.DBF`-Dateien über
  `dbf_reader` (mit globalem, mtime-validiertem Cache, `threading.RLock`) und
  schreibt über `dbf_writer` direkt in die Originaldateien. DBF bleibt
  Source of Truth. Jeder Schreibzugriff erzeugt zusätzlich einen
  Journal-Eintrag in der `-L`-Begleittabelle (laufende Original-Clients lesen
  externe Änderungen darüber ein) und löscht die veralteten `.CDX`-Indexdateien
  der geänderten Tabelle, damit das Original sie neu aufbaut.
- **Berechnungsschicht:** `sp5lib.calculations` besteht aus reinen Funktionen
  über `read_dbf`-Records (kein I/O, keine `SP5Database`-Abhängigkeit). Beide
  Fassaden (`SP5Database` und `SP5PostgresDatabase`) rufen dieselben
  Funktionen auf; die Äquivalenz wird in `tests/test_pg_calculations.py`
  getestet.
- **ORM-Pfad:** `sp5lib.orm` ist rein additiv ("Read-Mirror"). `sync.sync_all()`
  spiegelt 19 DBF-Tabellen per Upsert in SQLite/PostgreSQL;
  `SP5PostgresDatabase` bedient denselben Methodensatz wie `SP5Database`
  (Teilmenge, s. Abschnitt 5) direkt aus dem ORM.
- **Nebendaten:** Funktionen ohne DBF-Gegenstück (bcrypt-Hashes, TOTP,
  Wünsche, Tauschanfragen, iCal-Tokens, Templates, Changelog, Kommentare,
  Arbeitsplatz-Zuordnungen) speichert `SP5Database` in JSON-Sidecars — teils
  neben den DBF-Dateien (`db_path`), teils im Backend-Datenverzeichnis der
  Host-App (`_resource_paths.data_dir()` / `api_data_dir()` via
  `SP5_BACKEND_DIR`).

### 1.4 Datenflüsse

**DBF-Lesepfad** (`dbf_reader.read_dbf`):
1. 32-Byte-Header parsen (`num_records`, `header_size`, `record_size`).
2. 32-Byte-Felddeskriptoren bis Terminator `0x0D` lesen (Name, Typ, Länge,
   Dezimalstellen). Doppelte Feldnamen werden positionsbasiert disambiguiert:
   das zweite `START`-Feld der 5DADEM heißt im Record-Dict `START2`
   (Schreibpfad verwendet dieselbe Konvention, Werte round-trippen).
3. Records sequenziell lesen; gelöschte Records (Flag `0x2A`) überspringen.
4. Feld-Decoding nach Typ:
   - `C` (Character): Binärfelder (`DIGEST`, `CREATIME`, `UUID` —
     `BINARY_C_FIELDS`) kommen als rohe, ungestrippte `bytes` zurück (ein
     dekodierter MD5-Digest wäre irreversibel zerstört). Sonst Heuristik
     `_is_utf16_le` (Bytes < `0x08` an ungeraden Positionen, erkennt auch
     Griechisch/Kyrillisch/Hebräisch/Arabisch) → UTF-16-LE mit
     `\x00\x00`-Terminator, sonst Latin-1 (Datenfelder wie `WORKDAYS`,
     `STARTEND*` sind plain ASCII).
   - `D` (Date): `YYYYMMDD` → ISO `YYYY-MM-DD` mit **echter Kalendervalidierung**
     über `datetime.date` (weist z. B. `20230231` ab → `None`).
   - `N`/`F`: int/float, leere/defekte Werte → `0`.
   - `L`: `T/t/Y/y/1` → `True`.
   - `M` (Memo): immer `None` — `.DBT`-Memodateien werden **nicht** gelesen.
5. Fehlertoleranz: fehlende/abgeschnittene/korrupte Datei → leere Liste statt Exception.

**DBF-Schreibpfad** (`dbf_writer`), Encoding-Kontrakt byte-genau gegen reale
SP5-Dateien verifiziert (`tests/test_writer_parity.py`): Text-`C` =
UTF-16-LE + `\x00\x00` + `\x20`-Padding; ASCII-Klasse (`WORKDAYS`,
`VALIDDAYS`, `DAILYDEM`, `STARTEND*`, 5USER-`CATEGORY`/`REPORT`) = cp1252;
Binär-`C` = rohe Bytes mit `\x00`-Padding; `D` = `YYYYMMDD` ASCII; `N`
rechtsbündig; `F` immer mit 4 Nachkommastellen (wie in den Originaldateien,
unabhängig vom `dec`-Byte des Deskriptors); `L` = `T`/`F`:
- `append_record`: Record-Bytes bauen → Record-Size-Mismatch wird abgewiesen
  statt still abzuschneiden → exklusives `fcntl.flock` → Record-Count
  **innerhalb des Locks neu lesen** (TOCTOU-Schutz) → vor dem EOF-Marker
  (`0x1A`) schreiben → Marker re-appenden → Count + Header-Datum (Bytes 1–3)
  aktualisieren. Bei Schreibfehler Rollback per `truncate()` auf die
  Vor-Schreibposition.
- `update_record`: Read-modify-write des Zielrecords **unter demselben Lock**;
  nur übergebene Felder werden überschrieben; gelöschte Records werden abgewiesen.
- `delete_record`: setzt nur das Delete-Flag `0x2A` (Soft-Delete wie dBASE).
- `pack_table`: PACK — entfernt als gelöscht markierte Records physisch,
  aktualisiert Count/Datum, leert anschließend das `-L`-Journal (Zähler-Reset,
  Original-Clients machen daraufhin einen Voll-Reload) und löscht die
  CDX-Dateien von Tabelle und Journal. `SP5Database.compact_database()` packt
  darüber alle Tabellen.
- `find_all_records`: Shared Lock, liefert `(raw_index, record)` mit
  AND-Filtersemantik; `raw_index` ist direkt an `delete_record`/`update_record`
  übergebbar.
- **Journal & Indizes (nach jedem erfolgreichen Schreibzugriff):** ein Eintrag
  in der `-L`-Begleittabelle (`NUMBER` fortlaufend, `CHANGEID1..3` =
  tabellenspezifischer zusammengesetzter Schlüssel, `CHANGE` = 1 für
  Upsert / 2 für Delete) plus Löschen der veralteten `.CDX`-Dateien der
  Tabelle (Original baut sie beim nächsten Öffnen neu; Opt-out über
  `INVALIDATE_CDX = False`). Ein fehlendes oder defektes `-L`-File degradiert
  zu einer Warnung und blockiert den Datenschreibzugriff nie.
- Integritäts-Guards: numerischer Overflow wirft `ValueError` statt still die
  höchstwertigen Stellen abzuschneiden; String-Truncation wird per
  `logger.warning` sichtbar gemacht.
- **Grenze der Interop:** das Original nutzt CodeBase-Byte-Range-Locks in den
  Dateien, diese Bibliothek POSIX-`flock` pro Datei — die beiden
  Locking-Verfahren sehen einander nicht. Gleichzeitiges Schreiben, während
  ein Original-Client läuft, ist daher nicht sicher; sequenzielle Koexistenz
  (Original geschlossen während der Schreibzugriffe) ist es.

**DBF→ORM-Sync** (`orm/sync.py`): pro Tabelle Upsert nach DBF-`ID`
(`session.get` → update oder insert). Besonderheiten:
- `sync_groups`: zweiphasig — `super_id` (Selbstreferenz) wird erst nach dem
  Anlegen aller Gruppen aufgelöst; hängende Referenzen → `NULL` + Log.
- `sync_group_assignments` und `sync_cycle_assignments`: die DBF-`ID` in
  5GRASG/5CYASS ist **kein** globaler Schlüssel → Full delete + Re-Insert mit
  Autoincrement-PK und De-Duplizierung auf `(employee_id, group_id)` bzw.
  `(employee_id, cycle_id, start)`; hängende Referenzen werden übersprungen.
- Datumsbehaftete Tabellen (`MASHI`, `SPSHI`, `ABSEN`, `HOLID`, `BOOK`,
  `OVER`, `SPDEM`): Zeilen mit leerem/ungültigem `DATE` werden übersprungen
  und gezählt geloggt.
- Referenzspalten (employee/shift/leave-type/workplace) sind bewusst **plain
  Integer ohne DB-Foreign-Key**, damit schmutzige Altdaten synchronisierbar
  bleiben.
- `sync_all(engine, daten_path)` → Dict mit 19 Zählern, eine Transaktion
  (Commit am Ende, Rollback bei Fehler).

## 2. Öffentliche Schnittstelle

Die Schnittstellen sind die Python-Module und das `sp5lib`-CLI (Abschnitt 2.7);
HTTP-Endpoints gibt es nicht. `sp5lib/__init__.py` ist (bis auf einen
Kommentar) leer — Konsumenten importieren Submodule direkt.

### 2.1 `sp5lib.dbf_reader`
- `read_dbf(filepath, encoding_hint="utf-16-le") -> list[dict]` — kompletter
  Tabelleninhalt ohne gelöschte Records; Binärfelder als `bytes`, doppelte
  Feldnamen disambiguiert (`START2`). Hinweis: `encoding_hint` wird intern
  nicht ausgewertet (s. Abschnitt 5).
- `get_table_fields(filepath) -> list[dict]` — Felddeskriptoren
  (`name`, `type`, `len`, `dec`).
- `BINARY_C_FIELDS` — Menge der als `bytes` behandelten `C`-Felder
  (`DIGEST`, `CREATIME`, `UUID`).
- Semi-öffentliche Helfer (vom API-Repo bzw. Writer/CLI mitbenutzt):
  `_decode_string`, `_is_utf16_le`, `_parse_date`, `_parse_record`,
  `_dedupe_names`.

### 2.2 `sp5lib.dbf_writer`
- `append_record(filepath, fields, record) -> int` (neuer Record-Count)
- `update_record(filepath, fields, record_index, data) -> None`
- `delete_record(filepath, fields, record_index) -> None`
- `pack_table(filepath) -> int` (Anzahl physisch entfernter Records)
- `find_all_records(filepath, fields=None, **filters) -> list[tuple[int, dict]]`
- Modul-Schalter `INVALIDATE_CDX` (Default `True`): CDX-Invalidierung nach
  Schreibzugriffen abschaltbar.
- Semi-öffentlich genutzt: `_encode_string`, `_encode_field`, `_read_header_info`.

### 2.3 `sp5lib.database` — `SP5Database(db_path)`
Die zentrale DBF-Fassade; **171 öffentliche Methoden**. Gruppiert (vollständige
Aufstellung nach Funktionsbereich, Methodennamen wie im Code):

- **Stammdaten lesen:** `get_employees`, `get_employee`, `get_groups`,
  `get_group_members`, `get_all_group_members`, `get_employee_groups`,
  `get_shifts`, `get_shift`, `get_leave_types`, `get_leave_type`,
  `get_workplaces`, `get_holidays`, `get_holiday_dates`, `get_periods`,
  `get_extracharges`, `get_all_group_assignments`, `get_stats`.
- **Stammdaten schreiben:** `create_employee`, `update_employee`,
  `delete_employee` (Soft-Delete `HIDE`), `activate_employee`, `create_group`,
  `update_group`, `delete_group`, `add_group_member`, `remove_group_member`,
  `create_shift`, `update_shift`, `hide_shift`, `shift_active_usage_count`,
  `create_leave_type`, `update_leave_type`, `hide_leave_type`,
  `leave_type_active_usage_count`, `create_holiday`, `update_holiday`,
  `delete_holiday`, `create_workplace`, `update_workplace`, `hide_workplace`,
  `create_period`, `delete_period`, `create_extracharge`, `update_extracharge`,
  `delete_extracharge`.
- **Dienstplan:** `get_schedule`, `get_schedule_day`, `get_schedule_week`,
  `get_schedule_year`, `add_schedule_entry`, `delete_schedule_entry`,
  `delete_shift_only`, `delete_absence_only`, `add_absence`, `update_absence`
  (inkl. Teiltags-Abwesenheiten via INTERVAL/START/END), `get_absences_list`,
  `add_spshi_entry`, `update_spshi_entry`, `delete_spshi_entry_by_id`,
  `get_spshi_entries_for_day`, `get_schedule_conflicts`.
- **Bedarf/Besetzung:** `get_staffing`, `get_staffing_requirements`,
  `set_staffing_requirement`, `get_special_staffing`,
  `create_special_staffing`, `update_special_staffing`,
  `delete_special_staffing`.
- **Zyklen (Schichtmodelle):** `get_cycles`, `get_shift_cycles`,
  `get_shift_cycle`, `create_shift_cycle`, `update_shift_cycle`,
  `delete_shift_cycle`, `set_cycle_entry`, `clear_cycle_entries`,
  `get_cycle_assignments`, `get_cycle_assignment_for_employee`,
  `assign_cycle`, `remove_cycle_assignment`, `generate_schedule_from_cycle`
  (Auto-Planung inkl. Verfügbarkeit/Skills/Wochenstunden/RESTR),
  `get_cycle_exceptions`, `set_cycle_exception`, `delete_cycle_exception`.
- **Restriktionen/Urlaubssperren:** `get_restrictions`, `set_restriction`,
  `remove_restriction`, `get_holiday_bans`, `create_holiday_ban`,
  `delete_holiday_ban`.
- **Konten/Urlaub/Überstunden:** `get_leave_entitlements`,
  `set_leave_entitlement`, `get_leave_balance`, `get_leave_balance_group`,
  `get_overtime_records`, `get_overtime_summary`, `get_bookings`,
  `create_booking`, `delete_booking`, `get_carry_forward`,
  `set_carry_forward`, `calculate_annual_statement`, `calculate_time_balance`,
  `get_zeitkonto`, `get_annual_close_preview`, `check_annual_close_exists`,
  `run_annual_close` (Option `keep_entitlements`), `forfeit_rest`
  (Resturlaubs-Verfall), `calculate_extracharge_hours` (auch über freie
  Zeiträume).
- **Statistik/Auswertung:** `get_statistics` (Monat oder freier
  Auswertungszeitraum), `get_personnel_table`, `get_utilization`,
  `get_employee_stats_year`, `get_employee_stats_month`,
  `get_sickness_statistics`, `get_burnout_radar`.
- **Benutzer/Auth/2FA:** `get_users`, `create_user`, `update_user`,
  `delete_user`, `check_user_permission`, `get_user_permissions` (granulare
  5USER-Rechte-Flags), `verify_user_password` (bcrypt-Sidecar zuerst,
  MD5-`DIGEST`-Fallback mit Auto-Upgrade auf bcrypt), `change_password`,
  `totp_get_status`, `totp_generate_secret`, `totp_enable`, `totp_verify`,
  `totp_disable`, `get_employee_access`, `set_employee_access`,
  `delete_employee_access`, `get_group_access`, `set_group_access`,
  `delete_group_access`, `get_usett`, `update_usett`.
- **Notizen/Kommentare/Wünsche/Tausch:** `get_notes`, `add_note`,
  `update_note`, `delete_note`, `get_schedule_comments`,
  `add_schedule_comment`, `delete_schedule_comment`, `get_wishes`, `add_wish`,
  `update_wish_status`, `delete_wish`, `get_swap_requests`,
  `create_swap_request`, `get_swap_request_history`, `partner_respond_swap`,
  `resolve_swap_request`, `cancel_swap_request`, `expire_old_swap_requests`,
  `delete_swap_request`.
- **Templates/iCal/Workplace-Zuordnung/Changelog:** `get_schedule_templates`,
  `create_schedule_template`, `delete_schedule_template`,
  `apply_schedule_template`, `get_week_entries_for_template`,
  `get_ical_token_for_employee`, `create_ical_token`, `revoke_ical_token`,
  `resolve_ical_token`, `get_workplace_employees`,
  `assign_employee_to_workplace`, `remove_employee_from_workplace`,
  `get_changelog`, `log_action`.
- **Wartung:** `compact_database` (PACK über alle Tabellen via
  `dbf_writer.pack_table`, liefert Zähler je Tabelle).

Modul-Global (vom API-Repo direkt verwendet): `_GLOBAL_DBF_CACHE`,
`_CACHE_LOCK` (Cache-Verwaltung über Prozessgrenzen der Worker).

### 2.4 `sp5lib.pg_database` — `SP5PostgresDatabase(database_url)`
Gleiche Methodensignaturen wie `SP5Database`, aber nur eine **Teilmenge von 76
Methoden** (Stammdaten-CRUD, Schedule-Basics, Users/2FA inkl. granularer
Rechte, Notizen, Statistik/Personaltabelle/Auslastung, Urlaubskonten lesen +
Verfall, Changelog, `init_db`). Sieben Auswertungsmethoden sind **dieselben
Funktionsobjekte** wie in der DBF-Fassade (`calculate_time_balance`,
`get_zeitkonto`, `get_employee_stats_year`/`_month`, `get_schedule_year`,
`calculate_extracharge_hours`, `get_leave_balance_group`) und laufen über
gespiegelte Adapter-Helfer durch `sp5lib.calculations`. 96 Methoden der
DBF-Fassade fehlen (Details Abschnitt 5);
`run_annual_close`/`get_annual_close_preview` werfen gezielt
`NotImplementedError`.

### 2.5 `sp5lib.orm` (Paket-`__all__`, vollständig)
- **Engine/Session/Schema:** `Base`, `get_engine`, `get_session`, `init_db`
  (zusätzlich importierbar, aber nicht in `__all__`: `base.session_scope`).
- **Modelle (kanonisch in `orm/models.py`):** `Company`, `Employee`, `Group`,
  `GroupAssignment`, `Shift`, `LeaveType`, `Workplace`, `ShiftAssignment`,
  `SpecialShift`, `Absence`, `Holiday`, `Period`, `AccountBooking`,
  `OvertimeEntry`, `LeaveEntitlement`, `ShiftDemand`, `SpecialDemand`,
  `Cycle`, `CycleAssignment`, `Restriction`.
- **Legacy-Aliase (gleiche Tabellen):** `ScheduleEntry` (= `ShiftAssignment`),
  `Booking` (= `AccountBooking`), `OvertimeRecord` (= `OvertimeEntry`),
  `StaffingRequirement` (= `ShiftDemand`).
- **Repositories:** `EmployeeRepository`, `GroupRepository`, `ShiftRepository`,
  `LeaveTypeRepository`, `WorkplaceRepository`, `ShiftAssignmentRepository`,
  `SpecialShiftRepository`, `AbsenceRepository`, `HolidayRepository`,
  `PeriodRepository`, `AccountBookingRepository`, `OvertimeEntryRepository`,
  `LeaveEntitlementRepository`, `ShiftDemandRepository`,
  `SpecialDemandRepository`, `CycleRepository`, `CycleAssignmentRepository`,
  `RestrictionRepository`.
- **Nur in `orm/models_pg.py` (ohne DBF-Sync):** `User`, `CycleEntry`, `Note`,
  `HolidayBan`, `ExtraCharge`, `Settings`, `ChangelogEntry` (+ Re-Exporte
  aller kanonischen Modelle und der Aliase für Bestandsimporte).
- **`orm/sync.py`:** `sync_employees`, `sync_groups`, `sync_group_assignments`,
  `sync_shifts`, `sync_leave_types`, `sync_workplaces`,
  `sync_shift_assignments`, `sync_special_shifts`, `sync_absences`,
  `sync_holidays`, `sync_periods`, `sync_book`, `sync_overtime`,
  `sync_leave_entitlements`, `sync_shift_demand`, `sync_special_demand`,
  `sync_cycles`, `sync_cycle_assignments`, `sync_restrictions`, `sync_all`.

### 2.6 `sp5lib.calculations`
Reine Funktionen über `read_dbf`-Records; die Mitarbeiter-Rechenparameter
(5EMPL) reisen als `EmployeeContext`-Dataclass, der Feiertagskalender (5HOLID)
als `dict[date, int]` (`holiday_calendar()`). Wichtigste Funktionsgruppen:
- **Parsing/Basis:** `to_date`, `parse_day_mask`, `parse_startend`,
  `holiday_calendar`, `day_index` (0 = Montag … 6 = Sonntag, 7 = Feiertag),
  `clamp_to_employment`, `is_employed`, `count_working_days`,
  `is_working_day`, `booking_sum`.
- **Soll/Ist:** `get_nominal_hours` (CALCBASE-Dispatcher: Tages-/Wochen-/
  Monats-/Gesamtbasis), `shift_hours_on_day` (`DURATION0..7`),
  `get_work_hours`, `get_actual_hours`, `duty_day_counts`.
- **Abwesenheiten:** `absence_hours`, `charge_factor`
  (CHARGETYP/CHARGEHRS/COUNTALL), `absence_sums`, `absence_days_by_type`.
- **Saldo/Konten:** `get_saldo`, `get_overtime_hours`, `leave_taken`,
  `leave_account`, `annual_close`, `forfeit_rest`.
- **Zuschläge:** `daily_work_intervals`, `extracharge_hours_on_day`,
  `get_extracharge_hours` (Fenster-Schnittmengen statt DURATION-Summen).
- **Personaltabelle/Bedarf:** `shift_assignment_counts`,
  `count_special_shifts`, `personnel_table_row`, `count_assigned`,
  `utilization_status`, `demand_for_day`, `expand_cycle_assignments`
  (5CYASS-Rotation → konkrete Dienste).

### 2.7 CLI (`sp5lib.cli`, Konsolenkommando `sp5lib`)
Arbeitet direkt auf einem Schichtplaner5-Datenverzeichnis (`5*.DBF`):
- `sp5lib info <Daten>` — Tabellenübersicht (Records/Felder je Tabelle,
  SP5-Build aus 5BUILD).
- `sp5lib dump <Daten> <Tabelle> [--json|--csv] [--limit N]` — Tabelleninhalt
  als JSON (Default) oder CSV; Binärfelder als Hex-String; Tabellennamen
  tolerant (`EMPL` ≙ `5EMPL.DBF`).
- `sp5lib validate <Daten>` — liest alle Tabellen, meldet Header-/
  Encoding-Probleme (Exit-Code ≠ 0 bei Befund).
- `sp5lib sync <Daten> --target sqlite:PATH|postgres:URL` — DBF → SQLite/
  PostgreSQL über `sp5lib.orm.sync.sync_all`.

Das CLI ist zugleich die Default-Stage des Dockerfile (`cli`: slim, non-root,
`ENTRYPOINT ["sp5lib"]`); `docker-compose.yml` bietet dafür den Service
`tools` und für Lint+Tests den Service `test` (Stage `test`).

### 2.8 Übrige Module
- `sp5lib.db_config`: `get_db_backend()`, `get_database_url()`,
  `is_postgresql()`, Konstanten `BACKEND_DBF`, `BACKEND_POSTGRESQL`.
- `sp5lib.db_factory`: `get_database(db_path=None)`.
- `sp5lib.sqlite_adapter`: `SP5SQLiteAdapter(sqlite_path)` mit `init_db()`,
  `sync_from_dbf(daten_path)`, `get_employees()`, `get_groups()`,
  `get_bookings_for_employee(id)`.
- `sp5lib.auto_migrate`: `run_startup_migration() -> MigrationResult`,
  `MigrationResult` (`success`, `had_migrations`, `to_dict()`), Konstanten
  `APP_SCHEMA_VERSION`, `DBF_SCHEMA_VERSION`.
- `sp5lib.email_service`: `EmailConfig`, `get_config()`, `send_email(...)`,
  `send_email_async(...)`, `send_notification_email(...)`.
- `sp5lib.color_utils`: `bgr_to_hex`, `bgr_to_rgb`, `is_light_color`.
- `sp5lib._resource_paths`: `backend_dir()`, `data_dir()`, `api_data_dir()`
  (gesteuert über `SP5_BACKEND_DIR`).

### 2.9 Konfigurations-Umgebungsvariablen (gesamte Lib)
`DB_BACKEND`, `DATABASE_URL`, `SP5_DB_PATH`, `SP5_BACKEND_DIR`,
`AUTO_MIGRATE`, `AUTO_BACKUP`, `SP5_SMTP_HOST/PORT/USER/PASSWORD/FROM/TLS/ENABLED`,
`SP5_APP_URL`. Nur für die Tests: `SP5_GOLDEN_DB` (Abschnitt 3.5).

## 3. Was ist implementiert (IST-Stand)

### 3.1 DBF-Kern (vollständig produktiv)
- Lesepfad für alle in SP5 vorkommenden Feldtypen außer Memo-Inhalten;
  UTF-16-LE-Erkennung per Heuristik (inkl. nicht-lateinischer Schriften);
  Binärfelder als rohe `bytes`; Kalendervalidierung von Datumsfeldern;
  De-Duplizierung doppelter Feldnamen; defensives Verhalten bei
  fehlenden/korrupten Dateien.
- Schreibpfad mit POSIX-Locking (`fcntl.flock`), TOCTOU-sicherem Record-Count,
  Rollback bei Teilschreibern, EOF-Marker-Erhalt, Header-Datums-Stempel,
  Overflow- und Record-Size-Schutz; Dateien sind byte-kompatibel zum
  Original-Format (per Parity-Tests abgesichert).
- Interop mit Original-Clients: `-L`-Änderungsjournal je Schreibzugriff,
  CDX-Invalidierung, `pack_table` (PACK inkl. Journal-Reset). Grenze:
  gleichzeitiges Schreiben bei laufendem Original-Client ist nicht sicher
  (unterschiedliche Locking-Verfahren, s. Abschnitt 1.4).
- `SP5Database` nutzt **30 DBF-Tabellen**: ABSEN, BOOK, CYASS, CYCLE,
  CYENT, CYEXC, DADEM, EMACC, EMPL, GRACC, GRASG, GROUP, HOBAN, HOLID, LEAEN,
  LEAVT, MAGRP, MASHI, NOTE, OVER, PERIO, RESTR, SHDEM, SHIFT, SPDEM, SPSHI,
  USER, USETT, WOPL, XCHAR.
- Globaler, threadsicherer DBF-Cache (mtime-basiert, `RLock`), explizite
  Invalidierung nach allen eigenen Schreibzugriffen.

### 3.2 Fachfunktionen in `SP5Database`
Voll implementiert (gegen DBF + JSON-Sidecars): kompletter Stammdaten-CRUD,
Monats-/Wochen-/Jahres-Dienstplan (inkl. expandierter, nicht materialisierter
Zyklusdienste im Lesepfad), Sonderschichten, Abwesenheiten (auch teiltags),
Konflikterkennung, Besetzungsbedarf (regulär + datumsbezogen),
Schichtmodelle/Zyklen inkl. automatischer Plangenerierung
(Verfügbarkeits-/Skill-/Wochenstunden-/Restriktions-Prüfungen),
Urlaubskonten + Jahresabschluss (`keep_entitlements`) + Resturlaubs-Verfall,
Zeitkonto/Überstunden, Zuschlagsberechnung über Zeitfenster, Statistiken
(Monat/Jahr/freier Zeitraum, Personaltabelle, Auslastung, Krankenstand,
Burnout-Radar), Benutzerverwaltung mit Rollenmodell und granularen
Rechte-Flags, Passwort-Verifikation MD5→bcrypt-Auto-Migration, TOTP-2FA mit
Backup-Codes, Mitarbeiter-/Gruppen-Zugriffsrechte (5EMACC/5GRACC), Notizen,
Plan-Kommentare, Wunsch- und Tauschbörse mit Status-Historie und Ablauf,
Schedule-Templates, iCal-Token-Verwaltung, Audit-Changelog (max. 5000
Einträge), Datenbank-Komprimierung (PACK). Alle Auswertungen rechnen über
`sp5lib.calculations`.

### 3.3 ORM-Layer (Read-Mirror komplett, Phase 1–6)
- 19 DBF-Tabellen als SQLAlchemy-2.0-Modelle mit `to_dict()`, das die echten
  DBF-Schlüssel spiegelt (z. B. `LEAVETYPID`, `ENTITLEMNT`, `DESCRIPT`,
  `RESERVED`); `init_db()` legt alle Tabellen an.
- 18 Repositories: Read-API (`list`/`get` mit Datums-, Jahres-, Employee-,
  Gruppen-, Hidden-Filtern); `EmployeeRepository`/`GroupRepository` zusätzlich
  mit Write-Methoden (`create`, `update`, `soft_delete`, `search`,
  Member-Management).
- `sync_all()` über 19 Tabellen, robust gegen schmutzige Altdaten (s. 1.4).
- SQLite-Engine mit WAL + `foreign_keys=ON`-Pragma; PG-Engine mit Pooling und
  `pool_pre_ping`.
- Laut Roadmap in `orm/README.md` bewusst **nicht** implementiert:
  ORM→DBF-Write-back (Phase 7) und die FastAPI-Anbindung (App-seitig).

### 3.4 PostgreSQL-Backend
`SP5PostgresDatabase` deckt Stammdaten-CRUD, Schedule-Grundoperationen,
User-/2FA-Verwaltung (inkl. granularer Rechte), Notizen, Changelog und die
Auswertungsschicht ab — identische Response-Shapes wie die DBF-Fassade
(`to_dict()`-Spiegelung, `_color_fields`, generierte `SHORTNAME`s,
`TIMES_BY_WEEKDAY`). **Berechnungsparität:** Statistiken, Personaltabelle,
Auslastung, Zeitkonto, Jahres-/Monatsstatistik, Jahresplan, Zuschläge,
Urlaubskonten und Resturlaubs-Verfall laufen über dieselben
`sp5lib.calculations`-Funktionen wie auf DBF;
`tests/test_pg_calculations.py` füttert beide Backends mit identischen
Fixtures und prüft auf identische Ergebnisse. Dokumentierte Brücken:
`HRSMONTH > 0` gilt als Monatsbasis (das PG-Schema hat keine
CALCBASE-Spalte); Zyklus-Ausnahmen (5CYEXC) haben keinen ORM-Spiegel und
werden auf PG nicht angewandt; der Jahresabschluss wirft
`NotImplementedError` (kein Entitlement-Schreibpfad im ORM-Spiegel).

### 3.5 Infrastruktur
- `auto_migrate.run_startup_migration()`: PG → Alembic-`upgrade head` mit
  vorherigem `pg_dump`-Backup; DBF → versionsgestempelte Extension-Registry
  (derzeit leer) mit Verzeichnis-Backup.
- E-Mail-Service mit XSS-sicherem HTML-Template (Escaping vor
  `<br>`-Konvertierung, Scheme-Whitelist für CTA-Links) und
  Notification-Typ→Betreff-Mapping (deutsch).
- Tests: **187 Tests in 13 Dateien** (`pytest -q`):
  `test_orm.py` (50; In-Memory-SQLite, Modelle/Repos/Sync inkl.
  Altdaten-Härtefälle), `test_calculations.py` (42; Berechnungsschicht inkl.
  Randfälle), `test_dbf.py` (19; Roundtrip mit selbstgebauten Mini-DBFs,
  Overflow-/Datums-Guards), `test_database_calculations.py` (19;
  Fassaden-Verdrahtung), `test_writer_parity.py` (15; Byte-Parität, Journal,
  CDX, PACK), `test_cli.py` (14), `test_pg_calculations.py` (12;
  DBF↔PG-Äquivalenz), `test_thread_safety.py` (4), `test_absence_partial_day.py`
  (4), `test_annual_close_options.py`, `test_cache_invalidation.py`,
  `test_holiday_writes.py`, `test_user_permissions.py` (je 2).
- **Golden-Suite** (`tests/test_golden_sample_db.py`, 18 Tests): läuft gegen
  eine lokale Original-Beispieldatenbank, wenn `SP5_GOLDEN_DB=/pfad/zu/Daten`
  gesetzt ist (sonst Modul-Skip). Die Referenz-DB bleibt lokal und wird nie
  committet.
- CI (`.github/workflows/ci.yml`): ruff + pytest auf Python 3.10–3.12.
- Release (`.github/workflows/release.yml`): Build (sdist + wheel,
  `twine check --strict`) und Publish nach **PyPI via Trusted
  Publishing/OIDC** bei `v*`-Tags; manueller `workflow_dispatch` →
  TestPyPI-Dry-Run.
- Docker: Stage `test` (reproduzierbares ruff + pytest), Stage `cli`
  (Default-Image mit `ENTRYPOINT ["sp5lib"]`).

## 4. Cross-Repo-Verdrahtung

Drei unabhängige Schwester-Repos (Geschwisterverzeichnisse unter
`~/Projects/`):

```
openschichtplaner5 (App)  ──requirements──►  openschichtplaner5-api (API)
        │                                            │
        └────────────── beide ──────────────────────►  libopenschichtplaner5 (dieses Repo)
```

**Release-Pfad (Produktion/CI):**
- `openschichtplaner5-api/pyproject.toml` deklariert
  `libopenschichtplaner5[postgres]>=1.7.0` (PyPI-Release dieses Repos).
- `openschichtplaner5/backend/requirements.txt` deklariert
  `openschichtplaner5-api>=1.2.0` **und** direkt
  `libopenschichtplaner5[postgres]>=1.7.0` (bewusst direkte Abhängigkeit, da
  App-Skripte `sp5lib` selbst importieren).
- Veröffentlicht wird durch Pushen eines `v*`-Tags hierauf
  (Trusted Publishing, keine gespeicherten Tokens).

**Editable-Workflow (Entwicklung):**
- App-Repo: `make dev-link` → `pip install -e ../libopenschichtplaner5
  -e ../openschichtplaner5-api` ins `backend/.venv` (dokumentiert in
  `openschichtplaner5/docs/DEVELOPMENT.md`).
- API-Repo-README: `pip install -e "../libopenschichtplaner5[postgres]"`.
- Daher müssen die drei Repos als Geschwister ausgecheckt sein, damit die
  `../`-Pfade auflösen.

**Laufzeit-Kopplung an die Host-App:**
- Die API setzt `SP5_BACKEND_DIR`, damit `_resource_paths` die
  JSON-Datenverzeichnisse (`<backend>/data`, `<backend>/api/data`) und
  `auto_migrate` die `alembic.ini`/`alembic/`-Struktur der Host-App findet —
  die Alembic-Migrationsskripte selbst liegen **nicht** in diesem Repo.
- `DB_BACKEND`/`DATABASE_URL`/`SP5_DB_PATH` wählen das Backend zur Laufzeit.
- Das API-Repo importiert breit aus `sp5lib` (u. a. `database`, `db_factory`,
  `dbf_reader`/`dbf_writer`, `orm`, `auto_migrate`, `email_service`,
  `color_utils`) — inklusive einiger **privater** Symbole
  (`_GLOBAL_DBF_CACHE`, `_decode_string`, `_encode_string`,
  `_read_header_info`); diese sind de facto Teil des Kompatibilitätsvertrags.
- Dieses Repo selbst referenziert keines der Schwester-Repos (keine
  Abhängigkeit nach "oben"), nur die `[project.urls]` verweisen auf die App.

## 5. Known Issues & Einschränkungen

1. **API-Paritätslücke DBF ↔ PostgreSQL:** `sp5lib/pg_database.py` bietet
   76 der 171 öffentlichen Methoden der DBF-Fassade; **96 fehlen** (u. a.
   Tausch-/Wunschbörse, iCal-Tokens, Zyklen-Schreibpfad/Auto-Planung,
   Templates, Besetzungsbedarf, Restriktionen, Buchungs-/Konten-Schreibpfad,
   Zugriffsrechte 5EMACC/5GRACC, Krankenstand/Burnout-Radar,
   Konflikterkennung, Perioden- und Zuschlags-Stammdaten-Schreibpfad).
   `sp5lib/db_factory.py` und der Docstring in `pg_database.py` ("same public
   API … seamless switch") versprechen mehr Austauschbarkeit, als derzeit
   existiert — mit `DB_BACKEND=postgresql` laufen entsprechende API-Routen
   auf `AttributeError`. Die Auswertungs-/Berechnungsmethoden selbst sind
   seit 1.7.0 paritätisch (s. 3.4).
2. **PG-Berechnungsbrücken:** Jahresabschluss
   (`run_annual_close`/`get_annual_close_preview`) wirft auf PG
   `NotImplementedError`; `HRSMONTH > 0` wird mangels CALCBASE-Spalte als
   Monatsbasis interpretiert; Zyklus-Ausnahmen (5CYEXC) werden auf PG nicht
   angewandt (kein ORM-Spiegel).
3. **ORM-Spiegel deckt nicht alle DBF-Tabellen ab:** `database.py` nutzt 30
   Tabellen, `orm/sync.py` spiegelt 19. Ohne Sync sind u. a. `5USER`, `5NOTE`,
   `5CYENT`, `5USETT`, `5XCHAR`, `5HOBAN`, `5EMACC`, `5GRACC`, `5CYEXC`,
   `5DADEM`, `5MAGRP` — obwohl `orm/models_pg.py` für einige davon Modelle
   definiert (`User`, `Note`, `CycleEntry`, `HolidayBan`, `ExtraCharge`,
   `Settings`, `ChangelogEntry`). Ein DBF→PG-Umstieg verliert also Benutzer,
   Notizen und Zyklus-Details, sofern die App das nicht selbst befüllt.
4. **Kein paralleler Schreibbetrieb mit laufendem Original-Client:** das
   Original nutzt CodeBase-Byte-Range-Locks (atomare Gruppen-Locks über Haupt-
   und `-L`-Dateien), diese Bibliothek POSIX-`flock` pro Datei — die
   Verfahren sehen einander nicht, und Daten- + Journal-Schreibzugriff sind
   kein atomares Paar. Sequenzielle Koexistenz (Original geschlossen während
   der Schreibzugriffe) ist sicher und getestet.
5. **Toter Parameter:** `read_dbf(filepath, encoding_hint="utf-16-le")` in
   `sp5lib/dbf_reader.py:179` — `encoding_hint` wird nie ausgewertet.
6. **Memo-Felder (Typ `M`) werden nicht unterstützt** (`dbf_reader.py:167`,
   `dbf_writer.py:208`): `.DBT`-Dateien werden weder gelesen noch geschrieben;
   Werte sind immer `None`/Spaces. Für Tabellen mit Memo-Spalten ist das ein
   stiller Datenverlust-Pfad beim Anzeigen (nicht beim Schreiben — der Writer
   lässt sie leer).
7. **POSIX-only-Schreibpfad:** `sp5lib/dbf_writer.py` verwendet `fcntl.flock`
   — unter Windows nicht importierbar. Da die Originaldaten von einer
   Windows-Anwendung stammen, ist die Bibliothek serverseitig faktisch auf
   Linux/macOS festgelegt (nirgends dokumentiert; pyproject behauptet
   "OS Independent").
8. **Stale Pfad-Defaults aus der Monorepo-Zeit:** `sp5lib/db_factory.py:52`
   und `sp5lib/auto_migrate.py:389` fallen ohne `SP5_DB_PATH` auf
   `<paket>/../../../sp5_db/Daten` zurück — relativ zu `site-packages` zeigt
   das ins Leere. Funktioniert nur im historischen In-Tree-Layout.
9. **`sp5lib/__init__.py` ist leer** (eine Kommentarzeile): kein
   `__version__`, keine Re-Exporte. Die Version steht nur in `pyproject.toml`;
   Konsumenten können sie zur Laufzeit nur über
   `importlib.metadata.version("libopenschichtplaner5")` ermitteln.
10. **Doku-/Code-Inkonsistenzen in `sp5lib/orm/base.py`:** Docstring verspricht
    "scoped sessions for multi-threaded WSGI/ASGI servers" (Z. 8), implementiert
    ist ein einfacher `sessionmaker`; `session_scope` existiert, fehlt aber im
    `__all__`/Import von `sp5lib/orm/__init__.py`.
11. **Drei parallele Persistenz-Ansätze:** DBF (`database.py`), Raw-SQLite-PoC
    (`sp5lib/sqlite_adapter.py`, nur EMPL/GROUP/BOOK, Kommentar "Proof of
    Concept") und das ORM. Der `sqlite_adapter` ist durch `sp5lib.orm`
    konzeptionell überholt, wird aber noch vom SQLite-Export der API verwendet
    (`openschichtplaner5-api/sp5api/routers/admin.py:434`) — Altlast, die erst
    nach API-Umbau entfernt werden kann.
12. **Privater Symbol-Export als faktische API:** Das API-Repo importiert
    `_GLOBAL_DBF_CACHE`, `_decode_string`, `_is_utf16_le`, `_parse_date`,
    `_encode_string`, `_read_header_info`. Refactorings an diesen "privaten"
    Helfern brechen den Konsumenten; sie sollten entweder öffentlich gemacht
    oder API-seitig ersetzt werden.
13. **Logger-Namensräume geerbt:** `"sp5api"` (`database.py:37`),
    `"sp5api.orm.sync"` (`orm/sync.py:46`), `"sp5api.db_factory"`,
    `"sp5api.sqlite_adapter"`, `"sp5api.pg"`, `"sp5.email"` — die
    Standalone-Bibliothek loggt überwiegend unter dem Namensraum der
    Host-App; uneinheitlich und irreführend.
14. **Scope-Creep der "DBF-Bibliothek":** `email_service.py` (SMTP),
    Tauschbörse-/Wunsch-/Template-Logik und JSON-Sidecar-Storage in
    `database.py` sind App-Fachlogik in der Datenzugriffsbibliothek. Die
    Sidecars koppeln die Lib zudem an das Verzeichnislayout der Host-App
    (`SP5_BACKEND_DIR`); Daten desselben Mandanten liegen verteilt über
    `db_path` (z. B. `wishes.json`, `ical_tokens.json`, `5USER_BCRYPT.json`)
    und `<backend>/data` (`changelog.json`, `swap_requests.json`,
    `schedule_comments.json`) — kein gemeinsames Locking/Backup mit den DBFs.
15. **`database.py` ist ein 7073-Zeilen-God-Object** mit 171 öffentlichen
    Methoden; die Berechnungslogik ist seit 1.7.0 nach
    `sp5lib.calculations` ausgelagert, aber Persistenz, Fachlogik und
    Formatierung (HEX-Farben, Anzeige-Namen) bleiben vermischt. Funktional in
    Ordnung, aber das größte Wartbarkeits-Risiko des Repos.
16. **CI/Metadata-Drift:** CI testet Python 3.10–3.12
    (`.github/workflows/ci.yml:15`), die Classifier in `pyproject.toml:23`
    versprechen zusätzlich 3.13.
17. **Legacy-Krypto bewusst erhalten:** Das 16-Byte-MD5-`DIGEST`-Feld in
    `5USER.DBF` bleibt Schreibziel (`database.py:762`, `pg_database.py:995`) —
    nötig für Kompatibilität mit dem Original-Client, aber MD5-Hashes der
    Passwörter bleiben dadurch dauerhaft auf Platte (bcrypt-Sidecar bzw.
    `bcrypt_hash`-Spalte mildern das nur für die Web-Seite).
18. **Multi-Tenant-Vorgriff ohne Backend:** `Company`-Modell und
    `company_id`-Spalten in `orm/models.py:31/122/206` haben kein
    DBF-Gegenstück und keinen Sync — spekulative Struktur, die derzeit nur vom
    API-Companies-Router (PG-seitig) genutzt werden kann.
19. **`auto_migrate`-Totgewicht:** `APP_SCHEMA_VERSION = "head"`
    (`auto_migrate.py:30`) wird nirgends ausgewertet; die
    DBF-Extension-Registry `_DBF_EXTENSIONS` (Z. 225) ist leer — der DBF-Zweig
    stempelt effektiv nur `.sp5_schema_version`-Dateien.

## 6. Konventionen

**Spec-Referenzen im Code:** Kommentare und Docstrings (vor allem in
`dbf_reader.py`, `dbf_writer.py`, `calculations.py`, `database.py` und den
Tests) referenzieren Kapitel und Regeln der **projektinternen
Verhaltensspezifikation des Original-Dateiformats** — Schreibweisen wie
„Spec 3.4“ (Kapitelnummer) oder „D-41“ (durchnummerierte Einzelregel,
„D-nn“). Diese Spezifikation ist die interne Format- und Verhaltensreferenz
des Projekts: Sie beschreibt, wie die originale Schichtplaner5-Anwendung
ihre Dateien aufbaut und ihre Werte berechnet, und dient als Grundlage für
Interop- und Paritätsentscheidungen. Sie ist kein Bestandteil dieses Repos;
die Nummern sind stabil, sodass sich Code-Stellen eindeutig einer Regel
zuordnen lassen.
