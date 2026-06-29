# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Zeitkonto-Jahresübersicht (`get_zeitkonto`) deutlich schneller bei echter
  Datenmenge.** Die Übersicht baute die Berechnungs-Eingaben (`_calc_inputs`:
  Bewegungsdaten 5MASHI/5SPSHI/5ABSEN/5BOOK/5OVER, Feiertage, Schicht-/
  Abwesenheitsdefinitionen, Zyklus-Expansion) bisher **pro Mitarbeiter neu** —
  bei N Mitarbeitern also N volle Tabellen-Scans je Bewegungstabelle (O(N×Tabelle)).
  Jetzt werden die Eingaben **einmal für alle Mitarbeiter** gebaut und je MA nur
  noch zerschnitten (neuer Helfer `_time_balance_from_inputs`, den auch die
  Einzelberechnung `calculate_time_balance` nutzt). Messung an 30 MA / 15 330
  5MASHI-Sätzen (2 Jahre): **106,6 → 49,3 ms je Aufruf (~2,2×)**; der Vorteil
  wächst mit der Mitarbeiterzahl. **Verhaltenswahrend:** Ausgabe byte-identisch
  über beide Jahre, Gruppenfilter und alle 30 Einzel-Zeitkonten (0 Abweichungen
  gegen die alte Logik); DBF- und Postgres-Backend bleiben äquivalent
  (`test_pg_calculations`); neuer Invariant-Test
  `test_zeitkonto_matches_per_employee_balance` (Revert→rot bestätigt).

## [1.23.0] - 2026-06-29

### Fixed

- **Benutzerrollen/-rechte aus echten 5USER-Daten korrekt aufgelöst.** `5USER.RIGHTS`
  ist KEIN Boolean, sondern der Berechtigungs-**Modus** des Originals (`SP5Data.dll`
  FUN_10043890/FUN_100435a0): `0` = volle Lese-/Schreibrechte (Planer), `1` =
  Nur-Leserechte (Leser), `2` = differenzierte Rechte pro Gruppe/MA (mind. Planer),
  `3` = wie 1 (read-only). `_role_from_record` prüfte invertiert `RIGHTS==1 → Planer`
  und stufte dadurch echte Planer (RIGHTS 0/2) fälschlich als „Leser" ein. Jetzt:
  `RIGHTS∈{1,3} → Leser`, sonst (0/2, nicht-Admin) → `Planer`, `ADMIN=1 → Admin`.
- **Durchsetzung an den Modus gekoppelt (nicht nur Anzeige):** Im Nur-Lesemodus
  (RIGHTS 1/3) liefern `_build_user_dict`/`get_user_permissions` jetzt **alle**
  Schreibrechte als `False` — auch wenn die gespeicherten W*-Flags (Altbestand echter
  DBs) noch gesetzt sind. Bisher hätte osp5 so einem read-only-Konto fälschlich
  Schreibzugriff erlaubt. Anzeige-/Sichtbarkeitsflags (SHOW*) bleiben erhalten.
- **Schreibkonvention konsistent:** `create_user`/`update_user` schreiben den Modus
  jetzt passend (`Planer → RIGHTS=0`, `Leser → RIGHTS=1`); vorher wurde invertiert
  `RIGHTS=1` für „Planer" geschrieben, im Widerspruch zur Original-Bedeutung.
  Regressionstests in `tests/test_user_permissions.py`.

## [1.22.1] - 2026-06-29

### Fixed

- **Datenintegrität: nebenläufige Schreibvorgänge vergaben doppelte IDs** und konnten
  dadurch unter Last Datensätze „vertauschen". Die ID neuer Sätze wurde als
  `max(ID)+1` aus einem Lesevorgang berechnet, der NICHT unter demselben Lock lief
  wie das anschließende Append. Bedient FastAPI mehrere synchrone Schreib-Requests
  parallel aus seinem Threadpool, lasen mehrere denselben `max` und schrieben
  dieselbe ID. Zwei Sätze mit gleicher ID ließen `find_all_records(ID=…)` mehrere
  Treffer liefern, sodass ein anschließendes ID-adressiertes Update/Delete den
  falschen (fremden) Satz traf. `append_record(…, autoid_field=…)` vergibt die ID
  jetzt atomar INNERHALB des exklusiven Append-Locks (nur das ID-Feld wird gescannt,
  also auch auf großen Tabellen günstig); alle ID-vergebenden Schreibwege der Fassade
  sowie die `NUMBER`-Vergabe des Änderungsjournals nutzen das. Damit kann kein
  Schreibvorgang mehr einen fremden Datensatz verändern (Round-Trip-/Byte-Paritäts-
  und Nebenläufigkeits-Regressionstests in `tests/test_concurrent_write_ids.py`).

## [1.22.0] - 2026-06-29

### Added

- `add_spshi_entry(noextra=…)` und `update_spshi_entry` (Schlüssel `NOEXTRA`) setzen
  jetzt das 5SPSHI-Flag `NOEXTRA` („keine Arbeitszeitzuschläge berechnen", Spec 3.8.3
  Nr. 13). Es wirkt für freie Sonderdienste ohne Schicht-Referenz; bei gesetzter
  `SHIFTID` gilt weiterhin das `NOEXTRA` der Schicht. `get_spshi_entries_for_day`
  liefert das Flag als `noextra` (bool) mit. Bisher war es beim Anlegen hart `0`.

## [1.21.0] - 2026-06-29

### Added

- `update_holiday_ban(ban_id, data)` ändert eine Urlaubssperre in `5HOBAN.DBF`
  feldweise. Nur die übergebenen Schlüssel (`group_id`, `start_date`, `end_date`,
  `reason`) werden geschrieben; nicht angegebene Felder bleiben unverändert. Gibt
  den aktualisierten Satz als dict zurück bzw. `None`, wenn keine Sperre diese ID
  hat. Damit lässt sich eine Urlaubssperre bearbeiten, statt sie löschen und neu
  anlegen zu müssen.

## [1.20.0] - 2026-06-28

### Added

- `update_period(period_id, data)` ändert einen gekennzeichneten Zeitraum in
  `5PERIO.DBF` feldweise. Nur die übergebenen Schlüssel (`group_id`, `start`,
  `end`, `color` als BGR-Int, `description`) werden geschrieben; nicht angegebene
  Felder bleiben unverändert. Gibt den aktualisierten Satz als dict zurück bzw.
  `None`, wenn kein Zeitraum diese ID hat. Damit lässt sich ein Zeitraum
  bearbeiten, statt ihn löschen und neu anlegen zu müssen.

## [1.19.0] - 2026-06-28

### Added

- `update_booking(booking_id, …)` ändert eine bestehende manuelle Kontobuchung in
  `5BOOK.DBF`. Nur die übergebenen Felder (`date_str`, `booking_type`, `value`,
  `note`) werden geschrieben; nicht angegebene Felder bleiben unverändert. Gibt den
  aktualisierten Satz als dict zurück bzw. `None`, wenn keine Buchung diese ID hat.
  Damit lässt sich eine Buchung bearbeiten, statt sie löschen und neu anlegen zu
  müssen.

## [1.18.0] - 2026-06-28

### Added

- `assign_cycle` akzeptiert ein optionales `end_date` und schreibt es nach
  `5CYASS.END`. Damit lässt sich eine Schichtmodell-Zuordnung befristen: die
  Zyklus-Expansion erzeugt nur bis einschließlich dieses Datums Dienste (das
  Expandieren berücksichtigte `END` bereits). Ohne Ende bleibt die Zuordnung offen.

## [1.17.0] - 2026-06-28

### Added

- `create_shift`/`update_shift` und `create_leave_type`/`update_leave_type`
  schreiben jetzt das Fettschrift-Flag `BOLD` (5SHIFT.BOLD / 5LEAVT.BOLD). Das
  Feld ist im Original vorhanden, wurde aber nie gesetzt. Beim Update wird ein
  explizit übergebenes `BOLD=0` durchgereicht (Flag bleibt abschaltbar). GET
  liefert das Feld bereits aus dem Rohsatz.

## [1.16.0] - 2026-06-28

### Added

- `create_user`/`update_user`: einzelne 5USER-Schreibrechte (WDUTIES, WABSENCES,
  WOVERTIMES, WNOTES, WDEVIATION, WCYCLEASS, WSWAPONLY, WPAST, ADDEMPL, BACKUP)
  sind nun explizit setzbar und überschreiben die rollenbasierten Defaults. Wird
  ein Flag im Daten-Dict übergeben, hat es Vorrang vor dem aus der Rolle
  abgeleiteten Standard (Spec 9.6). Anzeige-/Sichtbarkeitsflags (SHOW*) bleiben
  davon unberührt (`_WRITE_PERMISSION_FIELDS`).

## [1.15.0] - 2026-06-28

### Added

- `get_user_identity(user_id)`: liefert das Identitäts-/Rechte-Dict eines Benutzers
  per ID im selben Shape wie ein erfolgreicher Login (`_build_user_dict`), aber ohne
  Passwort/Digest zu prüfen. Grundlage für die Admin-Impersonation („Als Benutzer
  ansehen") in der API: der Ziel-User wird als Autorisierungs-Principal übernommen,
  sodass dessen Rolle/Rechte/Sichtbarkeit gelten. Versteckte (HIDE) und unbekannte
  Benutzer ⇒ `None`. Der Login-/Digest-Pfad bleibt unberührt.

## [1.14.4] - 2026-06-28

### Fixed

- Konflikterkennung meldet keine Falsch-Konflikte mehr bei normaler Soll-/Ist-Abweichung.
  `get_schedule_conflicts` bezog Sollplan-Einträge (5MASHI.TYPE=1, Spec 4.12/D-58) mit ein,
  sodass eine geplante Soll-Schicht, die mit einer Ist-Abwesenheit (z. B. Krankenstand) am
  selben Tag zusammentraf, fälschlich als „Schicht + Abwesenheit"-Konflikt erschien — das ist
  aber die normale Soll-/Ist-Zwei-Ebenen-Ansicht (das Original-Schichtplaner5 kennt zwischen
  den Ebenen gar keine Konfliktprüfung). Konflikte werden jetzt ausschließlich auf der
  Ist-Ebene ermittelt (Sollplan-Ziele ausgeschlossen). Echte Konflikte bleiben erhalten: ein
  tatsächlicher Ist-Dienst neben einer Abwesenheit (oder ein Feiertags-/Über-10-h-Dienst)
  wird weiterhin gemeldet.

## [1.14.3] - 2026-06-28

### Changed

- DBF-Lese-Cache invalidiert jetzt inhaltsbasiert statt nur über die mtime. Auf
  Deployments, deren DBF-Verzeichnis periodisch gespiegelt/neu synchronisiert wird
  (z. B. ein 15-Minuten-Mirror auf einem Bind-Mount), änderte bisher jeder Sync die
  mtime und erzwang ein vollständiges Neu-Parsen ALLER Tabellen beim nächsten Zugriff
  — auch wenn sich inhaltlich nichts geändert hatte. Der Cache prüft bei geänderter
  mtime nun den Inhalts-Hash der Datei (einmalig gelesen, derselbe Puffer wird ggf.
  geparst) und behält bei unverändertem Inhalt den vorhandenen Parse. Gemessen sinkt
  die Strafe eines No-op-Syncs von ~66 ms auf ~1,5 ms (15 000-Datensatz-Tabelle, ~36×
  → ~2× gegenüber warm). Der Monatsindex ist an denselben Inhalts-Hash gekoppelt. Die
  DBF-Dateien bleiben alleinige Quelle der Wahrheit; Schreibzugriffe gehen unverändert
  sofort byte-genau auf die DBF (write-through), der Cache ist reine Lese-Schicht.

## [1.14.2] - 2026-06-28

### Changed

- DBF-Parser beschleunigt: Die Feld-Spezifikationen (Typ, Länge, Offset,
  Binärfeld-Flag) werden je Tabelle nur noch einmal berechnet statt für jeden
  Datensatz erneut. Das Einlesen großer Tabellen wird dadurch spürbar schneller
  (~24 % weniger Parse-Zeit bei einer 15 000-Datensatz-Tabelle gemessen), was
  vor allem das erste Laden des Dienstplans und das Neuladen nach Schreibzugriffen
  betrifft. Die dekodierten Werte sind unverändert byte-identisch (über alle
  Feldtypen verifiziert); rein interne Optimierung, keine API-Änderung.
- `get_schedule` liest die Schicht-Zuordnungen (5MASHI) jetzt über einen nach
  Monat gruppierten Index statt die gesamte Tabelle pro Aufruf zu durchsuchen.
  Der Index ist eine reine Lese-Schicht über dem mtime-DBF-Cache und wird bei
  jedem Schreibzugriff konsistent invalidiert; die Ausgabe ist unverändert (über
  24 Monate × 3 Plan-Sichten, 30 660 Einträge byte-identisch verifiziert). Warm
  ~37 % schneller (2,42 → 1,53 ms je Monatsabruf bei 15 000 Datensätzen); der
  Vorteil wächst mit der Tabellengröße.

## [1.14.1] - 2026-06-28

### Fixed

- Login mit Original-5USER-Konten, deren Passwort eine andere Byte-Kodierung als
  reines ASCII nutzt: Schichtplaner5 (Delphi/Windows) prüft das Passwort gegen den
  gespeicherten MD5-Digest unter zwei Kodierungen — Delphi-WideString (UTF-16-LE)
  und System-ANSI (CP1252) — während diese Bibliothek nur `MD5(utf-8)` verglich.
  Dadurch konnten sich Konten mit UTF-16-LE-Digest (z. B. das Beispielkonto
  „Leitung") oder mit Umlaut-Passwörtern (CP1252) nie anmelden, ASCII-Passwörter
  dagegen schon (utf-8 == cp1252). `verify_user_password` probiert die Kodierungen
  jetzt durch (`utf-8`, `cp1252`, `utf-16-le`) und akzeptiert bei jedem Treffer; ein
  erfolgreicher Legacy-Login wird wie bisher auf bcrypt migriert. Falsche Passwörter
  werden weiterhin abgewiesen.

### Added

- `login_diagnostics` meldet zusätzlich `digest_all_zero` (kein Passwort gesetzt /
  Konto deaktiviert), `digest_is_empty_md5` (leeres Passwort — Original-Parität) und
  `encodings_tried`; bei erfolgreichem Legacy-Login nennt das Migrations-Log die
  zutreffende Kodierung (`MD5[utf-16-le] → bcrypt`). Privacy-safe — nie das Passwort
  oder den rohen Digest.

## [1.14.0] - 2026-06-28

### Added

- `get_schedule(...)` liefert für Abwesenheiten jetzt `interval`, `start_time` und
  `end_time` mit (Teiltage, Spec 3.5.2/D-54: 0=ganz, 1=vorm., 2=nachm., 3=stundenweise
  mit Minuten ab Mitternacht) — DBF- und PostgreSQL-Backend gleichermaßen. Damit kann
  der Dienstplan Teiltags-Abwesenheiten erkennen und beim Wiederherstellen/Verschieben
  einer Zelle die Granularität erhalten (A10), statt sie als ganztägig zurückzugeben.

## [1.13.0] - 2026-06-16

### Added

- `SP5Database.extracharge_hours_by_day(...)`: Zeitzuschläge je Tag (Spec 3.8) —
  je (Mitarbeiter, Tag, Zuschlag) eine Zeile mit Stunden > 0, über dieselbe
  Berechnungsschicht wie `calculate_extracharge_hours` (Tageswechsel-Split,
  Fensterschnitt, NOEXTRA, expandierte 5CYASS). Zeitraum als Monat oder freier
  Bereich; die Summe der Tageszeilen je Regel entspricht dem aggregierten Wert.

## [1.12.2] - 2026-06-16

### Fixed

- Release-Workflow: `sigstore/cosign-installer` auf eine existierende Version
  gepinnt (`@v4.1.2`) — der zuvor referenzierte bewegliche `@v4`-Tag existiert
  nicht, wodurch der (optionale, standardmäßig inaktive) Docker-Job bereits in
  der Job-Vorbereitung scheiterte. Damit veröffentlicht ein Tag wieder
  vollständig nach PyPI, ghcr und als GitHub-Release.

## [1.12.1] - 2026-06-16

### Changed

- Release-Automatik vervollständigt: ein Versions-Tag veröffentlicht jetzt
  zusätzlich zum PyPI-Paket automatisch das `sp5lib`-CLI-Image nach
  `ghcr.io/mschabhuettl/libopenschichtplaner5` (multi-arch amd64+arm64, Tags
  volle Version / Minor / `latest`) und legt ein GitHub-Release mit dem
  Changelog-Auszug sowie wheel, sdist und einem SPDX-SBOM als Assets an.
  Release-Assets und Image tragen eine Build-Provenance-Attestation; je Image
  wird ein SBOM erzeugt und attestiert. Optionale cosign-Signierung über die
  Repo-Variable `ENABLE_COSIGN`.

## [1.12.0] - 2026-06-16

### Added

- `SP5Database.login_diagnostics(name)`: datenschutzsichere Diagnose zu einem
  *fehlgeschlagenen* Login (Benutzer vorhanden ja/nein, gespeichertes Digest-Format,
  bcrypt-Sidecar vorhanden) — berührt oder protokolliert nie das Passwort. Damit
  lässt sich ein Real-DB-Login-Sonderfall aus den Server-Logs erklären.

## [1.11.0] - 2026-06-12

### Added

- `SP5Database.reorder(entity, ordered_ids)`: manuelle, programmweite Stammdaten-
  Sortierung (Spec 5.1 Nr. 4) — vergibt POSITION 1..N in der gegebenen Reihenfolge
  für `employees/shifts/groups/leave_types/workplaces`.

## [1.10.0] - 2026-06-12

### Added

- Einschränkungs-Grad (5RESTR.RESTRICT, Spec 4.11, Dekompilat-belegt):
  `set_restriction(grade=…)` schreibt 0=keine / 1=„auf Anfrage" / 2=„nie"
  (Vorgabe 2) und aktualisiert den Grad eines bestehenden Satzes.
- Optionaler CDX-Index-Schreiber (ROADMAP §B.2): `SP5_CDX_WRITE=1` (Default aus)
  baut die FoxPro-Compound-Index-`.CDX` nach jedem Schreibzugriff byte-genau neu
  auf, statt sie zu verwerfen — das Original öffnet die Tabelle dann ohne
  Index-Neuaufbau. Default bleibt die bewährte Invalidierung; bei unbekannter
  Schlüsselform fällt der Schreiber sicher auf Löschen zurück. 60/60 Beispiel-CDX
  byte-identisch reproduziert (einzige Ausnahme dokumentiert: der CodeBase-interne
  Key-Op-Zähler in Header-Offset 0x08 wird beim In-place-Rebuild erhalten).
- Differenzierte Sichtbarkeit (Spec 9.5.3): `get_user_visible_employee_ids` /
  `get_user_visible_group_ids` leiten aus 5GRACC (inkl. Untergruppen-Vererbung
  über SUPERID) und 5EMACC die für einen Benutzer sichtbaren Mitarbeiter/Gruppen
  ab (`None` = unbeschränkt). Der subtraktive 5EMACC-„kein Zugriff"-Override ist
  als unsicher dokumentiert (5EMACC in der Referenz-DB leer) und wirkt additiv.
- Arbeitsplatz im Dienstplan (Spec 6.4): `add_schedule_entry(workplace_id=…)` und
  `set_schedule_workplace(...)` setzen 5MASHI.WORKPLACID; `get_schedule` reichert
  `workplace_name` aus 5WOPL an.
- Soll-/Istplan (Spec 4.12, D-58): `get_schedule(plan=…)` filtert reguläre
  Dienste nach 5MASHI.TYPE — `ist` (Vorgabe, schedule_type≠1), `soll` (==1) oder
  `both`; jeder Dienst trägt `schedule_type`. `add_schedule_entry(schedule_type=…)`
  schreibt TYPE; Soll- und Ist-Eintrag dürfen am selben Tag koexistieren.
  TYPE-Kodierung (0=Ist, 1=Soll) aus dem Dekompilat belegt; 5SPSHI.TYPE bleibt
  davon getrennt (Sonderdienst vs. Arbeitszeitabweichung, D-53).
- `SP5Database.apply_absence_visibility(data, mode)`: wendet die dreiwertige
  SHOWABS-Sichtbarkeit (Spec 9.5.2 Nr. 2.1, 9.2 Nr. 3, D-67) auf beliebig
  verschachtelte Plan-Strukturen an — 0 = vollständig, 1 = anonymisiert
  (5USETT-ANOA*-Ersatzdarstellung), 2 = Abwesenheiten ausgeblendet.
- Benutzer-Dict (`_build_user_dict`) trägt jetzt `SHOWABS_MODE` (Rohwert 0/1/2);
  `update_user`/`get_users` lesen/schreiben den dreiwertigen SHOWABS.

### Fixed

- `SHOWABS` wird nicht mehr fälschlich als Wahrheitswert mit invertierter
  Polarität behandelt: Die Berechtigung „darf Abwesenheiten sehen" ist jetzt
  korrekt `mode != 2` (0 = vollständig **und** 1 = anonymisiert sind sichtbar).

## [1.9.0] - 2026-06-12

### Added

- `SP5Database.eligible_replacements` and `calculations.is_eligible_replacement`:
  replacement candidates filtered by group membership, employment period,
  availability (not already assigned / not absent) and shift restriction.
- Round-trip write tests covering every facade write path (movement and master
  data), verifying value, change journal and index invalidation.
- `prepare-release` workflow (manual dispatch): bumps the version
  (patch/minor/major or explicit), cuts the `[Unreleased]` changelog section
  into a release section, updates the compare links and pushes commit +
  annotated tag — the tag keeps driving the PyPI publish. Dry-run mode
  (default) only reports the planned changes in the step summary. The workflow
  refuses to release when the `[Unreleased]` section is missing or empty.
- `RELEASING.md` documents the release flow.
- Optional oracle test (`tests/test_oracle_calculations.py`, gated by
  `SP5_GOLDEN_DB`): cross-checks `calculations.get_nominal_hours` against the
  values the original program displays for the sample database.

### Fixed

- Shift restrictions now use the original day index (0=Mon..6=Sun, 7=holiday)
  instead of "0=all, 1=Mon..7=Sun (ISO)" in the auto-scheduler.

## [1.8.0] - 2026-06-12

### Added

- **`sp5lib`-CLI** (`sp5lib.cli`, console script): standalone tools for a
  Schichtplaner5 database directory — `info` (records per table, SP5 build),
  `dump` (table as JSON/CSV, `--limit`), `validate` (reads all tables, reports
  errors and encoding issues via exit code) and `sync` (DBF → SQLite/PostgreSQL
  via `sp5lib.orm.sync`).
- Dockerfile default stage `cli`: slim non-root image with
  `ENTRYPOINT ["sp5lib"]`; compose service `tools` for ad-hoc CLI runs.

## [1.7.0] - 2026-06-11

The calculation-layer release: a central `sp5lib.calculations` module implements
the original's computation rules, the `SP5Database` facade is rewired onto it,
writes are interoperable with a running original client (change journal + CDX
strategy + byte parity), a golden regression suite runs against the original
sample database, and the PostgreSQL backend reaches calculation parity with the
DBF backend.

### Added

- **`sp5lib.calculations`** — central, side-effect-free calculation layer
  implementing the original's computation rules: nominal/actual hours with
  the CALCBASE dispatcher (day/week/month/total basis), day-index-correct shift
  durations (`DURATION0..7`, holiday = index 7), 5SPSHI replacement, expanded
  rotation cycles (5CYASS), absence crediting (CHARGETYP/CHARGEHRS/COUNTALL,
  INTERVAL half days), account bookings, leave accounts and forfeiture,
  surcharge windows (window intersection instead of DURATION), personnel table
  and demand/utilization.
- `SP5Database` evaluation facades wired to the calculation layer:
  `get_statistics` (month or free evaluation period),
  `get_personnel_table`, `get_utilization`, `forfeit_rest`, leave balance per
  leave type, surcharges over a free period, expanded cycle duties in the
  schedule read path, and PACK via `compact_database` /
  `dbf_writer.pack_table`.
- **Write interop with a running original client** in `sp5lib.dbf_writer`:
  - *Change journal:* every write appends a matching entry to the `-L`
    companion table, with the composite keys the original format expects, so
    running original clients pick up external changes. A missing or corrupt
    `-L` file degrades to a warning and never blocks the data write.
  - *CDX strategy:* stale `.CDX` index files of a modified table are deleted
    after every successful write so the original rebuilds them instead of
    reusing indexes that no longer match the table
    (`INVALIDATE_CDX = False` opts out).
  - Files written by the library are byte-for-byte compatible with the
    original file format (header fields, encoding, EOF marker).
- **Golden regression suite** (`tests/test_golden_sample_db.py`): runs the
  reader against the original Schichtplaner 5 sample database when
  `SP5_GOLDEN_DB=/path/to/Daten` is set (entire module skips otherwise). The
  reference DB stays local and is never committed; only non-personal master
  data is asserted.
- **PostgreSQL backend calculation parity**: `SP5PostgresDatabase` calls the
  same `sp5lib.calculations` functions as `SP5Database` — `get_statistics`
  (incl. free period), `get_personnel_table`, `get_utilization`,
  `forfeit_rest`, `calculate_time_balance`, `get_zeitkonto`,
  `get_employee_stats_year`/`_month`, `get_schedule_year`,
  `calculate_extracharge_hours` (plus a read-only `get_extracharges` over the
  5XCHAR mirror) and `get_leave_balance`(`_group`). Where possible the methods
  are reused as the very same function objects over mirrored adapter helpers,
  and `tests/test_pg_calculations.py` drives both backends with identical
  fixture rows and asserts identical results. Annual close
  (`run_annual_close`/`get_annual_close_preview`) raises `NotImplementedError`
  on PG (the ORM mirror has no entitlement write facade yet). Documented
  bridges: `HRSMONTH > 0` is computed as monthly base (the PG schema has no
  CALCBASE column); cycle exceptions (5CYEXC) have no ORM mirror and are not
  applied on PG.
- Facade write features: part-day absences (INTERVAL/START/END), half holidays
  and repeat-years (incl. PG parity) in `create`/`update_holiday`, the annual
  close option `keep_entitlements`, granular 5USER permission flags, and
  NOEXTRA passthrough in `create`/`update_shift`.

### Fixed

- Central DBF read-cache invalidation after all own writes — no more stale
  reads after create/update/delete through the facade.
- `append_record` refuses a record-size mismatch instead of silently
  truncating the record.
- A corrupt `-L` journal no longer blocks the main data write.
- The UTF-16 heuristic in the reader now recognizes non-Latin scripts.
- The cycle generator uses the correct day index (`DURATION0` = Monday).

### Changed

- Orphaned legacy helpers `_count_working_days` and
  `_time_window_overlap_minutes` removed from `database.py` (replaced by the
  calculation layer).
- `pg_database.get_statistics` now returns the calculation-layer result shape
  (adds `group_name`/`group_id`/`sick_days`, accepts `date_from`/`date_to`)
  instead of the previous naive DURATION0 sums.

### Notes

- `sp5lib.sqlite_adapter` was audited for removal but is still used by the
  API's SQLite export endpoint (`/api/backup/sqlite`) — it stays.

## [1.6.0] - 2026-05-27

ORM Phase 6 — completes the read-only mirror with the demand, rotation-cycle
and restriction tables. Additive and backward compatible. `sync_all()` now
covers 19 tables.

### Added

- **`ShiftDemand`** (`staffing_requirements`, from `5SHDEM.DBF`),
  **`SpecialDemand`** (`special_demands`, `5SPDEM.DBF`), **`Cycle`** (`cycles`,
  `5CYCLE.DBF`), **`CycleAssignment`** (`cycle_assignments`, `5CYASS.DBF`) and
  **`Restriction`** (`restrictions`, `5RESTR.DBF`) ORM models, importable from
  `sp5lib.orm`, with `to_dict()` mirroring the real DBF keys. `init_db()`
  creates the tables.
- Repositories: **`ShiftDemandRepository`** `list(shift_id, weekday, group_id)`,
  **`SpecialDemandRepository`** `list(date_from, date_to, shift_id)`,
  **`CycleRepository`** `list(include_hidden=False)`,
  **`CycleAssignmentRepository`** `list(employee_id, cycle_id)`,
  **`RestrictionRepository`** `list(employee_id, shift_id)` — all with `get(id)`.
- DBF → ORM upsert `sync.sync_shift_demand`, `sync.sync_special_demand`,
  `sync.sync_cycles`, `sync.sync_cycle_assignments`, `sync.sync_restrictions`,
  wired into `sync.sync_all()`. `5SPDEM` rows with a blank/invalid `DATE` are
  skipped and logged. `sync_cycle_assignments` follows the 5GRASG pattern
  (autoincrement PK, de-dup on `(employee_id, cycle_id, start)`) since the DBF
  `ID` is not guaranteed unique.

### Changed

- `ShiftDemand` / `Cycle` / `CycleAssignment` / `Restriction` are defined
  canonically in `sp5lib.orm.models` and re-exported from `sp5lib.orm.models_pg`.
  The previous name **`StaffingRequirement`** (→ `ShiftDemand`) remains
  importable as an alias (same table `staffing_requirements`), so existing
  imports keep working.

### Notes

- DBF field mapping (verified against `database.py`): 5SHDEM/5SPDEM `MIN`/`MAX`;
  5RESTR free-text reason is the `RESERVED` field (`to_dict()` exposes it as
  `RESERVED`). 5CYCLE length is `SIZE`/`UNIT`.

## [1.5.0] - 2026-05-27

ORM Phase 5 — account bookings, overtime and leave entitlements (the data
behind the time-account / overtime / leave-balance features). Additive and
backward compatible.

### Added

- **`AccountBooking`** (`bookings_pg`, from `5BOOK.DBF`), **`OvertimeEntry`**
  (`overtime_records`, `5OVER.DBF`) and **`LeaveEntitlement`**
  (`leave_entitlements`, `5LEAEN.DBF`) ORM models, importable from
  `sp5lib.orm`, with `to_dict()` mirroring the real DBF keys. `init_db()`
  creates the tables.
- **`AccountBookingRepository`** and **`OvertimeEntryRepository`** with
  `list(date_from=None, date_to=None, employee_id=None)` + `get(id)`;
  **`LeaveEntitlementRepository`** with `list(year=None, employee_id=None)` +
  `get(id)`.
- DBF → ORM upsert `sync.sync_book`, `sync.sync_overtime`,
  `sync.sync_leave_entitlements`, wired into `sync.sync_all()`. `BOOK`/`OVER`
  rows with a blank/invalid `DATE` are skipped and logged. `sync_all()` now
  covers 14 tables.

### Changed

- `AccountBooking` / `OvertimeEntry` / `LeaveEntitlement` are defined
  canonically in `sp5lib.orm.models` and re-exported from
  `sp5lib.orm.models_pg`. The previous names **`Booking`** (→ `AccountBooking`)
  and **`OvertimeRecord`** (→ `OvertimeEntry`) remain importable as aliases
  (same tables `bookings_pg` / `overtime_records`), so existing imports keep
  working.

### Notes

- `LeaveEntitlement` maps the DBF fields `ENTITLEMNT` → `entitlement`,
  `REST` → `carry_forward`, `INDAYS` → `in_days`; `to_dict()` mirrors the DBF
  spellings (`ENTITLEMNT` / `REST` / `INDAYS`).

## [1.4.0] - 2026-05-27

ORM Phase 4 — reference tables (holidays, accounting periods) plus a sync
robustness fix that lets `sync_all()` run to completion on real data.

### Added

- **`Holiday`** (`holidays`, from `5HOLID.DBF`) and **`Period`** (`periods`,
  from `5PERIO.DBF`) ORM models, importable from `sp5lib.orm`, with `to_dict()`
  mirroring the real DBF keys. `init_db()` creates the tables.
- **`HolidayRepository`** with `list(year=None)` (a given year plus recurring
  `interval == 1` holidays) and `get(id)`.
- **`PeriodRepository`** with `list(date_from=None, date_to=None,
  group_id=None)` and `get(id)`.
- DBF → ORM upsert `sync.sync_holidays` and `sync.sync_periods`, wired into
  `sync.sync_all()`.

### Fixed

- `sync.sync_group_assignments` no longer aborts `sync_all` with
  `UNIQUE constraint failed: group_assignments.id`. The `ID` column in
  `5GRASG.DBF` is a per-group running index, not a global key, so it is no
  longer used as the primary key (the autoincrement `id` is). Assignments are
  de-duplicated on `(employee_id, group_id)`, and rows referencing a
  non-existent employee or group are skipped and logged. `sync_all()` now
  completes over the full set of tables.

### Changed

- `Holiday` is now defined canonically in `sp5lib.orm.models` and re-exported
  from `sp5lib.orm.models_pg` (used by `pg_database`); `Period` is likewise
  available from both. No behaviour change for existing imports.

### Notes

- `Period` maps the DBF `DESCRIPT` field (the period label) — the request
  referred to it as `NAME`, but the actual `5PERIO.DBF` field is `DESCRIPT`,
  which `to_dict()` mirrors (alongside `GROUPID` / `START` / `END` / `COLOR`).

## [1.3.0] - 2026-05-27

ORM Phase 3 — the time-based roster. Adds the schedule-entry tables to the
SQLAlchemy layer plus a sync robustness fix. Additive and backward compatible.

### Added

- **`ShiftAssignment`** (`schedule_entries`, from `5MASHI.DBF`),
  **`SpecialShift`** (`special_shifts`, `5SPSHI.DBF`) and **`Absence`**
  (`absences`, `5ABSEN.DBF`) ORM models, importable from `sp5lib.orm`, each with
  a `to_dict()` mirroring the DBF keys (`DATE` / `EMPLOYEEID` / `SHIFTID` /
  `LEAVETYPID` / …). `init_db()` creates the tables.
- **`ShiftAssignmentRepository`**, **`SpecialShiftRepository`**,
  **`AbsenceRepository`** with `list(date_from=None, date_to=None,
  employee_id=None)` (date-window + per-employee filtering) and `get(id)`.
- DBF → ORM upsert `sync.sync_shift_assignments`, `sync.sync_special_shifts`,
  `sync.sync_absences`, wired into `sync.sync_all()`. Blank/invalid `DATE`
  values are skipped and logged; references (employee/shift/leave-type) are
  plain indexed integers with no DB-level FK, so dirty legacy data syncs.

### Fixed

- `sync.sync_groups` no longer aborts `sync_all` with
  `FOREIGN KEY constraint failed` when `5GROUP.DBF` contains a `super_id` that
  points to a non-existent group. Dangling parent references are now resolved
  in a second pass: unknown references are set to `NULL` and logged. This also
  makes group ordering in the DBF irrelevant.

### Changed

- `ShiftAssignment`, `SpecialShift` and `Absence` are defined canonically in
  `sp5lib.orm.models` and re-exported from `sp5lib.orm.models_pg`. The former
  `ScheduleEntry` name (MASHI) remains importable as an alias of
  `ShiftAssignment`, so existing
  `from sp5lib.orm.models_pg import ScheduleEntry, SpecialShift, Absence`
  imports (used by `pg_database`) keep working unchanged.

## [1.2.0] - 2026-05-27

ORM Phase 2 — adds the next three core entities to the SQLAlchemy layer
(`sp5lib.orm`), mirroring the Phase 1 Employee/Group patterns. Additive and
backward compatible.

### Added

- **`Shift`** (`shifts`, from `5SHIFT.DBF`), **`LeaveType`** (`leave_types`,
  `5LEAVT.DBF`) and **`Workplace`** (`workplaces`, `5WOPL.DBF`) ORM models,
  importable directly from `sp5lib.orm`. `init_db()` creates the tables.
- **`ShiftRepository`**, **`LeaveTypeRepository`**, **`WorkplaceRepository`**
  with `list(include_hidden=False)` and `get(id)`.
- DBF → ORM upsert for the three tables via `sync.sync_shifts`,
  `sync.sync_leave_types`, `sync.sync_workplaces`; `sync.sync_all()` now also
  returns `shifts` / `leave_types` / `workplaces` counts.
- ORM unit tests (`tests/test_orm.py`, in-memory SQLite) covering models,
  repositories, `to_dict()` and the sync upsert.

### Changed

- `Shift`, `LeaveType` and `Workplace` are now defined canonically in
  `sp5lib.orm.models` and **re-exported** from `sp5lib.orm.models_pg` (single
  source of truth, identical `to_dict()`). Existing
  `from sp5lib.orm.models_pg import Shift, LeaveType, Workplace` imports keep
  working unchanged.

## [1.1.0] - 2026-05-26

Initial standalone release of **libopenschichtplaner5** (import name `sp5lib`).

This is the first release of the library as an independent, pip-installable
package. The code was extracted — **with its full git history** — from the
`backend/sp5lib/` directory of
[OpenSchichtplaner5](https://github.com/mschabhuettl/openschichtplaner5).
The `1.1.0` version preserves the version line the library carried inside that
project, so there is no regression for existing consumers; OpenSchichtplaner5
continues to import it unchanged as `sp5lib`.

### Added

- Packaging as the `libopenschichtplaner5` distribution (importable as `sp5lib`),
  publishable to PyPI with an sdist and a pure-Python wheel.
- `sp5lib.dbf_reader` — pure-Python DBF reader (UTF-16-LE detection, date
  parsing, field decoding) for the original Schichtplaner5 FoxPro/dBASE files.
- `sp5lib.dbf_writer` — safe DBF writer with exclusive `flock`, TOCTOU-safe
  record counting, rollback, and EOF-marker preservation.
- `sp5lib.database` — high-level `SP5Database` facade over the DBF tables
  (employees, shifts, schedule, absences, authentication, 2FA, …).
- `sp5lib.db_factory`, `sp5lib.sqlite_adapter`, `sp5lib.pg_database` — optional
  SQLite and PostgreSQL backends.
- `sp5lib.orm` — SQLAlchemy models (SQLite `models.py`, PostgreSQL
  `models_pg.py`), `repository`, and `sync`.
- `sp5lib.auto_migrate` — Alembic-based automatic migrations.
- `sp5lib.email_service` — SMTP notification emails with HTML-escaped templates.
- `sp5lib.color_utils` — FoxPro BGR ↔ hex/RGB color helpers.
- `py.typed` marker so type checkers consume the bundled type hints.
- `postgres` extra (`psycopg2-binary`) for the optional PostgreSQL backend.
- Continuous integration running ruff and pytest on Python 3.10–3.12.
- Release workflow publishing to PyPI via Trusted Publishing on `v*` tags.

### Notes

- Runtime dependencies: `SQLAlchemy`, `alembic`, `bcrypt`, `pyotp`, `packaging`.
- Requires Python 3.10 or newer.
- Licensed under the MIT License.

[Unreleased]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.23.0...HEAD
[1.23.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.22.1...v1.23.0
[1.22.1]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.22.0...v1.22.1
[1.22.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.21.0...v1.22.0
[1.21.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.20.0...v1.21.0
[1.20.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.19.0...v1.20.0
[1.19.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.18.0...v1.19.0
[1.18.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.17.0...v1.18.0
[1.17.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.16.0...v1.17.0
[1.16.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.15.0...v1.16.0
[1.15.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.14.4...v1.15.0
[1.14.4]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.14.3...v1.14.4
[1.14.3]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.14.2...v1.14.3
[1.14.2]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.14.1...v1.14.2
[1.14.1]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.14.0...v1.14.1
[1.14.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.13.0...v1.14.0
[1.13.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.12.2...v1.13.0
[1.12.2]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.12.1...v1.12.2
[1.12.1]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.12.0...v1.12.1
[1.12.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.11.0...v1.12.0
[1.11.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.10.0...v1.11.0
[1.10.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.9.0...v1.10.0
[1.9.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.8.0...v1.9.0
[1.8.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/mschabhuettl/libopenschichtplaner5/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/mschabhuettl/libopenschichtplaner5/releases/tag/v1.1.0
