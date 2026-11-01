# Data dictionary

Grain, keys and nullability. All timestamps are ISO-8601 UTC strings (`YYYY-MM-DDTHH:MM:SSZ`); dates are `YYYY-MM-DD`.

## Source files

### Events CSV (`events*.csv`), grain: one version of one event per line
| column | type | null? | rule |
|---|---|---|---|
| event_id | text | no | business key; unique per *version* in target |
| source_system | text | no | producing system |
| event_ts | timestamp | no | ISO-8601 **with** offset or `Z`; its UTC date drives reference lookup |
| updated_at | timestamp | no | source version clock; newest wins |
| dataset_name | text | no | reference dataset (lower-cased) |
| code | text | no | code to validate (upper-cased, trimmed) |
| amount | decimal | no | ≥ 0, ≤ 2 decimals → stored as integer cents |

Header names are case-insensitive and order-independent. Extra columns are ignored with a `schema_drift` note; missing required columns fail the file.

### Reference CSV (`reference_v*.csv`), grain: one validity window of one code
`dataset_name, code, description, status(active|deprecated), effective_from, effective_to(blank=open, exclusive), replaced_by_code(only on deprecated; must exist)`.
Unique key `(dataset_name, code, effective_from)`; windows of a code must not overlap.

## Target tables (SQLite)

### pipeline_run, one row per load attempt (PK `run_id`)
`run_type` (reference_load|event_load), `source_file`, `input_sha256`, `status` (running|success|failed), `started_at`, `ended_at`, `duration_seconds`,
`rows_read`, `rows_blank_skipped`, `rows_valid`, `rows_valid_deprecated`, `rows_rejected`, `amount_cents_accepted`, `amount_cents_rejected`,
`reconciled` (1 when in-transaction assertions passed), `ref_versions_json`, `notes`, `failure_reason`, `stage_timings_json`.
Invariant: `rows_read = rows_valid + rows_valid_deprecated + rows_rejected`.

### ref_version, one row per dataset snapshot (PK `ref_version_id`, immutable)
`dataset_name`, `content_hash`, `row_count`, `source_file`, `run_id` (FK), `loaded_at`.

### ref_code, windows per version (PK `ref_version_id, code, effective_from`; FK → ref_version)
`dataset_name`, `code`, `description`, `status`, `effective_from`, `effective_to` (nullable), `replaced_by_code` (nullable).

### event_validated, current accepted state (PK `event_id`)
| column | notes |
|---|---|
| event_id, source_system, event_ts, updated_at, dataset_name, code | normalised source fields |
| effective_date | UTC date of event_ts (derived) |
| amount_cents | integer ≥ 0 (derived) |
| validation_status | `valid` or `valid_deprecated` (derived) |
| warning_code / replaced_by_code | `CODE_DEPRECATED` and suggested successor, else NULL |
| ref_version_id | FK; reference version used for the decision |
| record_hash | hash of stripped payload; drives idempotency |
| run_id (FK), source_file, source_line | lineage to the producing run/row |

### event_reject, quarantine (PK `source_file, source_line`)
`reject_id` (deterministic), `run_id` (FK), `event_id` (nullable: may be the missing field), `reason_code`, `reason_detail`, `ref_version_id` (nullable),
`record_hash`, `raw_record` (JSON array of the original fields), `amount_cents` (nullable if unparseable), `rejected_at`.
One row per rejected source row (exactly one reason).

### event_history, append-only outcomes (PK `event_id, record_hash, outcome, ref_version_id`)
`outcome` ∈ `valid | valid_deprecated | rejected:<REASON>`; `run_id`, `source_file`, `source_line`, `recorded_at` of first occurrence. `ref_version_id = 0` means "no reference involved".

### run_reject_summary (PK `run_id, reason_code`), per-run reject counts, retained forever.

## Reject reason codes
`MALFORMED_RECORD`, `MISSING_REQUIRED_FIELD`, `INVALID_TIMESTAMP`, `INVALID_AMOUNT`, `DUPLICATE_EVENT`, `SUPERSEDED_VERSION`, `STALE_VERSION`,
`UNKNOWN_REFERENCE_DATASET`, `UNKNOWN_CODE`, `CODE_NOT_YET_EFFECTIVE`, `CODE_EXPIRED`.
Run-level failure codes (in `pipeline_run.failure_reason`): `EMPTY_FILE`, `INPUT_NOT_FOUND`, `UNREADABLE_FILE`, `SCHEMA_MISSING_COLUMNS`, `SCHEMA_DUPLICATE_COLUMNS`,
`NO_REFERENCE_DATA`, `INVALID_PIN`, `REFERENCE_VALIDATION_FAILED`, `EMPTY_REFERENCE`, `RECONCILIATION_FAILED`, `UNEXPECTED_ERROR`.

## Referential integrity
`event_validated.ref_version_id → ref_version`, `ref_code.ref_version_id → ref_version`, `event_*.run_id → pipeline_run`, `ref_version.run_id → pipeline_run`.
Enforced by `PRAGMA foreign_keys=ON`; re-checked in the report (`PRAGMA foreign_key_check`) plus "every accepted (version, code) exists in `ref_code`".

## Useful queries
```sql
-- reject reasons for the latest file load
SELECT reason_code, COUNT(*) FROM event_reject GROUP BY 1 ORDER BY 2 DESC;
-- deprecated codes still in use, with the code to migrate to
SELECT dataset_name, code, replaced_by_code, COUNT(*) FROM event_validated
 WHERE validation_status='valid_deprecated' GROUP BY 1,2,3 ORDER BY 4 DESC;
-- what changed for an event across reference refreshes
SELECT outcome, ref_version_id, run_id FROM event_history WHERE event_id='E010' ORDER BY rowid;
-- trace a reject back to its source line
SELECT source_file, source_line, raw_record, reason_detail FROM event_reject WHERE event_id='E014';
```
