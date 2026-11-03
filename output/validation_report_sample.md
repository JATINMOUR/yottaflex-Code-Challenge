# Validation report - run `evt_20261103T091418Z_d9e14a90`

Generated from database `demo.db`. All numbers are recomputed from the database.

## 1. Run summary

| field | value |
|---|---|
| run_type | event_load |
| source_file | events_sample.csv |
| status | success |
| started_at | 2026-11-03T09:14:18Z |
| ended_at | 2026-11-03T09:14:18Z |
| duration_seconds | 0.0149 |
| input_sha256 | bc3daa6a43cdaffae51ab0ea713681072db542bbaa3ac967fa66e1fa7becf91a |
| rows_read | 24 |
| rows_blank_skipped | 0 |
| rows_valid | 5 |
| rows_valid_deprecated | 4 |
| rows_rejected | 15 |
| amount_cents_accepted | 57349 |
| amount_cents_rejected | 58650 |
| ref_versions_json | {"diagnosis_code": 3, "procedure_code": 4} |
| notes |  |
| failure_reason | None |

## 2. Reconciliation checks

| check | result | detail |
|---|---|---|
| run counters: read = valid + deprecated + rejected | PASS | 24 = 5 + 4 + 15 |
| target accepted rows/amount match run counters | PASS | db=9/57349 run=9/57349 |
| target rejected rows/amount match run counters | PASS | db=15/58650 run=15/58650 |
| source rows = accepted + rejected in target | PASS | 24 = 9 + 15 |
| every reject traceable (file, line, raw record, id) | PASS | 0 untraceable |
| no record is both accepted and rejected (same file) | PASS | 0 overlaps |
| referential integrity (PRAGMA foreign_key_check) | PASS | 0 violations |
| every accepted event's (ref version, code) exists in reference data | PASS | 0 orphans |
| independent input recount = rows_read | PASS | file=24 run=24 |
| amount: parseable input total = accepted + rejected | PASS | input=115999 target=115999 cents (20 parseable rows) |

Overall: **ALL PASS**

## 3. Rejects by reason (this run)

| reason_code | rows |
|---|---|
| CODE_EXPIRED | 3 |
| INVALID_AMOUNT | 3 |
| INVALID_TIMESTAMP | 2 |
| CODE_NOT_YET_EFFECTIVE | 1 |
| DUPLICATE_EVENT | 1 |
| MALFORMED_RECORD | 1 |
| MISSING_REQUIRED_FIELD | 1 |
| SUPERSEDED_VERSION | 1 |
| UNKNOWN_CODE | 1 |
| UNKNOWN_REFERENCE_DATASET | 1 |

## 4. Accepted events by status and dataset (current target state)

| dataset_name | validation_status | events | total_amount |
|---|---|---|---|
| diagnosis_code | valid | 4 | 183.50 |
| diagnosis_code | valid_deprecated | 4 | 359.99 |
| procedure_code | valid | 1 | 30.00 |

## 5. Deprecated-code usage with suggested replacement (data-quality warning)

| dataset_name | code | replaced_by_code | events |
|---|---|---|---|
| diagnosis_code | D200 | D210 | 2 |
| diagnosis_code | D150 | D250 | 1 |
| diagnosis_code | D300 | D100 | 1 |

## 6. Reference versions in the database

| ref_version_id | dataset_name | rows | source_file | loaded_at |
|---|---|---|---|---|
| 1 | diagnosis_code | 6 | reference_v1.csv | 2026-11-03T09:14:18Z |
| 2 | procedure_code | 2 | reference_v1.csv | 2026-11-03T09:14:18Z |
| 3 | diagnosis_code | 7 | reference_v2.csv | 2026-11-03T09:14:18Z |
| 4 | procedure_code | 3 | reference_v2.csv | 2026-11-03T09:14:18Z |

## 7. Sample rejects (first 10, with source trace)

| source_file | line | event_id | reason_code | detail |
|---|---|---|---|---|
| events_sample.csv | 3 | E002 | SUPERSEDED_VERSION | superseded by line 19 (updated_at 2025-03-12T08:00:00Z >= 2025-03-10T10:00:00Z) |
| events_sample.csv | 5 | E004 | CODE_EXPIRED | 'D150' not effective on 2025-01-01 (v3) |
| events_sample.csv | 7 | E006 | CODE_EXPIRED | 'D300' not effective on 2024-06-01 (v3) |
| events_sample.csv | 8 | E007 | CODE_NOT_YET_EFFECTIVE | 'D900' effective from 2026-06-01, event on 2025-03-10 |
| events_sample.csv | 9 | E008 | UNKNOWN_CODE | 'D777' not in diagnosis_code v3 |
| events_sample.csv | 11 | E010 | CODE_EXPIRED | 'P200' not effective on 2025-03-11 (v4) |
| events_sample.csv | 12 | E011 | UNKNOWN_REFERENCE_DATASET | dataset 'lab_code' not loaded |
| events_sample.csv | 13 | E012 | MISSING_REQUIRED_FIELD | empty: code |
| events_sample.csv | 14 | E013 | INVALID_TIMESTAMP | event_ts 'not-a-date': Invalid isoformat string: 'not-a-date' |
| events_sample.csv | 15 | E014 | INVALID_AMOUNT | not a number: 'abc' |

## 8. Event history (changes preserved)

| event_id | outcomes recorded |
|---|---|
| E001 | valid@v1 -> valid@v3 |
| E002 | valid@v1 -> rejected:SUPERSEDED_VERSION@v0 -> valid_deprecated@v3 |
| E003 | valid_deprecated@v1 -> valid_deprecated@v3 |
| E004 | rejected:CODE_EXPIRED@v1 -> rejected:CODE_EXPIRED@v3 |
| E005 | valid_deprecated@v1 -> valid_deprecated@v3 |
| E006 | rejected:CODE_EXPIRED@v1 -> rejected:CODE_EXPIRED@v3 |
| E007 | rejected:CODE_NOT_YET_EFFECTIVE@v1 -> rejected:CODE_NOT_YET_EFFECTIVE@v3 |
| E008 | rejected:UNKNOWN_CODE@v1 -> rejected:UNKNOWN_CODE@v3 |
| E009 | valid@v2 -> valid@v4 |
| E010 | valid@v2 -> rejected:CODE_EXPIRED@v4 |

## 9. Run history

| run_id | type | file | status | read | valid | deprecated | rejected | seconds |
|---|---|---|---|---|---|---|---|---|
| ref_20261103T091418Z_6cd308a9 | reference_load | reference_v1.csv | success | 8 | 8 | 0 | 0 | 0.0098 |
| evt_20261103T091418Z_0377cd4b | event_load | events_sample.csv | success | 24 | 8 | 2 | 14 | 0.0154 |
| evt_20261103T091418Z_ebf12fc1 | event_load | events_sample.csv | success | 24 | 8 | 2 | 14 | 0.0172 |
| ref_20261103T091418Z_ce4bd2a1 | reference_load | reference_v2.csv | success | 10 | 10 | 0 | 0 | 0.0169 |
| evt_20261103T091418Z_d9e14a90 | event_load | events_sample.csv | success | 24 | 5 | 4 | 15 | 0.0149 |
