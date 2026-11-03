"""SQLite schema and connection helper."""
import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id                TEXT PRIMARY KEY,
    run_type              TEXT NOT NULL CHECK (run_type IN ('reference_load','event_load')),
    source_file           TEXT NOT NULL,
    input_sha256          TEXT,
    status                TEXT NOT NULL CHECK (status IN ('running','success','failed')),
    started_at            TEXT NOT NULL,
    ended_at              TEXT,
    duration_seconds      REAL,
    rows_read             INTEGER NOT NULL DEFAULT 0,
    rows_blank_skipped    INTEGER NOT NULL DEFAULT 0,
    rows_valid            INTEGER NOT NULL DEFAULT 0,
    rows_valid_deprecated INTEGER NOT NULL DEFAULT 0,
    rows_rejected         INTEGER NOT NULL DEFAULT 0,
    amount_cents_accepted INTEGER NOT NULL DEFAULT 0,
    amount_cents_rejected INTEGER NOT NULL DEFAULT 0,
    reconciled            INTEGER,
    ref_versions_json     TEXT,
    notes                 TEXT,
    failure_reason        TEXT,
    stage_timings_json    TEXT
);

CREATE TABLE IF NOT EXISTS ref_version (
    ref_version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name   TEXT NOT NULL,
    content_hash   TEXT NOT NULL,
    row_count      INTEGER NOT NULL,
    source_file    TEXT NOT NULL,
    run_id         TEXT NOT NULL REFERENCES pipeline_run(run_id),
    loaded_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_code (
    ref_version_id   INTEGER NOT NULL REFERENCES ref_version(ref_version_id),
    dataset_name     TEXT NOT NULL,
    code             TEXT NOT NULL,
    description      TEXT,
    status           TEXT NOT NULL CHECK (status IN ('active','deprecated')),
    effective_from   TEXT NOT NULL,
    effective_to     TEXT,                -- exclusive; NULL = open ended
    replaced_by_code TEXT,
    PRIMARY KEY (ref_version_id, code, effective_from)
);

CREATE TABLE IF NOT EXISTS event_validated (
    event_id          TEXT PRIMARY KEY,
    source_system     TEXT NOT NULL,
    event_ts          TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    effective_date    TEXT NOT NULL,
    dataset_name      TEXT NOT NULL,
    code              TEXT NOT NULL,
    amount_cents      INTEGER NOT NULL CHECK (amount_cents >= 0),
    validation_status TEXT NOT NULL CHECK (validation_status IN ('valid','valid_deprecated')),
    warning_code      TEXT,
    replaced_by_code  TEXT,
    ref_version_id    INTEGER NOT NULL REFERENCES ref_version(ref_version_id),
    record_hash       TEXT NOT NULL,
    run_id            TEXT NOT NULL REFERENCES pipeline_run(run_id),
    source_file       TEXT NOT NULL,
    source_line       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS event_reject (
    source_file    TEXT NOT NULL,
    source_line    INTEGER NOT NULL,
    reject_id      TEXT NOT NULL,
    run_id         TEXT NOT NULL REFERENCES pipeline_run(run_id),
    event_id       TEXT,
    reason_code    TEXT NOT NULL,
    reason_detail  TEXT,
    ref_version_id INTEGER,
    record_hash    TEXT NOT NULL,
    raw_record     TEXT NOT NULL,
    amount_cents   INTEGER,
    rejected_at    TEXT NOT NULL,
    PRIMARY KEY (source_file, source_line)
);

CREATE TABLE IF NOT EXISTS event_history (
    event_id       TEXT NOT NULL,
    record_hash    TEXT NOT NULL,
    outcome        TEXT NOT NULL,        -- valid | valid_deprecated | rejected:<REASON>
    ref_version_id INTEGER NOT NULL DEFAULT 0,
    run_id         TEXT NOT NULL,
    source_file    TEXT NOT NULL,
    source_line    INTEGER NOT NULL,
    recorded_at    TEXT NOT NULL,
    PRIMARY KEY (event_id, record_hash, outcome, ref_version_id)
);

CREATE TABLE IF NOT EXISTS run_reject_summary (
    run_id      TEXT NOT NULL REFERENCES pipeline_run(run_id),
    reason_code TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    PRIMARY KEY (run_id, reason_code)
);

CREATE INDEX IF NOT EXISTS ix_ref_version_dataset ON ref_version(dataset_name, ref_version_id);
CREATE INDEX IF NOT EXISTS ix_event_validated_code ON event_validated(dataset_name, code);
CREATE INDEX IF NOT EXISTS ix_event_reject_reason ON event_reject(reason_code);
"""


def connect(db_path):
    """Autocommit connection (isolation_level=None) so transactions are explicit."""
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    return conn
