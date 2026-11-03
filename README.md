# Build guide: Code Validator project on Windows (step by step)

Is guide ko upar se neeche follow karo. Har **Checkpoint** pe ruk ke command chalao: agar wo pass ho gaya to aage badho, nahi to Troubleshooting section dekho.
Total ~25 files. Koi external library nahi chahiye: sirf Python.

---

## STEP 0: Tools install karo (ek baar)

1. **Python 3.11 ya 3.12** download karo: https://www.python.org/downloads/ . Installer ke pehle screen pe **"Add python.exe to PATH" tick karna zaroori hai**, phir *Install Now*.
2. **VS Code** install karo (https://code.visualstudio.com/). Python extension optional hai.
3. **PowerShell** kholo (Start menu mein "PowerShell") aur check karo:

```powershell
py --version
```
Output `Python 3.x.x` aana chahiye. (Agar `py` na chale to `python --version` try karo, aur neeche sab jagah `py` ki jagah `python` likho.)

---

## STEP 1: Folder structure banao

PowerShell mein:

```powershell
cd $HOME\Documents
mkdir code_validator
cd code_validator
mkdir codevalidator, tests, docs, output
mkdir data\sample\reference, data\sample\expected
code .
```
`code .` se VS Code isi folder mein khulega. Ab har file ko VS Code mein **New File** se banao (Ctrl+N, phir Ctrl+S, aur path/naam exact waisa jaisa neeche likha hai).

Final structure aisa dikhega:

```text
code_validator/
  README.md  requirements.txt  bench.py
  codevalidator/   __init__.py __main__.py rules.py util.py db.py validate.py reference.py pipeline.py report.py datagen.py cli.py
  tests/           __init__.py helpers.py test_validators.py test_reference_load.py test_event_pipeline.py test_scale_and_cli.py
  data/sample/     events_sample.csv
  data/sample/reference/  reference_v1.csv reference_v2.csv
  data/sample/expected/   events_sample_expected_ref_v1.csv events_sample_expected_ref_v2.csv
  docs/            DATA_DICTIONARY.md DESIGN_NOTES.md
  output/          (reports yahan banenge)
```

> **Bahut zaroori (Windows/VS Code):**
> * Har file **UTF-8** mein save ho (VS Code ka default hai).
> * CSV files mein **trailing space mat hatao**. `events_sample.csv` mein ek line `p200 ,45.00` hai jisme `p200` ke baad **jaan-boojh ke space** hai (test normalisation check karta hai). Agar "trim trailing whitespace" setting on hai to us line ko manually check karo.
> * Python mein **indentation (4 spaces)** exactly copy karo. Tabs mat mix karo.
> * Code block copy karte waqt sirf andar ka content copy karo, ```` ```` ```` wali fence lines nahi.

---

## STEP 2: Foundation files (constants, helpers, database)

Ye chaaro files dusre modules ki neev hain. Inme business logic nahi, sirf constants, CSV reading, logging aur SQL tables hain.

#### codevalidator/__init__.py
Package marker. Is file ke bina `import codevalidator` nahi chalega.

**File: `codevalidator/__init__.py`**

````python
"""Code validation pipeline: validate incoming codes against versioned reference datasets."""
__version__ = "1.0.0"
````

#### codevalidator/rules.py
Saare reject reason codes aur required columns ek jagah. Source of truth.

**File: `codevalidator/rules.py`**

````python
"""Constants: column contracts, reason codes and statuses. Single source of truth."""

REQUIRED_EVENT_COLUMNS = [
    "event_id", "source_system", "event_ts", "updated_at",
    "dataset_name", "code", "amount",
]
REQUIRED_REF_COLUMNS = [
    "dataset_name", "code", "description", "status",
    "effective_from", "effective_to", "replaced_by_code",
]

# Reject reason codes (exactly one per rejected source row; first failing rule wins).
MALFORMED_RECORD = "MALFORMED_RECORD"
MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
INVALID_AMOUNT = "INVALID_AMOUNT"
UNKNOWN_REFERENCE_DATASET = "UNKNOWN_REFERENCE_DATASET"
UNKNOWN_CODE = "UNKNOWN_CODE"
CODE_NOT_YET_EFFECTIVE = "CODE_NOT_YET_EFFECTIVE"
CODE_EXPIRED = "CODE_EXPIRED"
DUPLICATE_EVENT = "DUPLICATE_EVENT"          # identical re-delivery inside one file
SUPERSEDED_VERSION = "SUPERSEDED_VERSION"    # older version of an event inside one file
STALE_VERSION = "STALE_VERSION"              # older/equal version than one already loaded

# Validation statuses of accepted events
VALID = "valid"
VALID_DEPRECATED = "valid_deprecated"
WARNING_CODE_DEPRECATED = "CODE_DEPRECATED"

# Reference statuses
REF_ACTIVE = "active"
REF_DEPRECATED = "deprecated"
REF_STATUSES = (REF_ACTIVE, REF_DEPRECATED)
````

#### codevalidator/util.py
Time/ID/hash helpers, safe CSV reader (empty file, blank lines), `PipelineError`, logging.

**File: `codevalidator/util.py`**

````python
"""Small shared helpers: time, hashing, CSV reading, logging."""
import csv
import hashlib
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

LOG = logging.getLogger("codevalidator")


class PipelineError(Exception):
    """A run-level failure (bad file/schema/reference data). Carries a stable code."""

    def __init__(self, code, message=""):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id(prefix="run"):
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def short_hash(*parts):
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:16]


def read_csv(path):
    """Return (header, [(physical_line_no, fields)], blank_lines_skipped).

    Raises PipelineError for missing/zero-byte/unreadable files. Blank lines are skipped
    and counted, never silently lost. Header-only files return an empty row list.
    """
    if not os.path.isfile(path):
        raise PipelineError("INPUT_NOT_FOUND", path)
    if os.path.getsize(path) == 0:
        raise PipelineError("EMPTY_FILE", "zero-byte file (no header)")
    rows, blanks = [], 0
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                raise PipelineError("EMPTY_FILE", "no header row")
            for fields in reader:
                if not fields:
                    blanks += 1
                    continue
                rows.append((reader.line_num, fields))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise PipelineError("UNREADABLE_FILE", str(exc))
    return header, rows, blanks


def setup_logging(level=logging.INFO, log_file=None):
    handlers = [logging.StreamHandler(sys.stderr)]
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level, handlers=handlers, force=True,
        format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
````

#### codevalidator/db.py
SQLite schema (saari tables) + connection. Idempotency ke liye primary keys yahin define hain.

**File: `codevalidator/db.py`**

````python
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
````

#### codevalidator/__main__.py
`py -m codevalidator` chalane ke liye entry point.

**File: `codevalidator/__main__.py`**

````python
import sys
from .cli import main

sys.exit(main())
````

### Checkpoint 2
```powershell
py -c "import codevalidator.db, codevalidator.util, codevalidator.rules; print('foundation OK')"
```

---

## STEP 3: Row validation + reference data


#### codevalidator/validate.py
Ek row ko parse/validate karta hai: timestamp (timezone zaroori), amount (cents mein, 2 decimals max), required fields, record hash.

**File: `codevalidator/validate.py`**

````python
"""Row-level parsing and field validation for incoming events (pure functions, no I/O)."""
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from . import rules
from .util import short_hash


class RecordError(Exception):
    def __init__(self, reason, detail):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Event:
    line_no: int
    event_id: str
    source_system: str
    event_ts: str          # normalised UTC, ISO-8601 'YYYY-MM-DDTHH:MM:SSZ'
    updated_at: str        # normalised UTC
    effective_date: date   # UTC calendar date of event_ts: the date used for reference lookup
    dataset_name: str      # lower-cased
    code: str              # upper-cased
    amount_cents: int
    record_hash: str


def parse_ts(value):
    """Strict ISO-8601 with an explicit offset ('Z' or +hh:mm). Returns aware UTC datetime."""
    v = value.strip()
    if v.endswith(("Z", "z")):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)  # ValueError on garbage
    if dt.tzinfo is None:
        raise ValueError("timezone offset required")
    return dt.astimezone(timezone.utc)


def fmt_ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_amount(value):
    """Non-negative decimal with at most 2 fraction digits -> integer cents (exact)."""
    try:
        d = Decimal(value.strip())
    except InvalidOperation:
        raise ValueError(f"not a number: {value!r}")
    if not d.is_finite():
        raise ValueError(f"not finite: {value!r}")
    if d < 0:
        raise ValueError(f"negative amount: {value!r}")
    if d.as_tuple().exponent < -2:
        raise ValueError(f"more than 2 decimal places: {value!r}")
    return int((d * 100).to_integral_value())


def compute_record_hash(fields):
    """Hash of the stripped raw payload; same payload => same hash (drives idempotency)."""
    return short_hash(*[f.strip() for f in fields])


def best_effort_amount(fields, idx):
    try:
        return parse_amount(fields[idx["amount"]])
    except (ValueError, IndexError):
        return None


def parse_event(line_no, fields, ncols, idx):
    """Validate one CSV row. Raises RecordError (first failing rule wins).

    Rule order: column count -> required fields -> timestamps -> amount.
    """
    if len(fields) != ncols:
        raise RecordError(rules.MALFORMED_RECORD, f"expected {ncols} fields, found {len(fields)}")
    val = {c: fields[i].strip() for c, i in idx.items()}
    empty = [c for c in rules.REQUIRED_EVENT_COLUMNS if not val[c]]
    if empty:
        raise RecordError(rules.MISSING_REQUIRED_FIELD, "empty: " + ",".join(empty))
    try:
        ts = parse_ts(val["event_ts"])
    except ValueError as exc:
        raise RecordError(rules.INVALID_TIMESTAMP, f"event_ts {val['event_ts']!r}: {exc}")
    try:
        upd = parse_ts(val["updated_at"])
    except ValueError as exc:
        raise RecordError(rules.INVALID_TIMESTAMP, f"updated_at {val['updated_at']!r}: {exc}")
    try:
        cents = parse_amount(val["amount"])
    except ValueError as exc:
        raise RecordError(rules.INVALID_AMOUNT, str(exc))
    payload = [fields[idx[c]] for c in rules.REQUIRED_EVENT_COLUMNS]
    return Event(
        line_no=line_no, event_id=val["event_id"], source_system=val["source_system"],
        event_ts=fmt_ts(ts), updated_at=fmt_ts(upd), effective_date=ts.date(),
        dataset_name=val["dataset_name"].lower(), code=val["code"].upper(),
        amount_cents=cents, record_hash=compute_record_hash(payload),
    )


def raw_json(fields):
    return json.dumps(fields, ensure_ascii=False)
````

#### codevalidator/reference.py
Reference snapshot load (version banata hai, same content dobara aaye to no-op), aur `RefIndex.classify()`: effective-date window logic (from inclusive, to exclusive).

**File: `codevalidator/reference.py`**

````python
"""Versioned reference datasets: load/refresh (idempotent, immutable versions) and lookup."""
import hashlib
import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Optional

from . import rules
from .db import connect
from .util import (LOG, PipelineError, new_run_id, read_csv, sha256_file, utc_now_iso)


# ----------------------------------------------------------------------------- lookup
@dataclass(frozen=True)
class Window:
    effective_from: date
    effective_to: Optional[date]   # exclusive
    status: str
    replaced_by: Optional[str]
    version_id: int


@dataclass(frozen=True)
class Decision:
    status: str                    # 'valid' | 'valid_deprecated' | 'reject'
    reason_code: Optional[str] = None
    detail: Optional[str] = None
    version_id: Optional[int] = None
    replaced_by: Optional[str] = None


class RefIndex:
    """In-memory index {(dataset, code): [Window,...]} for one chosen version per dataset.

    Built once per run so per-row validation is a dict lookup, not a SQL query.
    """

    def __init__(self, windows, versions):
        self.windows = windows      # dict[(dataset, code)] -> list[Window] sorted by from
        self.versions = versions    # dict[dataset] -> ref_version_id

    @classmethod
    def from_db(cls, conn, pins=None):
        pins = pins or {}
        versions = {}
        for r in conn.execute("SELECT dataset_name, MAX(ref_version_id) v FROM ref_version GROUP BY dataset_name"):
            versions[r["dataset_name"]] = r["v"]
        for ds, vid in pins.items():
            ok = conn.execute("SELECT 1 FROM ref_version WHERE dataset_name=? AND ref_version_id=?", (ds, vid)).fetchone()
            if not ok:
                raise PipelineError("INVALID_PIN", f"no version {vid} for dataset {ds!r}")
            versions[ds] = vid
        if not versions:
            raise PipelineError("NO_REFERENCE_DATA", "load a reference dataset first (load-ref)")
        windows = defaultdict(list)
        for ds, vid in versions.items():
            for r in conn.execute(
                "SELECT code, status, effective_from, effective_to, replaced_by_code "
                "FROM ref_code WHERE ref_version_id=? ORDER BY code, effective_from", (vid,)):
                windows[(ds, r["code"])].append(Window(
                    date.fromisoformat(r["effective_from"]),
                    date.fromisoformat(r["effective_to"]) if r["effective_to"] else None,
                    r["status"], r["replaced_by_code"], vid))
        return cls(dict(windows), versions)

    def classify(self, dataset, code, on_date):
        """Decide validity of `code` on `on_date`. effective_from inclusive, effective_to exclusive."""
        vid = self.versions.get(dataset)
        if vid is None:
            return Decision("reject", rules.UNKNOWN_REFERENCE_DATASET, f"dataset {dataset!r} not loaded")
        ws = self.windows.get((dataset, code))
        if not ws:
            return Decision("reject", rules.UNKNOWN_CODE, f"{code!r} not in {dataset} v{vid}", vid)
        for w in ws:
            if w.effective_from <= on_date and (w.effective_to is None or on_date < w.effective_to):
                if w.status == rules.REF_ACTIVE:
                    return Decision(rules.VALID, version_id=vid)
                return Decision(rules.VALID_DEPRECATED, rules.WARNING_CODE_DEPRECATED,
                                f"deprecated until {w.effective_to}", vid, w.replaced_by)
        if on_date < ws[0].effective_from:
            return Decision("reject", rules.CODE_NOT_YET_EFFECTIVE,
                            f"{code!r} effective from {ws[0].effective_from}, event on {on_date}", vid)
        return Decision("reject", rules.CODE_EXPIRED,
                        f"{code!r} not effective on {on_date} (v{vid})", vid)


# ----------------------------------------------------------------------------- loading
def _parse_ref_rows(header, rows):
    cols = [h.strip().lower() for h in header]
    missing = [c for c in rules.REQUIRED_REF_COLUMNS if c not in cols]
    if missing:
        raise PipelineError("SCHEMA_MISSING_COLUMNS", "reference file missing: " + ",".join(missing))
    idx = {c: cols.index(c) for c in rules.REQUIRED_REF_COLUMNS}
    errors, parsed = [], []
    for line_no, f in rows:
        if len(f) != len(cols):
            errors.append(f"line {line_no}: expected {len(cols)} fields, found {len(f)}")
            continue
        v = {c: f[i].strip() for c, i in idx.items()}
        ds, code = v["dataset_name"].lower(), v["code"].upper()
        rep = v["replaced_by_code"].upper() or None
        if not (ds and code and v["status"] and v["effective_from"]):
            errors.append(f"line {line_no}: dataset_name, code, status, effective_from are required")
            continue
        status = v["status"].lower()
        if status not in rules.REF_STATUSES:
            errors.append(f"line {line_no}: invalid status {v['status']!r}")
            continue
        try:
            d_from = date.fromisoformat(v["effective_from"])
            d_to = date.fromisoformat(v["effective_to"]) if v["effective_to"] else None
        except ValueError as exc:
            errors.append(f"line {line_no}: bad date ({exc})")
            continue
        if d_to is not None and d_to <= d_from:
            errors.append(f"line {line_no}: effective_to must be after effective_from")
            continue
        if rep and status != rules.REF_DEPRECATED:
            errors.append(f"line {line_no}: replaced_by_code only allowed on deprecated rows")
            continue
        if rep == code:
            errors.append(f"line {line_no}: code cannot replace itself")
            continue
        parsed.append((line_no, ds, code, v["description"], status, d_from, d_to, rep))
    return parsed, errors


def _check_integrity(parsed, errors):
    """Duplicate keys, overlapping windows, dangling replaced_by_code (within the same dataset)."""
    seen, by_code, codes = set(), defaultdict(list), set()
    for line_no, ds, code, _d, _s, d_from, d_to, rep in parsed:
        key = (ds, code, d_from)
        if key in seen:
            errors.append(f"line {line_no}: duplicate key {key}")
        seen.add(key)
        by_code[(ds, code)].append((d_from, d_to, line_no))
        codes.add((ds, code))
    for key, ws in by_code.items():
        ws.sort(key=lambda w: w[0])
        for a, b in zip(ws, ws[1:]):
            if a[1] is None or b[0] < a[1]:
                errors.append(f"{key}: overlapping effective windows (lines {a[2]} and {b[2]})")
    for line_no, ds, code, _d, _s, _f, _t, rep in parsed:
        if rep and (ds, rep) not in codes:
            errors.append(f"line {line_no}: replaced_by_code {rep!r} not found in dataset {ds!r}")


def load_reference(db_path, path, run_id=None):
    """Load a reference snapshot file (may hold several datasets). Atomic and idempotent.

    A dataset whose canonical content hash equals its latest stored version is a no-op;
    otherwise a NEW immutable version is appended (old versions are never modified).
    Returns a dict summary; failures are recorded in pipeline_run and nothing is written.
    """
    run_id = run_id or new_run_id("ref")
    conn = connect(db_path)
    t0, source_file = time.perf_counter(), os.path.basename(path)
    conn.execute("INSERT INTO pipeline_run(run_id, run_type, source_file, status, started_at) "
                 "VALUES (?, 'reference_load', ?, 'running', ?)", (run_id, source_file, utc_now_iso()))
    LOG.info("run_id=%s type=reference_load file=%s status=started", run_id, source_file)
    summary = {"run_id": run_id, "status": "failed", "datasets": {}, "failure_reason": None, "rows_read": 0}
    try:
        sha = sha256_file(path) if os.path.isfile(path) else None
        header, rows, blanks = read_csv(path)
        summary["rows_read"] = len(rows)
        parsed, errors = _parse_ref_rows(header, rows)
        _check_integrity(parsed, errors)
        if errors:
            raise PipelineError("REFERENCE_VALIDATION_FAILED", "; ".join(errors[:10]) +
                                (f" (+{len(errors) - 10} more)" if len(errors) > 10 else ""))
        if not parsed:
            raise PipelineError("EMPTY_REFERENCE", "reference file has a header but no rows")
        by_ds = defaultdict(list)
        for p in parsed:
            by_ds[p[1]].append(p)
        notes = []
        conn.execute("BEGIN IMMEDIATE")
        for ds, items in sorted(by_ds.items()):
            canon = sorted((p[2], str(p[5]), str(p[6] or ""), p[4], p[7] or "", p[3]) for p in items)
            chash = hashlib.sha256(json.dumps(canon).encode()).hexdigest()
            latest = conn.execute("SELECT ref_version_id, content_hash FROM ref_version WHERE dataset_name=? "
                                  "ORDER BY ref_version_id DESC LIMIT 1", (ds,)).fetchone()
            if latest and latest["content_hash"] == chash:
                summary["datasets"][ds] = {"version_id": latest["ref_version_id"], "created": False}
                notes.append(f"{ds}: unchanged (v{latest['ref_version_id']})")
                continue
            cur = conn.execute("INSERT INTO ref_version(dataset_name, content_hash, row_count, source_file, run_id, loaded_at) "
                               "VALUES (?,?,?,?,?,?)", (ds, chash, len(items), source_file, run_id, utc_now_iso()))
            vid = cur.lastrowid
            conn.executemany(
                "INSERT INTO ref_code(ref_version_id, dataset_name, code, description, status, effective_from, "
                "effective_to, replaced_by_code) VALUES (?,?,?,?,?,?,?,?)",
                [(vid, ds, p[2], p[3], p[4], str(p[5]), str(p[6]) if p[6] else None, p[7]) for p in items])
            summary["datasets"][ds] = {"version_id": vid, "created": True}
            notes.append(f"{ds}: created v{vid} ({len(items)} rows)")
        _finish(conn, run_id, t0, "success", rows_read=len(rows), rows_valid=len(rows), sha=sha,
                blanks=blanks, notes="; ".join(notes),
                versions={d: v["version_id"] for d, v in summary["datasets"].items()})
        conn.execute("COMMIT")
        summary["status"] = "success"
    except Exception as exc:  # noqa: BLE001 - run-level failure is recorded, not hidden
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        code = exc.code if isinstance(exc, PipelineError) else "UNEXPECTED_ERROR"
        reason = str(exc) if isinstance(exc, PipelineError) else f"UNEXPECTED_ERROR: {exc!r}"
        summary["failure_reason"] = reason
        _finish(conn, run_id, t0, "failed", failure=reason, rows_read=summary["rows_read"])
        LOG.error("run_id=%s type=reference_load status=failed code=%s reason=%s", run_id, code, reason)
    finally:
        conn.close()
    LOG.info("run_id=%s type=reference_load status=%s datasets=%s", run_id, summary["status"], summary["datasets"])
    return summary


def _finish(conn, run_id, t0, status, failure=None, rows_read=0, rows_valid=0, sha=None, blanks=0,
            notes=None, versions=None):
    conn.execute(
        "UPDATE pipeline_run SET status=?, ended_at=?, duration_seconds=?, rows_read=?, rows_valid=?, "
        "rows_blank_skipped=?, input_sha256=?, notes=?, failure_reason=?, ref_versions_json=?, reconciled=? WHERE run_id=?",
        (status, utc_now_iso(), round(time.perf_counter() - t0, 4), rows_read, rows_valid, blanks, sha, notes,
         failure, json.dumps(versions, sort_keys=True) if versions else None, 1 if status == "success" else None, run_id))
````

### Checkpoint 3
```powershell
py -c "import codevalidator.reference, codevalidator.validate; print('validation OK')"
```

---

## STEP 4: Main pipeline

#### codevalidator/pipeline.py
Sabse important file. Stages: read, field validation, in-file dedupe, stale check, reference check, atomic transaction publish, reconciliation. Ek hi transaction hone se retry safe hai.

**File: `codevalidator/pipeline.py`**

````python
"""Event load pipeline: parse -> dedupe -> stale-check -> reference classify -> atomic publish.

Design: one SQLite transaction per input file. Either the whole file is published (accepted
rows, rejects, history, run metrics, reconciliation) or nothing is (run row is then marked failed).
"""
import json
import os
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional

from . import rules
from .db import connect
from .reference import RefIndex
from .util import LOG, PipelineError, new_run_id, read_csv, sha256_file, short_hash, utc_now_iso
from .validate import RecordError, best_effort_amount, compute_record_hash, parse_event, raw_json


class InjectedFailure(RuntimeError):
    """Raised only by tests/demos through `fail_after_rows` to simulate a mid-load crash."""


@dataclass
class Reject:
    line_no: int
    event_id: Optional[str]
    reason: str
    detail: str
    record_hash: str
    raw: str
    amount_cents: Optional[int] = None
    ref_version_id: Optional[int] = None
    delete_stored: bool = False      # reference-based reject of a record already stored as valid


@dataclass
class RunResult:
    run_id: str
    status: str
    source_file: str
    rows_read: int = 0
    rows_blank_skipped: int = 0
    rows_valid: int = 0
    rows_valid_deprecated: int = 0
    rows_rejected: int = 0
    amount_cents_accepted: int = 0
    amount_cents_rejected: int = 0
    reject_breakdown: dict = field(default_factory=dict)
    failure_reason: Optional[str] = None
    notes: str = ""
    stage_seconds: dict = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def rows_accepted(self):
        return self.rows_valid + self.rows_valid_deprecated


def parse_pins(items):
    """['diagnosis_code=1', ...] -> {'diagnosis_code': 1}"""
    pins = {}
    for it in items or []:
        ds, _, v = it.partition("=")
        if not ds or not v.isdigit():
            raise PipelineError("INVALID_PIN", f"expected dataset=version_id, got {it!r}")
        pins[ds.strip().lower()] = int(v)
    return pins


def load_events(db_path, input_path, run_id=None, pins=None, chunk_size=10000, fail_after_rows=None):
    """Validate + publish one events file. Never raises for data problems: returns RunResult.

    Safe to re-run: same input + same reference versions => identical target state.
    """
    run_id = run_id or new_run_id("evt")
    source_file = os.path.basename(input_path)
    conn = connect(db_path)
    t0 = time.perf_counter()
    res = RunResult(run_id=run_id, status="failed", source_file=source_file)
    conn.execute("INSERT INTO pipeline_run(run_id, run_type, source_file, status, started_at) "
                 "VALUES (?, 'event_load', ?, 'running', ?)", (run_id, source_file, utc_now_iso()))
    LOG.info("run_id=%s type=event_load file=%s status=started", run_id, source_file)
    sha, versions = None, None
    try:
        sha = sha256_file(input_path) if os.path.isfile(input_path) else None
        versions = _process(conn, input_path, source_file, run_id, pins, chunk_size, fail_after_rows, res, t0)
        res.status = "success"
    except Exception as exc:  # noqa: BLE001 - recorded in pipeline_run, then reported to caller
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        res.status = "failed"
        res.failure_reason = str(exc) if isinstance(exc, PipelineError) else f"UNEXPECTED_ERROR: {exc!r}"
        LOG.error("run_id=%s status=failed reason=%s", run_id, res.failure_reason)
        # Nothing was published (rolled back); keep only what we measured before the failure.
        res.rows_valid = res.rows_valid_deprecated = res.rows_rejected = 0
        res.amount_cents_accepted = res.amount_cents_rejected = 0
        res.reject_breakdown = {}
    res.duration_seconds = round(time.perf_counter() - t0, 4)
    if res.status == "failed":
        conn.execute(
            "UPDATE pipeline_run SET status='failed', ended_at=?, duration_seconds=?, rows_read=?, "
            "rows_blank_skipped=?, input_sha256=?, failure_reason=?, stage_timings_json=? WHERE run_id=?",
            (utc_now_iso(), res.duration_seconds, res.rows_read, res.rows_blank_skipped, sha,
             res.failure_reason, json.dumps(res.stage_seconds), run_id))
    conn.close()
    LOG.info("run_id=%s status=%s read=%d valid=%d deprecated=%d rejected=%d seconds=%.3f %s",
             run_id, res.status, res.rows_read, res.rows_valid, res.rows_valid_deprecated,
             res.rows_rejected, res.duration_seconds, res.failure_reason or "")
    return res


def _process(conn, input_path, source_file, run_id, pins, chunk_size, fail_after_rows, res, t0):
    stage = {}

    # ---- stage 1: read + schema check --------------------------------------------------
    s = time.perf_counter()
    header, rows, blanks = read_csv(input_path)
    res.rows_read, res.rows_blank_skipped = len(rows), blanks
    cols = [h.strip().lower() for h in header]
    if len(set(cols)) != len(cols):
        raise PipelineError("SCHEMA_DUPLICATE_COLUMNS", ",".join(cols))
    missing = [c for c in rules.REQUIRED_EVENT_COLUMNS if c not in cols]
    if missing:
        raise PipelineError("SCHEMA_MISSING_COLUMNS", "missing: " + ",".join(missing))
    extra = [c for c in cols if c not in rules.REQUIRED_EVENT_COLUMNS]
    notes = []
    if extra:
        notes.append("schema_drift: ignored unexpected columns " + ",".join(extra))
        LOG.warning("run_id=%s %s", run_id, notes[-1])
    if blanks:
        notes.append(f"{blanks} blank line(s) skipped")
    idx = {c: cols.index(c) for c in rules.REQUIRED_EVENT_COLUMNS}
    ncols = len(cols)
    index = RefIndex.from_db(conn, parse_pins_dict(pins))
    stage["read_and_index"] = time.perf_counter() - s

    # ---- stage 2: field validation -----------------------------------------------------
    s = time.perf_counter()
    events, rejects = [], []
    for line_no, fields in rows:
        try:
            events.append(parse_event(line_no, fields, ncols, idx))
        except RecordError as exc:
            eid = fields[idx["event_id"]].strip() if len(fields) == ncols else None
            rejects.append(Reject(line_no, eid or None, exc.reason, exc.detail,
                                  compute_record_hash(fields), raw_json(fields),
                                  best_effort_amount(fields, idx) if len(fields) == ncols else None))
    stage["field_validation"] = time.perf_counter() - s

    # ---- stage 3: in-file duplicates / out-of-order versions ---------------------------
    s = time.perf_counter()
    by_id = defaultdict(list)
    for ev in events:
        by_id[ev.event_id].append(ev)
    survivors = []
    raw_by_line = {ln: f for ln, f in rows}
    for grp in by_id.values():
        # newest updated_at wins; ties -> first occurrence (so exact re-deliveries keep line 1)
        win = max(grp, key=lambda e: (e.updated_at, -e.line_no))
        survivors.append(win)
        for e in grp:
            if e is not win:
                same = e.record_hash == win.record_hash
                rejects.append(Reject(
                    e.line_no, e.event_id,
                    rules.DUPLICATE_EVENT if same else rules.SUPERSEDED_VERSION,
                    f"identical to line {win.line_no}" if same else
                    f"superseded by line {win.line_no} (updated_at {win.updated_at} >= {e.updated_at})",
                    e.record_hash, raw_json(raw_by_line[e.line_no]), e.amount_cents))
    survivors.sort(key=lambda e: e.line_no)
    stage["dedupe"] = time.perf_counter() - s

    # ---- stage 4: cross-batch late / out-of-order check --------------------------------
    s = time.perf_counter()
    stored = {}
    ids = [e.event_id for e in survivors]
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        q = "SELECT event_id, record_hash, updated_at FROM event_validated WHERE event_id IN (%s)" % ",".join("?" * len(part))
        for r in conn.execute(q, part):
            stored[r["event_id"]] = (r["record_hash"], r["updated_at"])
    candidates = []
    for e in survivors:
        st = stored.get(e.event_id)
        if st and st[0] != e.record_hash and st[1] >= e.updated_at:
            rejects.append(Reject(e.line_no, e.event_id, rules.STALE_VERSION,
                                  f"stored version has updated_at {st[1]} >= incoming {e.updated_at}",
                                  e.record_hash, raw_json(raw_by_line[e.line_no]), e.amount_cents))
        else:
            candidates.append(e)
    stage["stale_check"] = time.perf_counter() - s

    # ---- stage 5: reference classification ---------------------------------------------
    s = time.perf_counter()
    accepted = []   # (Event, Decision)
    for e in candidates:
        d = index.classify(e.dataset_name, e.code, e.effective_date)
        if d.status == "reject":
            rejects.append(Reject(e.line_no, e.event_id, d.reason_code, d.detail, e.record_hash,
                                  raw_json(raw_by_line[e.line_no]), e.amount_cents, d.version_id,
                                  delete_stored=True))
        else:
            accepted.append((e, d))
    stage["reference_check"] = time.perf_counter() - s

    # ---- counters (before write so a failed run still reports what it saw) --------------
    res.rows_valid = sum(1 for _, d in accepted if d.status == rules.VALID)
    res.rows_valid_deprecated = len(accepted) - res.rows_valid
    res.rows_rejected = len(rejects)
    res.amount_cents_accepted = sum(e.amount_cents for e, _ in accepted)
    res.amount_cents_rejected = sum(r.amount_cents or 0 for r in rejects)
    res.reject_breakdown = dict(Counter(r.reason for r in rejects))
    if res.rows_accepted + res.rows_rejected != res.rows_read:
        raise PipelineError("INTERNAL_COUNT_MISMATCH", "every row must be accepted or rejected exactly once")

    # ---- stage 6: atomic publish -------------------------------------------------------
    s = time.perf_counter()
    now = utc_now_iso()
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("DELETE FROM event_reject WHERE source_file=?", (source_file,))
    conn.executemany("DELETE FROM event_validated WHERE event_id=? AND record_hash=?",
                     [(r.event_id, r.record_hash) for r in rejects if r.delete_stored])
    upsert = (
        "INSERT INTO event_validated(event_id, source_system, event_ts, updated_at, effective_date, dataset_name, "
        "code, amount_cents, validation_status, warning_code, replaced_by_code, ref_version_id, record_hash, "
        "run_id, source_file, source_line) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(event_id) DO UPDATE SET source_system=excluded.source_system, event_ts=excluded.event_ts, "
        "updated_at=excluded.updated_at, effective_date=excluded.effective_date, dataset_name=excluded.dataset_name, "
        "code=excluded.code, amount_cents=excluded.amount_cents, validation_status=excluded.validation_status, "
        "warning_code=excluded.warning_code, replaced_by_code=excluded.replaced_by_code, "
        "ref_version_id=excluded.ref_version_id, record_hash=excluded.record_hash, run_id=excluded.run_id, "
        "source_file=excluded.source_file, source_line=excluded.source_line")
    written = 0
    for i in range(0, len(accepted), chunk_size):
        part = accepted[i:i + chunk_size]
        conn.executemany(upsert, [
            (e.event_id, e.source_system, e.event_ts, e.updated_at, e.effective_date.isoformat(), e.dataset_name,
             e.code, e.amount_cents, d.status, d.reason_code, d.replaced_by, d.version_id, e.record_hash,
             run_id, source_file, e.line_no) for e, d in part])
        written += len(part)
        if fail_after_rows is not None and written >= fail_after_rows:
            raise InjectedFailure(f"injected failure after {written} accepted rows")
    conn.executemany(
        "INSERT INTO event_reject(source_file, source_line, reject_id, run_id, event_id, reason_code, reason_detail, "
        "ref_version_id, record_hash, raw_record, amount_cents, rejected_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [(source_file, r.line_no, short_hash(source_file, str(r.line_no)), run_id, r.event_id, r.reason, r.detail,
          r.ref_version_id, r.record_hash, r.raw, r.amount_cents, now) for r in rejects])
    hist = [(e.event_id, e.record_hash, d.status, d.version_id, run_id, source_file, e.line_no, now) for e, d in accepted]
    hist += [(r.event_id, r.record_hash, "rejected:" + r.reason, r.ref_version_id or 0, run_id, source_file,
              r.line_no, now) for r in rejects if r.event_id and r.reason != rules.DUPLICATE_EVENT]
    conn.executemany("INSERT OR IGNORE INTO event_history(event_id, record_hash, outcome, ref_version_id, run_id, "
                     "source_file, source_line, recorded_at) VALUES (?,?,?,?,?,?,?,?)", hist)
    conn.execute("DELETE FROM run_reject_summary WHERE run_id=?", (run_id,))
    conn.executemany("INSERT INTO run_reject_summary(run_id, reason_code, row_count) VALUES (?,?,?)",
                     [(run_id, k, v) for k, v in res.reject_breakdown.items()])
    stage["publish"] = time.perf_counter() - s

    # ---- stage 7: reconciliation assertions (inside the transaction) --------------------
    a = conn.execute("SELECT COUNT(*) n, COALESCE(SUM(amount_cents),0) amt FROM event_validated WHERE run_id=?", (run_id,)).fetchone()
    b = conn.execute("SELECT COUNT(*) n, COALESCE(SUM(amount_cents),0) amt FROM event_reject WHERE run_id=?", (run_id,)).fetchone()
    if (a["n"], a["amt"]) != (res.rows_accepted, res.amount_cents_accepted) or \
       (b["n"], b["amt"]) != (res.rows_rejected, res.amount_cents_rejected) or \
       a["n"] + b["n"] != res.rows_read:
        raise PipelineError("RECONCILIATION_FAILED",
                            f"target accepted={a['n']}/{a['amt']} rejected={b['n']}/{b['amt']} vs source read={res.rows_read}")
    res.stage_seconds = {k: round(v, 4) for k, v in stage.items()}
    res.notes = "; ".join(notes)
    conn.execute(
        "UPDATE pipeline_run SET status='success', ended_at=?, duration_seconds=?, input_sha256=?, rows_read=?, "
        "rows_blank_skipped=?, rows_valid=?, rows_valid_deprecated=?, rows_rejected=?, amount_cents_accepted=?, "
        "amount_cents_rejected=?, reconciled=1, ref_versions_json=?, notes=?, stage_timings_json=? WHERE run_id=?",
        (utc_now_iso(), round(time.perf_counter() - t0, 4), sha256_file(input_path), res.rows_read,
         res.rows_blank_skipped, res.rows_valid, res.rows_valid_deprecated, res.rows_rejected,
         res.amount_cents_accepted, res.amount_cents_rejected, json.dumps(index.versions, sort_keys=True),
         res.notes, json.dumps(res.stage_seconds), run_id))
    conn.execute("COMMIT")
    return index.versions


def parse_pins_dict(pins):
    if pins is None:
        return {}
    if isinstance(pins, dict):
        return {k.lower(): int(v) for k, v in pins.items()}
    return parse_pins(pins)
````

### Checkpoint 4
```powershell
py -c "import codevalidator.pipeline; print('pipeline OK')"
```

---

## STEP 5: Report, data generator, CLI


#### codevalidator/report.py
Database se reconciliation checks dobara calculate karke Markdown report banata hai.

**File: `codevalidator/report.py`**

````python
"""Reconciliation checks and Markdown validation report (all recomputed from the database)."""
import json
import os
from dataclasses import dataclass

from .db import connect
from .util import read_csv
from .rules import REQUIRED_EVENT_COLUMNS
from .validate import parse_amount


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


def latest_run(conn, run_type="event_load", source_file=None):
    q = "SELECT * FROM pipeline_run WHERE run_type=? AND status='success'"
    args = [run_type]
    if source_file:
        q += " AND source_file=?"
        args.append(source_file)
    return conn.execute(q + " ORDER BY started_at DESC, rowid DESC LIMIT 1", args).fetchone()


def reconcile(conn, run_id, input_path=None):
    """Independent re-checks of a successful event_load run. Returns list[Check]."""
    run = conn.execute("SELECT * FROM pipeline_run WHERE run_id=?", (run_id,)).fetchone()
    if run is None:
        return [Check("run exists", False, run_id)]
    out = []
    accepted = run["rows_valid"] + run["rows_valid_deprecated"]
    out.append(Check("run counters: read = valid + deprecated + rejected",
                     run["rows_read"] == accepted + run["rows_rejected"],
                     f"{run['rows_read']} = {run['rows_valid']} + {run['rows_valid_deprecated']} + {run['rows_rejected']}"))
    newest = latest_run(conn, "event_load", run["source_file"])
    if newest and newest["run_id"] == run_id:
        a = conn.execute("SELECT COUNT(*) n, COALESCE(SUM(amount_cents),0) s FROM event_validated WHERE run_id=?", (run_id,)).fetchone()
        r = conn.execute("SELECT COUNT(*) n, COALESCE(SUM(amount_cents),0) s FROM event_reject WHERE run_id=?", (run_id,)).fetchone()
        out.append(Check("target accepted rows/amount match run counters",
                         (a["n"], a["s"]) == (accepted, run["amount_cents_accepted"]), f"db={a['n']}/{a['s']} run={accepted}/{run['amount_cents_accepted']}"))
        out.append(Check("target rejected rows/amount match run counters",
                         (r["n"], r["s"]) == (run["rows_rejected"], run["amount_cents_rejected"]), f"db={r['n']}/{r['s']} run={run['rows_rejected']}/{run['amount_cents_rejected']}"))
        out.append(Check("source rows = accepted + rejected in target", a["n"] + r["n"] == run["rows_read"],
                         f"{run['rows_read']} = {a['n']} + {r['n']}"))
        bad = conn.execute("SELECT COUNT(*) FROM event_reject WHERE run_id=? AND (source_file IS NULL OR source_line IS NULL OR raw_record IS NULL OR reject_id IS NULL)", (run_id,)).fetchone()[0]
        out.append(Check("every reject traceable (file, line, raw record, id)", bad == 0, f"{bad} untraceable"))
        both = conn.execute("SELECT COUNT(*) FROM event_reject r JOIN event_validated v ON v.event_id=r.event_id AND v.record_hash=r.record_hash AND v.source_file=r.source_file "
                            "WHERE r.run_id=? AND r.reason_code NOT IN ('DUPLICATE_EVENT')", (run_id,)).fetchone()[0]
        out.append(Check("no record is both accepted and rejected (same file)", both == 0, f"{both} overlaps"))
    else:
        out.append(Check("target-vs-run checks", True,
                         f"skipped: run superseded by {newest['run_id'] if newest else 'n/a'} for this file"))
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    out.append(Check("referential integrity (PRAGMA foreign_key_check)", len(fk) == 0, f"{len(fk)} violations"))
    orphan = conn.execute("SELECT COUNT(*) FROM event_validated v LEFT JOIN ref_code c ON c.ref_version_id=v.ref_version_id AND c.code=v.code "
                          "WHERE c.code IS NULL").fetchone()[0]
    out.append(Check("every accepted event's (ref version, code) exists in reference data", orphan == 0, f"{orphan} orphans"))
    if input_path and os.path.isfile(input_path):
        header, rows, blanks = read_csv(input_path)
        cols = [h.strip().lower() for h in header]
        idx = {c: cols.index(c) for c in REQUIRED_EVENT_COLUMNS if c in cols}
        total, parseable = 0, 0
        for _ln, f in rows:
            if len(f) == len(cols) and "amount" in idx:
                try:
                    total += parse_amount(f[idx["amount"]])
                    parseable += 1
                except ValueError:
                    pass
        out.append(Check("independent input recount = rows_read", len(rows) == run["rows_read"], f"file={len(rows)} run={run['rows_read']}"))
        tgt = run["amount_cents_accepted"] + run["amount_cents_rejected"]
        out.append(Check("amount: parseable input total = accepted + rejected", total == tgt,
                         f"input={total} target={tgt} cents ({parseable} parseable rows)"))
    return out


def _table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(x) for x in r) + " |" for r in rows]
    return "\n".join(lines)


def build_report(db_path, run_id=None, input_path=None):
    conn = connect(db_path)
    try:
        run = conn.execute("SELECT * FROM pipeline_run WHERE run_id=?", (run_id,)).fetchone() if run_id else latest_run(conn)
        if run is None:
            return "# Validation report\n\nNo successful event_load run found.\n"
        rid = run["run_id"]
        L = [f"# Validation report - run `{rid}`", "",
             f"Generated from database `{os.path.basename(db_path)}`. All numbers are recomputed from the database.", "",
             "## 1. Run summary", ""]
        keys = ["run_type", "source_file", "status", "started_at", "ended_at", "duration_seconds", "input_sha256",
                "rows_read", "rows_blank_skipped", "rows_valid", "rows_valid_deprecated", "rows_rejected",
                "amount_cents_accepted", "amount_cents_rejected", "ref_versions_json", "notes", "failure_reason"]
        L.append(_table(["field", "value"], [(k, run[k]) for k in keys]))
        L += ["", "## 2. Reconciliation checks", ""]
        checks = reconcile(conn, rid, input_path)
        L.append(_table(["check", "result", "detail"], [(c.name, "PASS" if c.passed else "**FAIL**", c.detail) for c in checks]))
        L += ["", f"Overall: **{'ALL PASS' if all(c.passed for c in checks) else 'FAILURES PRESENT'}**", "",
              "## 3. Rejects by reason (this run)", ""]
        L.append(_table(["reason_code", "rows"], [(r[0], r[1]) for r in conn.execute(
            "SELECT reason_code, row_count FROM run_reject_summary WHERE run_id=? ORDER BY row_count DESC, reason_code", (rid,))]) or "none")
        L += ["", "## 4. Accepted events by status and dataset (current target state)", ""]
        L.append(_table(["dataset_name", "validation_status", "events", "total_amount"], [
            (r[0], r[1], r[2], f"{r[3] / 100:,.2f}") for r in conn.execute(
                "SELECT dataset_name, validation_status, COUNT(*), SUM(amount_cents) FROM event_validated GROUP BY 1,2 ORDER BY 1,2")]))
        L += ["", "## 5. Deprecated-code usage with suggested replacement (data-quality warning)", ""]
        L.append(_table(["dataset_name", "code", "replaced_by_code", "events"], [tuple(r) for r in conn.execute(
            "SELECT dataset_name, code, COALESCE(replaced_by_code,''), COUNT(*) FROM event_validated "
            "WHERE validation_status='valid_deprecated' GROUP BY 1,2,3 ORDER BY 4 DESC, 2 LIMIT 10")]))
        L += ["", "## 6. Reference versions in the database", ""]
        L.append(_table(["ref_version_id", "dataset_name", "rows", "source_file", "loaded_at"], [tuple(r) for r in conn.execute(
            "SELECT ref_version_id, dataset_name, row_count, source_file, loaded_at FROM ref_version ORDER BY ref_version_id")]))
        L += ["", "## 7. Sample rejects (first 10, with source trace)", ""]
        L.append(_table(["source_file", "line", "event_id", "reason_code", "detail"], [tuple(r) for r in conn.execute(
            "SELECT source_file, source_line, COALESCE(event_id,''), reason_code, reason_detail FROM event_reject "
            "WHERE run_id=? ORDER BY source_line LIMIT 10", (rid,))]))
        L += ["", "## 8. Event history (changes preserved)", ""]
        L.append(_table(["event_id", "outcomes recorded"], [tuple(r) for r in conn.execute(
            "SELECT event_id, GROUP_CONCAT(outcome || '@v' || ref_version_id, ' -> ') FROM "
            "(SELECT * FROM event_history ORDER BY recorded_at, rowid) GROUP BY event_id HAVING COUNT(*) > 1 ORDER BY event_id LIMIT 10")]) or "none")
        L += ["", "## 9. Run history", ""]
        L.append(_table(["run_id", "type", "file", "status", "read", "valid", "deprecated", "rejected", "seconds"], [tuple(r) for r in conn.execute(
            "SELECT run_id, run_type, source_file, status, rows_read, rows_valid, rows_valid_deprecated, rows_rejected, duration_seconds "
            "FROM pipeline_run ORDER BY started_at, rowid")]))
        return "\n".join(L) + "\n"
    finally:
        conn.close()
````

#### codevalidator/datagen.py
Fixed seed wala synthetic data generator. Expected-answer labels **alag file** mein likhta hai.

**File: `codevalidator/datagen.py`**

````python
"""Deterministic synthetic data generator (fixed seed). Labels are written to a SEPARATE file.

Reference layout per dataset (50 codes, prefix D/P):
  idx  0-29  active                     [2020-01-01, open)
  idx 30-39  deprecated (has successor) [2020-01-01, 2026-01-01)
  idx 40-44  deprecated, then expired   [2020-01-01, 2024-06-01)
  idx 45-49  not yet effective          [2026-06-01, open)
Events fall in 2024-07-01..2025-12-31 so each category's expected outcome is known by construction,
independent of the pipeline code (the labels are an oracle, not a copy of the pipeline logic).
"""
import csv
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DATASETS = {"diagnosis_code": "D", "procedure_code": "P"}
SOURCES = ["ehr_a", "ehr_b", "lab_feed", "claims_api"]
EVENT_HEADER = ["event_id", "source_system", "event_ts", "updated_at", "dataset_name", "code", "amount"]
REF_HEADER = ["dataset_name", "code", "description", "status", "effective_from", "effective_to", "replaced_by_code"]
CATEGORIES = [  # (name, weight)
    ("valid", 58), ("valid_deprecated", 10), ("expired", 4), ("not_yet", 3), ("unknown_code", 3),
    ("unknown_dataset", 2), ("missing_field", 2), ("bad_ts", 2), ("bad_amount", 2), ("malformed", 2),
    ("boundary", 2), ("dup_exact", 5), ("superseded", 5),
]


def _code(prefix, i):
    return f"{prefix}{i:03d}"


def reference_rows(version=1):
    rows = []
    for ds, p in DATASETS.items():
        for i in range(50):
            c = _code(p, i)
            if i < 30:
                st, frm, to, rep = "active", "2020-01-01", "", ""
            elif i < 40:
                st, frm, to, rep = "deprecated", "2020-01-01", "2026-01-01", _code(p, i - 30)
            elif i < 45:
                st, frm, to, rep = "deprecated", "2020-01-01", "2024-06-01", _code(p, i - 40)
            else:
                st, frm, to, rep = "active", "2026-06-01", "", ""
            if version == 2 and 20 <= i < 25:        # refresh: active codes get retired in 2026
                st, to, rep = "deprecated", "2026-03-01", _code(p, i + 5)
            rows.append([ds, c, f"{ds} {c}", st, frm, to, rep])
        if version == 2:                              # refresh: new codes appear
            for i in range(50, 55):
                rows.append([ds, _code(p, i), f"{ds} {_code(p, i)} (new)", "active", "2025-01-01", "", ""])
    return rows


def _ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_events(n, seed=42):
    """Return (rows, labels). rows: list[list[str]] in file order; labels: list[(line, event_id, outcome)]."""
    rng = random.Random(seed)
    names = [c[0] for c in CATEGORIES]
    weights = [c[1] for c in CATEGORIES]
    start = datetime(2024, 7, 1, tzinfo=timezone.utc)
    span = int((datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc) - start).total_seconds())
    items = []   # dicts: fields, event_id, expected, kind, group
    counter = 0

    def base(counter):
        ds = rng.choice(list(DATASETS))
        ts = start + timedelta(seconds=rng.randint(0, span))
        upd = ts + timedelta(minutes=rng.randint(0, 2880))      # delivered late
        return {"eid": f"EVT-{counter:08d}", "ds": ds, "p": DATASETS[ds], "ts": ts, "upd": upd,
                "amt": f"{rng.randint(0, 500000) / 100:.2f}", "src": rng.choice(SOURCES)}

    def fields(b, code=None, ds=None):
        return [b["eid"], b["src"], _ts(b["ts"]), _ts(b["upd"]), ds or b["ds"], code or "", b["amt"]]

    while len(items) < n:
        counter += 1
        cat = rng.choices(names, weights)[0]
        b = base(counter)
        p = b["p"]
        if cat == "valid":
            items.append(dict(f=fields(b, _code(p, rng.randint(0, 29))), eid=b["eid"], exp="valid"))
        elif cat == "valid_deprecated":
            items.append(dict(f=fields(b, _code(p, rng.randint(30, 39))), eid=b["eid"], exp="valid_deprecated"))
        elif cat == "expired":
            items.append(dict(f=fields(b, _code(p, rng.randint(40, 44))), eid=b["eid"], exp="CODE_EXPIRED"))
        elif cat == "not_yet":
            items.append(dict(f=fields(b, _code(p, rng.randint(45, 49))), eid=b["eid"], exp="CODE_NOT_YET_EFFECTIVE"))
        elif cat == "unknown_code":
            items.append(dict(f=fields(b, f"{p}9{rng.randint(0, 99):02d}"), eid=b["eid"], exp="UNKNOWN_CODE"))
        elif cat == "unknown_dataset":
            items.append(dict(f=fields(b, _code(p, 1), ds="icd_legacy"), eid=b["eid"], exp="UNKNOWN_REFERENCE_DATASET"))
        elif cat == "missing_field":
            f = fields(b, _code(p, 1))
            f[rng.choice([0, 1, 2, 3, 4, 5, 6])] = ""
            items.append(dict(f=f, eid=f[0], exp="MISSING_REQUIRED_FIELD"))
        elif cat == "bad_ts":
            f = fields(b, _code(p, 1))
            f[rng.choice([2, 3])] = rng.choice(["not-a-date", "2025-13-45T00:00:00Z", "2025-03-10T09:00:00"])
            items.append(dict(f=f, eid=b["eid"], exp="INVALID_TIMESTAMP"))
        elif cat == "bad_amount":
            f = fields(b, _code(p, 1))
            f[6] = rng.choice(["abc", "-1.00", "1.234", "NaN"])
            items.append(dict(f=f, eid=b["eid"], exp="INVALID_AMOUNT"))
        elif cat == "malformed":
            f = fields(b, _code(p, 1))
            f = f[:-1] if rng.random() < 0.5 else f + ["extra"]
            items.append(dict(f=f, eid=b["eid"], exp="MALFORMED_RECORD"))
        elif cat == "boundary":     # effective_to is EXCLUSIVE: 2024-06-01 expired, 2024-05-31T23:59:59Z still valid
            code = _code(p, rng.randint(40, 44))
            if rng.random() < 0.5:
                b["ts"], exp = datetime(2024, 6, 1, tzinfo=timezone.utc), "CODE_EXPIRED"
            else:
                b["ts"], exp = datetime(2024, 5, 31, 23, 59, 59, tzinfo=timezone.utc), "valid_deprecated"
            b["upd"] = b["ts"] + timedelta(hours=1)
            items.append(dict(f=fields(b, code), eid=b["eid"], exp=exp))
        elif cat == "dup_exact":
            f = fields(b, _code(p, rng.randint(0, 29)))
            items.append(dict(f=f, eid=b["eid"], exp="valid", kind="dup", grp=b["eid"], upd=_ts(b["upd"])))
            items.append(dict(f=list(f), eid=b["eid"], exp="valid", kind="dup", grp=b["eid"], upd=_ts(b["upd"])))
        elif cat == "superseded":
            code = _code(p, rng.randint(0, 29))
            old = fields(b, code)
            b2 = dict(b, upd=b["upd"] + timedelta(hours=rng.randint(1, 48)), amt=f"{rng.randint(0, 500000) / 100:.2f}")
            new = fields(b2, code)
            items.append(dict(f=old, eid=b["eid"], exp="valid", kind="sup", grp=b["eid"], upd=old[3]))
            items.append(dict(f=new, eid=b["eid"], exp="valid", kind="sup", grp=b["eid"], upd=new[3]))
    items = items[:n]
    # a half-pair cut off at n would mislabel; drop incomplete duplicate/version groups
    cnt = defaultdict(int)
    for it in items:
        if it.get("grp"):
            cnt[it["grp"]] += 1
    items = [it for it in items if not it.get("grp") or cnt[it["grp"]] == 2]
    rng.shuffle(items)     # out-of-order delivery: newer versions may precede older ones
    groups = defaultdict(list)
    for pos, it in enumerate(items):
        it["line"] = pos + 2
        if it.get("grp"):
            groups[it["grp"]].append(it)
    for g in groups.values():
        if g[0]["kind"] == "dup":
            g.sort(key=lambda x: x["line"])
            g[1]["exp"] = "DUPLICATE_EVENT"          # first occurrence wins
        else:
            g.sort(key=lambda x: x["upd"])
            g[0]["exp"] = "SUPERSEDED_VERSION"       # newest updated_at wins regardless of position
    rows = [it["f"] for it in items]
    labels = [(it["line"], it["eid"], it["exp"]) for it in items]
    return rows, labels


def write_dataset(out_dir, n=1000, seed=42, prefix="events"):
    os.makedirs(out_dir, exist_ok=True)
    paths = {}
    for v in (1, 2):
        p = os.path.join(out_dir, f"reference_v{v}.csv")
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(REF_HEADER)
            w.writerows(reference_rows(v))
        paths[f"reference_v{v}"] = p
    rows, labels = generate_events(n, seed)
    ep = os.path.join(out_dir, f"{prefix}.csv")
    with open(ep, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(EVENT_HEADER)
        w.writerows(rows)
    lp = os.path.join(out_dir, f"{prefix}_expected_outcomes.csv")
    with open(lp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["source_line", "event_id", "expected_outcome"])
        w.writerows(labels)
    paths["events"], paths["labels"] = ep, lp
    return paths
````

#### codevalidator/cli.py
Commands: `init`, `load-ref`, `load-events`, `report`, `generate`, `demo`.

**File: `codevalidator/cli.py`**

````python
"""Command line interface.  python -m codevalidator <command> --help"""
import argparse
import os
import shutil
import sys

from . import datagen
from .db import connect
from .pipeline import load_events, parse_pins
from .reference import load_reference
from .report import build_report
from .util import PipelineError, setup_logging


def _print_result(res):
    print(f"run_id={res.run_id} status={res.status} file={res.source_file} read={res.rows_read} "
          f"valid={res.rows_valid} valid_deprecated={res.rows_valid_deprecated} rejected={res.rows_rejected} "
          f"seconds={res.duration_seconds}")
    if res.reject_breakdown:
        print("  rejects:", ", ".join(f"{k}={v}" for k, v in sorted(res.reject_breakdown.items())))
    if res.failure_reason:
        print("  failure_reason:", res.failure_reason)


def cmd_init(a):
    connect(a.db).close()
    print(f"initialised {a.db}")
    return 0


def cmd_load_ref(a):
    s = load_reference(a.db, a.file, a.run_id)
    print(f"run_id={s['run_id']} status={s['status']} datasets={s['datasets']}")
    if s["failure_reason"]:
        print("  failure_reason:", s["failure_reason"])
    return 0 if s["status"] == "success" else 2


def cmd_load_events(a):
    try:
        pins = parse_pins(a.pin)
    except PipelineError as e:
        print(e)
        return 2
    res = load_events(a.db, a.file, run_id=a.run_id, pins=pins, chunk_size=a.chunk_size)
    _print_result(res)
    return 0 if res.status == "success" else 2


def cmd_report(a):
    text = build_report(a.db, a.run_id, a.input)
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"report written to {a.out}")
    else:
        print(text)
    return 0


def cmd_generate(a):
    paths = datagen.write_dataset(a.out_dir, n=a.rows, seed=a.seed, prefix=a.prefix)
    for k, v in paths.items():
        print(f"{k}: {v}")
    return 0


def cmd_demo(a):
    """End-to-end walkthrough on the committed sample: load ref v1 -> events -> refresh ref v2 -> backfill rerun."""
    sample = a.sample_dir
    db = a.db
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(db + suffix):
            os.remove(db + suffix)
    events = os.path.join(sample, "events_sample.csv")
    print("== 1. load reference v1");
    load_reference(db, os.path.join(sample, "reference", "reference_v1.csv"))
    print("== 2. load events (reference v1)")
    _print_result(load_events(db, events))
    print("== 3. rerun the SAME file (idempotency check)")
    _print_result(load_events(db, events))
    print("== 4. reference refresh -> v2, then backfill by rerunning the file")
    load_reference(db, os.path.join(sample, "reference", "reference_v2.csv"))
    res = load_events(db, events)
    _print_result(res)
    text = build_report(db, res.run_id, events)
    os.makedirs(os.path.dirname(os.path.abspath(a.report)), exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"== report written to {a.report}")
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="codevalidator", description=__doc__)
    p.add_argument("--log-file", default=None, help="also write logs to this file")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init", help="create the SQLite database/schema"); s.add_argument("--db", required=True); s.set_defaults(fn=cmd_init)

    s = sub.add_parser("load-ref", help="load/refresh a reference snapshot CSV (idempotent)")
    s.add_argument("--db", required=True); s.add_argument("--file", required=True); s.add_argument("--run-id")
    s.set_defaults(fn=cmd_load_ref)

    s = sub.add_parser("load-events", help="validate and publish an events CSV (idempotent)")
    s.add_argument("--db", required=True); s.add_argument("--file", required=True); s.add_argument("--run-id")
    s.add_argument("--pin", action="append", help="pin reference version: dataset_name=version_id (repeatable)")
    s.add_argument("--chunk-size", type=int, default=10000)
    s.set_defaults(fn=cmd_load_events)

    s = sub.add_parser("report", help="reconciliation + data-quality report (Markdown)")
    s.add_argument("--db", required=True); s.add_argument("--run-id"); s.add_argument("--input", help="original events file for independent recount")
    s.add_argument("--out"); s.set_defaults(fn=cmd_report)

    s = sub.add_parser("generate", help="generate synthetic reference + events + labels (fixed seed)")
    s.add_argument("--out-dir", required=True); s.add_argument("--rows", type=int, default=100000)
    s.add_argument("--seed", type=int, default=42); s.add_argument("--prefix", default="events"); s.set_defaults(fn=cmd_generate)

    s = sub.add_parser("demo", help="run the end-to-end demo on the committed sample data")
    s.add_argument("--sample-dir", default="data/sample"); s.add_argument("--db", default="output/demo.db")
    s.add_argument("--report", default="output/validation_report_sample.md"); s.set_defaults(fn=cmd_demo)
    return p


def main(argv=None):
    a = build_parser().parse_args(argv)
    import logging
    setup_logging(logging.DEBUG if a.verbose else logging.INFO, a.log_file)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
````

### Checkpoint 5
```powershell
py -m codevalidator --help
```
Commands ki list dikhni chahiye.

---

## STEP 6: Sample data (hand-written, chhota)

Ye 24-row ka sample har edge case cover karta hai. Test inhi files ke against chalte hain, isliye content **bilkul same** hona chahiye.

#### data/sample/reference/reference_v1.csv

**File: `data/sample/reference/reference_v1.csv`**

````csv
dataset_name,code,description,status,effective_from,effective_to,replaced_by_code
diagnosis_code,D100,Hypertension,active,2020-01-01,,
diagnosis_code,D200,Type 2 diabetes,active,2020-01-01,,
diagnosis_code,D150,Legacy asthma code,deprecated,2020-01-01,2025-01-01,D250
diagnosis_code,D250,Asthma,active,2024-07-01,,
diagnosis_code,D300,Legacy code retired mid-2024,deprecated,2020-01-01,2024-06-01,D100
diagnosis_code,D900,Future code,active,2026-06-01,,
procedure_code,P100,Blood test,active,2020-01-01,,
procedure_code,P200,X-ray,active,2020-01-01,,
````

#### data/sample/reference/reference_v2.csv

**File: `data/sample/reference/reference_v2.csv`**

````csv
dataset_name,code,description,status,effective_from,effective_to,replaced_by_code
diagnosis_code,D100,Hypertension,active,2020-01-01,,
diagnosis_code,D200,Type 2 diabetes,deprecated,2020-01-01,2026-01-01,D210
diagnosis_code,D210,Type 2 diabetes (revised),active,2025-06-01,,
diagnosis_code,D150,Legacy asthma code,deprecated,2020-01-01,2025-01-01,D250
diagnosis_code,D250,Asthma,active,2024-07-01,,
diagnosis_code,D300,Legacy code retired mid-2024,deprecated,2020-01-01,2024-06-01,D100
diagnosis_code,D900,Future code,active,2026-06-01,,
procedure_code,P100,Blood test,active,2020-01-01,,
procedure_code,P200,X-ray,deprecated,2020-01-01,2025-03-01,P300
procedure_code,P300,X-ray (digital),active,2025-01-01,,
````

#### data/sample/events_sample.csv

**File: `data/sample/events_sample.csv`**

````csv
event_id,source_system,event_ts,updated_at,dataset_name,code,amount
E001,ehr,2025-03-10T09:00:00Z,2025-03-10T09:00:00Z,diagnosis_code,D100,100.00
E002,ehr,2025-03-10T10:00:00Z,2025-03-10T10:00:00Z,diagnosis_code,D200,250.50
E003,ehr,2024-12-31T23:59:59Z,2024-12-31T23:59:59Z,diagnosis_code,D150,80.00
E004,ehr,2025-01-01T00:00:00Z,2025-01-01T00:00:00Z,diagnosis_code,D150,80.00
E005,ehr,2024-05-31T12:00:00Z,2024-05-31T12:00:00Z,diagnosis_code,D300,10.00
E006,ehr,2024-06-01T00:00:00Z,2024-06-01T00:00:00Z,diagnosis_code,D300,10.00
E007,ehr,2025-03-10T09:00:00Z,2025-03-10T09:00:00Z,diagnosis_code,D900,5.00
E008,ehr,2025-03-10T09:00:00Z,2025-03-10T09:00:00Z,diagnosis_code,D777,5.00
E009,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,procedure_code,P100,30.00
E010,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,procedure_code,p200 ,45.00
E011,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,lab_code,P100,30.00
E012,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,procedure_code,,30.00
E013,lab,not-a-date,2025-03-11T09:00:00Z,procedure_code,P100,30.00
E014,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,procedure_code,P100,abc
E015,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,procedure_code,P100,-5.00
E016,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,procedure_code,P100
E001,ehr,2025-03-10T09:00:00Z,2025-03-10T09:00:00Z,diagnosis_code,D100,100.00
E002,ehr,2025-03-10T10:00:00Z,2025-03-12T08:00:00Z,diagnosis_code,D200,260.00
E017,ehr,2025-01-05T08:00:00Z,2025-03-10T08:00:00Z,diagnosis_code,D100,12.00
E018,ehr,2025-03-10T09:00:00+05:30,2025-03-10T09:00:00+05:30,diagnosis_code,D100,1.50
E019,ehr,2025-03-10T09:00:00,2025-03-10T09:00:00,diagnosis_code,D100,1.00
E020,lab,2025-03-11T09:00:00Z,2025-03-11T09:00:00Z,procedure_code,P100,1.234
E021,ehr,2025-07-01T09:00:00Z,2025-07-01T09:00:00Z,diagnosis_code,D250,70.00
E022,ehr,2025-07-01T09:00:00Z,2025-07-01T09:00:00Z,diagnosis_code,D200,9.99
````

#### data/sample/expected/events_sample_expected_ref_v1.csv

**File: `data/sample/expected/events_sample_expected_ref_v1.csv`**

````csv
source_line,event_id,expected_outcome
2,E001,valid
3,E002,SUPERSEDED_VERSION
4,E003,valid_deprecated
5,E004,CODE_EXPIRED
6,E005,valid_deprecated
7,E006,CODE_EXPIRED
8,E007,CODE_NOT_YET_EFFECTIVE
9,E008,UNKNOWN_CODE
10,E009,valid
11,E010,valid
12,E011,UNKNOWN_REFERENCE_DATASET
13,E012,MISSING_REQUIRED_FIELD
14,E013,INVALID_TIMESTAMP
15,E014,INVALID_AMOUNT
16,E015,INVALID_AMOUNT
17,E016,MALFORMED_RECORD
18,E001,DUPLICATE_EVENT
19,E002,valid
20,E017,valid
21,E018,valid
22,E019,INVALID_TIMESTAMP
23,E020,INVALID_AMOUNT
24,E021,valid
25,E022,valid
````

#### data/sample/expected/events_sample_expected_ref_v2.csv

**File: `data/sample/expected/events_sample_expected_ref_v2.csv`**

````csv
source_line,event_id,expected_outcome
2,E001,valid
3,E002,SUPERSEDED_VERSION
4,E003,valid_deprecated
5,E004,CODE_EXPIRED
6,E005,valid_deprecated
7,E006,CODE_EXPIRED
8,E007,CODE_NOT_YET_EFFECTIVE
9,E008,UNKNOWN_CODE
10,E009,valid
11,E010,CODE_EXPIRED
12,E011,UNKNOWN_REFERENCE_DATASET
13,E012,MISSING_REQUIRED_FIELD
14,E013,INVALID_TIMESTAMP
15,E014,INVALID_AMOUNT
16,E015,INVALID_AMOUNT
17,E016,MALFORMED_RECORD
18,E001,DUPLICATE_EVENT
19,E002,valid_deprecated
20,E017,valid
21,E018,valid
22,E019,INVALID_TIMESTAMP
23,E020,INVALID_AMOUNT
24,E021,valid
25,E022,valid_deprecated
````

> `events_sample.csv` mein line 17 (`E016,...,procedure_code,P100`) mein **jaan-boojh ke ek column kam hai** (malformed row test). Use "fix" mat karna.

### Checkpoint 6: pehla real run
```powershell
py -m codevalidator demo
```
Aisa output aana chahiye:
```text
== 2. load events (reference v1)
run_id=evt_... status=success ... read=24 valid=8 valid_deprecated=2 rejected=14
== 4. reference refresh -> v2, then backfill by rerunning the file
run_id=evt_... status=success ... read=24 valid=5 valid_deprecated=4 rejected=15
== report written to output\validation_report_sample.md
```
`output\validation_report_sample.md` kholo: usme "Overall: ALL PASS" hona chahiye.

---

## STEP 7: Tests


#### tests/__init__.py
Tests ke dauran log noise band karta hai.

**File: `tests/__init__.py`**

````python
import logging

logging.disable(logging.CRITICAL)   # keep test output readable; failures are asserted via RunResult/DB
````

#### tests/helpers.py
Shared test utilities (temp DB, labels reader, state fingerprint).

**File: `tests/helpers.py`**

````python
import csv
import os
import shutil
import sqlite3
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE = os.path.join(ROOT, "data", "sample")
EVENT_HEADER = "event_id,source_system,event_ts,updated_at,dataset_name,code,amount"
REF_HEADER = "dataset_name,code,description,status,effective_from,effective_to,replaced_by_code"


class TmpCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cv_test_")
        self.db = os.path.join(self.tmp, "t.db")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def write(self, name, text):
        p = os.path.join(self.tmp, name)
        with open(p, "w", encoding="utf-8", newline="") as f:
            f.write(text)
        return p

    def q(self, sql, args=()):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(sql, args).fetchall()
        finally:
            conn.close()

    def scalar(self, sql, args=()):
        return self.q(sql, args)[0][0]

    def outcomes(self, source_file):
        """{source_line: outcome} reconstructed from target tables (validated + rejected)."""
        out = {}
        for r in self.q("SELECT source_line, validation_status s FROM event_validated WHERE source_file=?", (source_file,)):
            out[r["source_line"]] = r["s"]
        for r in self.q("SELECT source_line, reason_code s FROM event_reject WHERE source_file=?", (source_file,)):
            assert r["source_line"] not in out, "line both accepted and rejected"
            out[r["source_line"]] = r["s"]
        return out

    def state(self):
        """Deterministic fingerprint of target tables (excludes run ids/timestamps)."""
        v = self.q("SELECT event_id, validation_status, code, amount_cents, ref_version_id, record_hash, source_line "
                   "FROM event_validated ORDER BY event_id")
        r = self.q("SELECT source_line, reason_code, event_id, record_hash FROM event_reject ORDER BY source_line")
        return [tuple(x) for x in v], [tuple(x) for x in r]


def read_labels(path):
    with open(path, newline="", encoding="utf-8") as f:
        return {int(r["source_line"]): r["expected_outcome"] for r in csv.DictReader(f)}


REF_BASIC = REF_HEADER + "\n" + "\n".join([
    "diagnosis_code,D100,Active code,active,2020-01-01,,",
    "diagnosis_code,D150,Deprecated code,deprecated,2020-01-01,2025-01-01,D100",
    "diagnosis_code,D900,Future code,active,2026-06-01,,",
]) + "\n"


def ev(eid="E1", src="s", ts="2025-03-10T09:00:00Z", upd="2025-03-10T09:00:00Z", ds="diagnosis_code", code="D100", amt="10.00"):
    return ",".join([eid, src, ts, upd, ds, code, amt])
````

#### tests/test_validators.py
Unit tests: timestamp, amount, effective-date boundaries, deprecated, gaps.

**File: `tests/test_validators.py`**

````python
import unittest
from datetime import date

from codevalidator import rules
from codevalidator.reference import RefIndex, Window
from codevalidator.validate import RecordError, parse_amount, parse_event, parse_ts

IDX = {c: i for i, c in enumerate(rules.REQUIRED_EVENT_COLUMNS)}


def row(**kw):
    base = dict(event_id="E1", source_system="s", event_ts="2025-03-10T09:00:00Z", updated_at="2025-03-10T09:00:00Z",
                dataset_name="diagnosis_code", code="d100 ", amount="12.50")
    base.update(kw)
    return [base[c] for c in rules.REQUIRED_EVENT_COLUMNS]


class FieldTests(unittest.TestCase):
    def test_happy_path_and_normalisation(self):
        e = parse_event(2, row(), 7, IDX)
        self.assertEqual((e.code, e.amount_cents, e.event_ts), ("D100", 1250, "2025-03-10T09:00:00Z"))

    def test_offset_normalised_to_utc_and_date_taken_in_utc(self):
        e = parse_event(2, row(event_ts="2025-03-10T01:00:00+05:30"), 7, IDX)
        self.assertEqual(e.event_ts, "2025-03-09T19:30:00Z")
        self.assertEqual(e.effective_date, date(2025, 3, 9))   # UTC date, not local date

    def test_timestamp_rules(self):
        for bad in ["not-a-date", "2025-13-01T00:00:00Z", "2025-03-10T09:00:00", "2025-03-10"]:
            with self.assertRaises(ValueError, msg=bad):
                parse_ts(bad)

    def test_amount_rules(self):
        self.assertEqual(parse_amount("0"), 0)
        self.assertEqual(parse_amount("7"), 700)
        self.assertEqual(parse_amount("0.10"), 10)
        for bad in ["abc", "-0.01", "1.234", "NaN", "Infinity", ""]:
            with self.assertRaises(ValueError, msg=bad):
                parse_amount(bad)

    def test_reasons_and_precedence(self):
        cases = [
            (row()[:-1], rules.MALFORMED_RECORD),
            (row(code="  "), rules.MISSING_REQUIRED_FIELD),
            (row(event_ts="x", amount="bad"), rules.INVALID_TIMESTAMP),      # timestamp before amount
            (row(amount="-1"), rules.INVALID_AMOUNT),
        ]
        for fields, reason in cases:
            with self.assertRaises(RecordError) as cm:
                parse_event(2, fields, 7, IDX)
            self.assertEqual(cm.exception.reason, reason)


class WindowTests(unittest.TestCase):
    """Domain edge cases: effective-date boundaries, deprecation, gaps."""

    def setUp(self):
        W = lambda f, t, s, r=None: Window(date.fromisoformat(f), date.fromisoformat(t) if t else None, s, r, 1)
        self.idx = RefIndex({
            ("d", "A"): [W("2020-01-01", None, "active")],
            ("d", "DEP"): [W("2020-01-01", "2025-01-01", "deprecated", "A")],
            ("d", "FUT"): [W("2026-06-01", None, "active")],
            ("d", "GAP"): [W("2020-01-01", "2021-01-01", "active"), W("2022-01-01", None, "active")],
        }, {"d": 1})

    def test_effective_from_inclusive_effective_to_exclusive(self):
        c = self.idx.classify
        self.assertEqual(c("d", "FUT", date(2026, 5, 31)).reason_code, rules.CODE_NOT_YET_EFFECTIVE)
        self.assertEqual(c("d", "FUT", date(2026, 6, 1)).status, rules.VALID)
        self.assertEqual(c("d", "DEP", date(2024, 12, 31)).status, rules.VALID_DEPRECATED)
        self.assertEqual(c("d", "DEP", date(2025, 1, 1)).reason_code, rules.CODE_EXPIRED)

    def test_deprecated_returns_replacement(self):
        self.assertEqual(self.idx.classify("d", "DEP", date(2024, 1, 1)).replaced_by, "A")

    def test_gap_between_windows_is_expired_and_reactivation_valid(self):
        self.assertEqual(self.idx.classify("d", "GAP", date(2021, 6, 1)).reason_code, rules.CODE_EXPIRED)
        self.assertEqual(self.idx.classify("d", "GAP", date(2022, 1, 1)).status, rules.VALID)

    def test_unknown_code_and_dataset(self):
        self.assertEqual(self.idx.classify("d", "ZZZ", date(2025, 1, 1)).reason_code, rules.UNKNOWN_CODE)
        self.assertEqual(self.idx.classify("x", "A", date(2025, 1, 1)).reason_code, rules.UNKNOWN_REFERENCE_DATASET)


if __name__ == "__main__":
    unittest.main()
````

#### tests/test_reference_load.py
Reference load: idempotent reload, refresh, invalid snapshots.

**File: `tests/test_reference_load.py`**

````python
import unittest

from codevalidator.reference import load_reference
from tests.helpers import REF_BASIC, REF_HEADER, TmpCase


class ReferenceLoadTests(TmpCase):
    def test_happy_path_creates_version(self):
        s = load_reference(self.db, self.write("r.csv", REF_BASIC))
        self.assertEqual(s["status"], "success")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_version"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_code"), 3)

    def test_reload_same_content_is_noop(self):
        p = self.write("r.csv", REF_BASIC)
        load_reference(self.db, p)
        s = load_reference(self.db, p)
        self.assertFalse(list(s["datasets"].values())[0]["created"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_version"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_code"), 3)

    def test_refresh_appends_new_version_and_keeps_old(self):
        load_reference(self.db, self.write("r1.csv", REF_BASIC))
        v2 = REF_BASIC + "diagnosis_code,D200,New code,active,2025-01-01,,\n"
        s = load_reference(self.db, self.write("r2.csv", v2))
        self.assertTrue(s["datasets"]["diagnosis_code"]["created"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_code WHERE ref_version_id=1"), 3)   # v1 untouched
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_code WHERE ref_version_id=2"), 4)

    def test_invalid_reference_is_rejected_atomically(self):
        bad_cases = {
            "bad status": "diagnosis_code,X1,d,retired,2020-01-01,,",
            "bad date": "diagnosis_code,X1,d,active,2020-13-01,,",
            "to before from": "diagnosis_code,X1,d,deprecated,2020-01-01,2019-01-01,",
            "dangling replacement": "diagnosis_code,X1,d,deprecated,2020-01-01,2021-01-01,NOPE",
            "overlap": "diagnosis_code,D100,d,active,2022-01-01,,",
            "duplicate key": "diagnosis_code,D100,d,active,2020-01-01,,",
            "wrong field count": "diagnosis_code,X1",
        }
        for name, line in bad_cases.items():
            with self.subTest(name):
                s = load_reference(self.db, self.write("bad.csv", REF_BASIC + line + "\n"))
                self.assertEqual(s["status"], "failed", name)
                self.assertIn("REFERENCE_VALIDATION_FAILED", s["failure_reason"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_version"), 0)       # nothing published
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM pipeline_run WHERE status='failed'"), len(bad_cases))

    def test_missing_column_empty_file_and_header_only(self):
        s = load_reference(self.db, self.write("a.csv", "dataset_name,code\nx,y\n"))
        self.assertIn("SCHEMA_MISSING_COLUMNS", s["failure_reason"])
        s = load_reference(self.db, self.write("b.csv", ""))
        self.assertIn("EMPTY_FILE", s["failure_reason"])
        s = load_reference(self.db, self.write("c.csv", REF_HEADER + "\n"))
        self.assertIn("EMPTY_REFERENCE", s["failure_reason"])


if __name__ == "__main__":
    unittest.main()
````

#### tests/test_event_pipeline.py
Pipeline scenarios: empty, malformed, duplicates, late data, rerun, partial failure + retry, backfill.

**File: `tests/test_event_pipeline.py`**

````python
import os
import unittest

from codevalidator.pipeline import load_events
from codevalidator.reference import load_reference
from codevalidator.report import reconcile
from codevalidator.db import connect
from tests.helpers import EVENT_HEADER, REF_BASIC, SAMPLE, TmpCase, ev, read_labels


class EventPipelineTests(TmpCase):
    def setUp(self):
        super().setUp()
        load_reference(self.db, self.write("ref.csv", REF_BASIC))

    def run_text(self, text, name="in.csv", **kw):
        return load_events(self.db, self.write(name, text), **kw)

    # ---- happy path / counts ------------------------------------------------------
    def test_happy_path(self):
        r = self.run_text(EVENT_HEADER + "\n" + ev("E1") + "\n" + ev("E2", code="D150", ts="2024-06-01T00:00:00Z") + "\n")
        self.assertEqual((r.status, r.rows_read, r.rows_valid, r.rows_valid_deprecated, r.rows_rejected), ("success", 2, 1, 1, 0))
        row = self.q("SELECT * FROM event_validated WHERE event_id='E2'")[0]
        self.assertEqual((row["validation_status"], row["warning_code"], row["replaced_by_code"]), ("valid_deprecated", "CODE_DEPRECATED", "D100"))
        run = self.q("SELECT * FROM pipeline_run WHERE run_id=?", (r.run_id,))[0]
        self.assertEqual((run["status"], run["reconciled"], run["rows_read"]), ("success", 1, 2))
        self.assertTrue(run["started_at"].endswith("Z") and run["ended_at"].endswith("Z"))

    # ---- empty / schema -----------------------------------------------------------
    def test_header_only_batch_succeeds_with_zero_rows(self):
        r = self.run_text(EVENT_HEADER + "\n")
        self.assertEqual((r.status, r.rows_read, r.rows_rejected), ("success", 0, 0))

    def test_zero_byte_file_fails_and_is_recorded(self):
        r = self.run_text("")
        self.assertEqual(r.status, "failed")
        self.assertIn("EMPTY_FILE", r.failure_reason)
        self.assertEqual(self.scalar("SELECT failure_reason FROM pipeline_run WHERE run_id=?", (r.run_id,)), r.failure_reason)

    def test_missing_input_file_fails(self):
        r = load_events(self.db, os.path.join(self.tmp, "nope.csv"))
        self.assertEqual(r.status, "failed")
        self.assertIn("INPUT_NOT_FOUND", r.failure_reason)

    def test_schema_change_missing_column_fails_whole_file_publishes_nothing(self):
        r = self.run_text("event_id,source_system,event_ts,dataset_name,code,amount\nE1,s,2025-03-10T09:00:00Z,diagnosis_code,D100,1\n")
        self.assertEqual(r.status, "failed")
        self.assertIn("SCHEMA_MISSING_COLUMNS", r.failure_reason)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_validated"), 0)

    def test_schema_change_extra_column_is_tolerated_and_noted(self):
        r = self.run_text(EVENT_HEADER + ",channel\n" + ev("E1") + ",web\n")
        self.assertEqual((r.status, r.rows_valid), ("success", 1))
        self.assertIn("schema_drift", self.scalar("SELECT notes FROM pipeline_run WHERE run_id=?", (r.run_id,)))

    def test_header_case_and_column_order_do_not_matter(self):
        r = self.run_text("AMOUNT,Code,dataset_name,updated_at,event_ts,source_system,event_id\n"
                          "5.00,d100,diagnosis_code,2025-03-10T09:00:00Z,2025-03-10T09:00:00Z,s,E1\n")
        self.assertEqual((r.status, r.rows_valid), ("success", 1))

    def test_blank_lines_counted_not_lost(self):
        r = self.run_text(EVENT_HEADER + "\n\n" + ev("E1") + "\n\n")
        self.assertEqual((r.rows_read, r.rows_blank_skipped), (1, 2))

    # ---- malformed / nulls / duplicates -------------------------------------------
    def test_malformed_and_null_records_quarantined_with_trace(self):
        r = self.run_text(EVENT_HEADER + "\n" + ev("E1") + "\n" + "E2,s,2025-03-10T09:00:00Z\n" +
                          ev("E3", code="") + "\n" + ev("", code="D100") + "\n")
        self.assertEqual((r.rows_valid, r.rows_rejected), (1, 3))
        rej = {x["source_line"]: x for x in self.q("SELECT * FROM event_reject")}
        self.assertEqual(rej[3]["reason_code"], "MALFORMED_RECORD")
        self.assertEqual(rej[4]["reason_code"], "MISSING_REQUIRED_FIELD")
        self.assertEqual(rej[5]["reason_code"], "MISSING_REQUIRED_FIELD")
        self.assertIsNone(rej[5]["event_id"])
        self.assertEqual(rej[3]["source_file"], "in.csv")
        self.assertIn("E2", rej[3]["raw_record"])

    def test_exact_duplicate_keeps_first_and_rejects_second(self):
        r = self.run_text(EVENT_HEADER + "\n" + ev("E1") + "\n" + ev("E1") + "\n")
        self.assertEqual((r.rows_valid, r.reject_breakdown), (1, {"DUPLICATE_EVENT": 1}))
        self.assertEqual(self.scalar("SELECT source_line FROM event_validated WHERE event_id='E1'"), 2)

    def test_out_of_order_versions_in_file_newest_updated_at_wins(self):
        newer = ev("E1", upd="2025-03-12T00:00:00Z", amt="99.00")
        older = ev("E1", upd="2025-03-11T00:00:00Z", amt="50.00")
        r = self.run_text(EVENT_HEADER + "\n" + newer + "\n" + older + "\n")     # newest arrives FIRST
        self.assertEqual(r.reject_breakdown, {"SUPERSEDED_VERSION": 1})
        self.assertEqual(self.scalar("SELECT amount_cents FROM event_validated WHERE event_id='E1'"), 9900)

    # ---- late-arriving data across files ------------------------------------------
    def test_late_older_version_in_later_file_is_stale_and_ignored(self):
        self.run_text(EVENT_HEADER + "\n" + ev("E1", upd="2025-03-12T00:00:00Z", amt="99.00") + "\n", name="a.csv")
        r = self.run_text(EVENT_HEADER + "\n" + ev("E1", upd="2025-03-11T00:00:00Z", amt="50.00") + "\n", name="b.csv")
        self.assertEqual(r.reject_breakdown, {"STALE_VERSION": 1})
        self.assertEqual(self.scalar("SELECT amount_cents FROM event_validated WHERE event_id='E1'"), 9900)

    def test_newer_version_in_later_file_updates_and_history_is_kept(self):
        self.run_text(EVENT_HEADER + "\n" + ev("E1", upd="2025-03-11T00:00:00Z", amt="50.00") + "\n", name="a.csv")
        self.run_text(EVENT_HEADER + "\n" + ev("E1", upd="2025-03-12T00:00:00Z", amt="99.00") + "\n", name="b.csv")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_validated"), 1)
        self.assertEqual(self.scalar("SELECT amount_cents FROM event_validated WHERE event_id='E1'"), 9900)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_history WHERE event_id='E1' AND outcome='valid'"), 2)

    # ---- domain edge cases ----------------------------------------------------------
    def test_effective_date_boundaries_at_load_level(self):
        rows = [ev("A", ts="2024-12-31T23:59:59Z", code="D150"),   # last second of window -> deprecated warning
                ev("B", ts="2025-01-01T00:00:00Z", code="D150"),   # window end (exclusive) -> expired
                ev("C", ts="2026-05-31T23:59:59Z", code="D900"),   # before effective_from -> not yet
                ev("D", ts="2026-06-01T00:00:00Z", code="D900")]   # exactly effective_from -> valid
        r = self.run_text(EVENT_HEADER + "\n" + "\n".join(rows) + "\n")
        self.assertEqual(r.reject_breakdown, {"CODE_EXPIRED": 1, "CODE_NOT_YET_EFFECTIVE": 1})
        self.assertEqual((r.rows_valid, r.rows_valid_deprecated), (1, 1))

    def test_invalid_reference_key(self):
        r = self.run_text(EVENT_HEADER + "\n" + ev("E1", ds="nope") + "\n" + ev("E2", code="ZZZ") + "\n")
        self.assertEqual(r.reject_breakdown, {"UNKNOWN_REFERENCE_DATASET": 1, "UNKNOWN_CODE": 1})

    def test_reference_refresh_backfill_revalidates_and_preserves_history(self):
        path = self.write("e.csv", EVENT_HEADER + "\n" + ev("E1", code="D200", ts="2025-03-10T09:00:00Z") + "\n")
        r1 = load_events(self.db, path)
        self.assertEqual(r1.reject_breakdown, {"UNKNOWN_CODE": 1})
        load_reference(self.db, self.write("r2.csv", REF_BASIC + "diagnosis_code,D200,Added later,active,2020-01-01,,\n"))
        r2 = load_events(self.db, path)                                     # backfill: same file, new reference
        self.assertEqual((r2.rows_valid, r2.rows_rejected), (1, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_reject"), 0)   # stale reject removed
        outcomes = [x[0] for x in self.q("SELECT outcome FROM event_history WHERE event_id='E1' ORDER BY rowid")]
        self.assertEqual(outcomes, ["rejected:UNKNOWN_CODE", "valid"])

    def test_pinned_reference_version_reproduces_old_result(self):
        path = self.write("e.csv", EVENT_HEADER + "\n" + ev("E1", code="D200") + "\n")
        load_reference(self.db, self.write("r2.csv", REF_BASIC + "diagnosis_code,D200,Added,active,2020-01-01,,\n"))
        self.assertEqual(load_events(self.db, path, pins={"diagnosis_code": 1}).reject_breakdown, {"UNKNOWN_CODE": 1})
        self.assertEqual(load_events(self.db, path).rows_valid, 1)
        bad = load_events(self.db, path, pins={"diagnosis_code": 99})
        self.assertIn("INVALID_PIN", bad.failure_reason)

    def test_valid_event_becoming_invalid_after_refresh_is_removed_not_left_stale(self):
        path = self.write("e.csv", EVENT_HEADER + "\n" + ev("E1", code="D100") + "\n")
        load_events(self.db, path)
        shrunk = REF_BASIC.replace("diagnosis_code,D100,Active code,active,2020-01-01,,", "diagnosis_code,D100,Active code,deprecated,2020-01-01,2025-01-01,D150") \
                          .replace("diagnosis_code,D150,Deprecated code,deprecated,2020-01-01,2025-01-01,D100", "diagnosis_code,D150,Now active,active,2020-01-01,,")
        self.assertEqual(load_reference(self.db, self.write("r2.csv", shrunk))["status"], "success")
        r = load_events(self.db, path)
        self.assertEqual(r.reject_breakdown, {"CODE_EXPIRED": 1})
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_validated"), 0)

    # ---- idempotency / recovery -----------------------------------------------------
    def test_rerun_same_input_creates_no_duplicates_and_same_state(self):
        text = (EVENT_HEADER + "\n" + ev("E1") + "\n" + ev("E1") + "\n" + ev("E2", code="ZZZ") + "\n" +
                ev("E3", code="D150") + "\n" + "bad,row\n")
        path = self.write("e.csv", text)
        load_events(self.db, path)
        s1 = self.state()
        hist1 = self.scalar("SELECT COUNT(*) FROM event_history")
        for _ in range(3):
            self.assertEqual(load_events(self.db, path).status, "success")
        self.assertEqual(self.state(), s1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_history"), hist1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM pipeline_run WHERE run_type='event_load'"), 4)   # runs are logged, data is not duplicated

    def test_partial_failure_rolls_back_everything_and_retry_succeeds(self):
        rows = [ev(f"E{i}") for i in range(10)] + [ev("BAD", code="ZZZ")]
        path = self.write("e.csv", EVENT_HEADER + "\n" + "\n".join(rows) + "\n")
        r = load_events(self.db, path, chunk_size=3, fail_after_rows=6)       # crash after 2 chunks were executed
        self.assertEqual(r.status, "failed")
        self.assertIn("injected failure", r.failure_reason)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_validated"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_reject"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_history"), 0)
        self.assertEqual(self.scalar("SELECT status FROM pipeline_run WHERE run_id=?", (r.run_id,)), "failed")
        retry = load_events(self.db, path, chunk_size=3)
        self.assertEqual((retry.status, retry.rows_valid, retry.rows_rejected), ("success", 10, 1))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM event_validated"), 10)

    def test_unreadable_encoding_fails_cleanly(self):
        p = os.path.join(self.tmp, "bin.csv")
        with open(p, "wb") as f:
            f.write(b"\xff\xfe\x00bad")
        r = load_events(self.db, p)
        self.assertEqual(r.status, "failed")

    # ---- sample dataset vs independent labels --------------------------------------
    def test_sample_matches_labels_for_ref_v1_and_v2(self):
        events = os.path.join(SAMPLE, "events_sample.csv")
        db = os.path.join(self.tmp, "s.db")
        self.db = db
        load_reference(db, os.path.join(SAMPLE, "reference", "reference_v1.csv"))
        r = load_events(db, events)
        self.assertEqual(self.outcomes("events_sample.csv"), read_labels(os.path.join(SAMPLE, "expected", "events_sample_expected_ref_v1.csv")))
        self.assertEqual((r.rows_valid, r.rows_valid_deprecated, r.rows_rejected), (8, 2, 14))
        load_reference(db, os.path.join(SAMPLE, "reference", "reference_v2.csv"))
        r = load_events(db, events)
        self.assertEqual(self.outcomes("events_sample.csv"), read_labels(os.path.join(SAMPLE, "expected", "events_sample_expected_ref_v2.csv")))
        self.assertEqual((r.rows_valid, r.rows_valid_deprecated, r.rows_rejected), (5, 4, 15))
        conn = connect(db)
        checks = reconcile(conn, r.run_id, events)
        conn.close()
        self.assertTrue(all(c.passed for c in checks), [c for c in checks if not c.passed])


if __name__ == "__main__":
    unittest.main()
````

#### tests/test_scale_and_cli.py
20,000-row run vs independent labels, generator determinism, CLI exit codes.

**File: `tests/test_scale_and_cli.py`**

````python
import filecmp
import os
import unittest

from codevalidator import cli, datagen
from codevalidator.db import connect
from codevalidator.pipeline import load_events
from codevalidator.reference import load_reference
from codevalidator.report import reconcile
from tests.helpers import SAMPLE, TmpCase, read_labels

N = 20000


class ScaleTests(TmpCase):
    def test_generator_is_deterministic_and_labels_separate(self):
        a = datagen.write_dataset(os.path.join(self.tmp, "a"), n=500, seed=7)
        b = datagen.write_dataset(os.path.join(self.tmp, "b"), n=500, seed=7)
        c = datagen.write_dataset(os.path.join(self.tmp, "c"), n=500, seed=8)
        for k in a:
            self.assertTrue(filecmp.cmp(a[k], b[k], shallow=False), k)
        self.assertFalse(filecmp.cmp(a["events"], c["events"], shallow=False))
        with open(a["events"], encoding="utf-8") as f:
            self.assertNotIn("expected", f.readline())          # no label columns leak into input

    def test_20k_run_matches_independent_labels_and_is_idempotent(self):
        d = os.path.join(self.tmp, "d")
        p = datagen.write_dataset(d, n=N, seed=42)
        load_reference(self.db, p["reference_v1"])
        r = load_events(self.db, p["events"])
        labels = read_labels(p["labels"])
        self.assertEqual(r.status, "success")
        self.assertEqual(r.rows_read, len(labels))
        self.assertEqual(self.outcomes("events.csv"), labels)       # every single row matches the oracle
        s1 = self.state()
        self.assertEqual(load_events(self.db, p["events"]).status, "success")
        self.assertEqual(self.state(), s1)
        conn = connect(self.db)
        checks = reconcile(conn, self.scalar("SELECT run_id FROM pipeline_run WHERE run_type='event_load' ORDER BY rowid DESC LIMIT 1"), p["events"])
        conn.close()
        self.assertTrue(all(c.passed for c in checks), [c for c in checks if not c.passed])
        # the run is not trivially all-valid: every reject category appears
        kinds = set(labels.values())
        for k in ("valid", "valid_deprecated", "CODE_EXPIRED", "CODE_NOT_YET_EFFECTIVE", "UNKNOWN_CODE", "DUPLICATE_EVENT",
                  "SUPERSEDED_VERSION", "MALFORMED_RECORD", "INVALID_TIMESTAMP", "INVALID_AMOUNT", "MISSING_REQUIRED_FIELD",
                  "UNKNOWN_REFERENCE_DATASET"):
            self.assertIn(k, kinds)


class CliTests(TmpCase):
    def test_cli_end_to_end_and_exit_codes(self):
        ev = os.path.join(SAMPLE, "events_sample.csv")
        ref = os.path.join(SAMPLE, "reference", "reference_v1.csv")
        out = os.path.join(self.tmp, "rep.md")
        self.assertEqual(cli.main(["init", "--db", self.db]), 0)
        self.assertEqual(cli.main(["load-ref", "--db", self.db, "--file", ref]), 0)
        self.assertEqual(cli.main(["load-events", "--db", self.db, "--file", ev]), 0)
        self.assertEqual(cli.main(["report", "--db", self.db, "--input", ev, "--out", out]), 0)
        with open(out, encoding="utf-8") as f:
            self.assertIn("ALL PASS", f.read())
        self.assertEqual(cli.main(["load-events", "--db", self.db, "--file", os.path.join(self.tmp, "missing.csv")]), 2)
        self.assertEqual(cli.main(["load-events", "--db", self.db, "--file", ev, "--pin", "bad"]), 2)


if __name__ == "__main__":
    unittest.main()
````

### Checkpoint 7
```powershell
py -m unittest discover -s tests -t . -v
```
Last mein `Ran 39 tests ... OK` aana chahiye. Output file mein save karna ho (evidence ke liye):
```powershell
py -m unittest discover -s tests -t . -v 2>&1 | Out-File -Encoding utf8 output\test_results.txt
```

---

## STEP 8: Benchmark + larger dataset

#### bench.py

**File: `bench.py`**

````python
"""Performance measurement + one optimisation comparison.

Usage: python bench.py [--rows 100000] [--seed 42]

1. End-to-end throughput of load_events on N synthetic rows (with per-stage timings).
2. Optimisation: reference lookup via in-memory index (implemented) vs one SQL query per row (naive baseline).
"""
import argparse
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import time
from datetime import date

from codevalidator import datagen
from codevalidator.db import connect
from codevalidator.pipeline import load_events
from codevalidator.reference import RefIndex, load_reference
from codevalidator.util import read_csv


def naive_classify_all(conn, rows, ver):
    """Baseline: one indexed SELECT per event row."""
    n = 0
    for _ln, f in rows:
        d = date.fromisoformat(f[2][:10])
        conn.execute("SELECT status, effective_from, effective_to FROM ref_code WHERE ref_version_id=? AND code=?",
                     (ver.get(f[4].lower(), 0), f[5].upper())).fetchall()
        n += 1
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="output/benchmark_results.txt")
    a = ap.parse_args()
    logging.disable(logging.CRITICAL)
    tmp = tempfile.mkdtemp(prefix="cv_bench_")
    try:
        t = time.perf_counter()
        paths = datagen.write_dataset(os.path.join(tmp, "data"), n=a.rows, seed=a.seed)
        gen_s = time.perf_counter() - t
        db = os.path.join(tmp, "bench.db")
        load_reference(db, paths["reference_v1"])
        res = load_events(db, paths["events"])
        res2 = load_events(db, paths["events"])             # rerun: idempotent, measures steady-state
        lines = [
            f"rows requested={a.rows} actual_rows={res.rows_read} seed={a.seed}",
            f"generate: {gen_s:.2f}s",
            f"load_events (first run):  {res.duration_seconds:.2f}s = {res.rows_read / res.duration_seconds:,.0f} rows/s",
            f"load_events (rerun, same input): {res2.duration_seconds:.2f}s = {res2.rows_read / res2.duration_seconds:,.0f} rows/s",
            "stage seconds (first run): " + json.dumps(res.stage_seconds),
            f"outcome: valid={res.rows_valid} valid_deprecated={res.rows_valid_deprecated} rejected={res.rows_rejected}",
        ]
        # optimisation comparison on the validation step only
        conn = connect(db)
        _h, rows, _b = read_csv(paths["events"])
        def ok(f):
            try:
                date.fromisoformat(f[2][:10])
                return len(f) == 7 and bool(f[5])
            except ValueError:
                return False
        sample = [r for r in rows if ok(r[1])][:50000]    # well-formed rows only: isolates the lookup cost
        t = time.perf_counter()
        naive_classify_all(conn, sample, RefIndex.from_db(conn).versions)
        naive = time.perf_counter() - t
        t = time.perf_counter()
        idx = RefIndex.from_db(conn)
        build = time.perf_counter() - t
        t = time.perf_counter()
        for _ln, f in sample:
            idx.classify(f[4].lower(), f[5].upper(), date.fromisoformat(f[2][:10]))
        fast = time.perf_counter() - t
        conn.close()
        lines += [
            "",
            f"Optimisation: reference lookup on {len(sample):,} rows",
            f"  naive  (1 SQL query per row):            {naive:.3f}s",
            f"  in-memory index (build {build * 1000:.1f} ms + lookups): {fast:.3f}s  -> {naive / (fast + build):.1f}x faster",
        ]
        text = "\n".join(lines)
        print(text)
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
````

```powershell
py bench.py --rows 100000
```
Ye throughput, stage timings aur optimisation comparison print karta hai aur `output\benchmark_results.txt` mein save karta hai.

Bada dataset khud generate karke chalane ke liye:

```powershell
py -m codevalidator generate --out-dir out\data --rows 100000 --seed 42
py -m codevalidator load-ref --db out\big.db --file out\data\reference_v1.csv
py -m codevalidator load-events --db out\big.db --file out\data\events.csv
py -m codevalidator report --db out\big.db --input out\data\events.csv --out output\validation_report_100k.md
```
`out\data\events_expected_outcomes.csv` = answer labels (pipeline inhe kabhi nahi padhta).

---

## STEP 9: Documentation files

Submission mein README, data dictionary aur design note bhi chahiye.

#### README.md

**File: `README.md`**

````markdown
# Code Validator: validating incoming codes against versioned reference data

A small, tested data-engineering pipeline. It ingests event files, validates every `code` against
**versioned reference datasets** (effective dates, deprecation, unknown values, refreshes),
publishes accepted events, quarantines everything else with a traceable reason, and proves
correctness with reconciliation checks.

* Python 3.9+ expected (developed and tested on 3.12), **standard library only** (SQLite). No installs, no credentials, no network.
* Safe to re-run: same input + same reference versions => identical target state.

## Quick start

```bash
# from the project root
python -m unittest discover -s tests -t . -v      # 39 tests, ~3 s
python -m codevalidator demo                      # end-to-end on the committed sample + writes a report
python bench.py --rows 100000                     # performance measurement
```

Step by step (what `demo` does):

```bash
python -m codevalidator init        --db out/codes.db
python -m codevalidator load-ref    --db out/codes.db --file data/sample/reference/reference_v1.csv
python -m codevalidator load-events --db out/codes.db --file data/sample/events_sample.csv
python -m codevalidator report      --db out/codes.db --input data/sample/events_sample.csv --out out/report.md

# larger, reproducible run (fixed seed) incl. independent expected-answer labels
python -m codevalidator generate    --out-dir out/data --rows 100000 --seed 42
python -m codevalidator load-ref    --db out/big.db --file out/data/reference_v1.csv
python -m codevalidator load-events --db out/big.db --file out/data/events.csv
```

Exit code is `0` on success, `2` when a run fails (the failure is also stored in `pipeline_run`).
Add `--log-file logs/run.log` (before the sub-command) to also write logs to a file.

## Evidence included in `output/`

| File | What it shows |
|---|---|
| `test_results.txt` | verbose run of all 39 tests (all pass) |
| `validation_report_sample.md` | sample run after a reference refresh: counters, **10 reconciliation checks**, rejects by reason, event history |
| `validation_report_100k.md` | same report for 100,000 generated rows (all checks PASS) |
| `benchmark_results.txt` | throughput, per-stage timings, optimisation comparison |

## Architecture / flow

```
 reference_vN.csv ──load-ref──► [validate snapshot: dates, overlaps, dangling replaced_by]
   (full snapshot)                  │ same content hash as latest? ─► no-op (idempotent)
                                    └► new immutable ref_version + ref_code rows (old versions kept)

 events.csv ──load-events──►  1 read + schema check (missing column => fail file; extra column => warn)
                              2 field validation   (arity, required, ISO-8601 tz-aware ts, amount)  ──┐
                              3 in-file dedupe     (exact dup / newest updated_at wins)             ──┤
                              4 cross-file stale check vs event_validated (late/out-of-order)       ──┤ rejects
                              5 reference check    (in-memory index: effective window, deprecated)  ──┘
                              6 ONE TRANSACTION: replace this file's rejects, upsert accepted,
                                append history, write run summary
                              7 reconciliation assertions inside the transaction (mismatch => ROLLBACK)
                                         │
        pipeline_run (metrics) ◄─────────┴──►  event_validated | event_reject | event_history
                                                         │
                                               report (SQL re-computation, 10 checks)
```

Modules: `rules.py` (constants) · `validate.py` (pure row parsing) · `reference.py` (load + versioned lookup) ·
`pipeline.py` (orchestration) · `report.py` (reconciliation/report) · `datagen.py` (generator) · `cli.py` · `db.py` (DDL).

## Business rules

**Reference snapshot** (`dataset_name, code, description, status, effective_from, effective_to, replaced_by_code`)
* One row = one code in one validity window. `effective_from` **inclusive**, `effective_to` **exclusive**, blank = open-ended.
* `status` is `active` or `deprecated`. Windows of one code may not overlap; `replaced_by_code` must exist in the same dataset.
* Any problem in a snapshot rejects the **whole** refresh (nothing is published) - a half-loaded reference is worse than none.

**Event decision** for `(dataset_name, code)` on the **UTC date of `event_ts`** against the chosen reference version:

| Situation | Result |
|---|---|
| inside an `active` window | `valid` |
| inside a `deprecated` window | `valid_deprecated` (accepted, warning `CODE_DEPRECATED`, suggested `replaced_by_code`) |
| before the first window | reject `CODE_NOT_YET_EFFECTIVE` |
| after / between windows | reject `CODE_EXPIRED` |
| code not in snapshot | reject `UNKNOWN_CODE` |
| dataset not loaded | reject `UNKNOWN_REFERENCE_DATASET` |

Row-level rules run in this order and the first failure is the single recorded reason:
`MALFORMED_RECORD` (field count) → `MISSING_REQUIRED_FIELD` → `INVALID_TIMESTAMP` (must be ISO-8601 **with** offset/`Z`) →
`INVALID_AMOUNT` (non-negative, ≤ 2 decimals, stored as integer cents) → in-file `DUPLICATE_EVENT` / `SUPERSEDED_VERSION` →
cross-file `STALE_VERSION` → reference reasons above. Normalisation: values trimmed, `code` upper-cased, `dataset_name` lower-cased.

**Versions of the same event** (`event_id` is the primary key; `updated_at` is the source's version clock):
newest `updated_at` wins; ties keep the first occurrence. An older/equal-but-different version arriving in a later file is `STALE_VERSION`.

## Idempotency, retry, restart, backfill

* **Atomic per file.** Everything for one input file is one SQLite transaction. A crash/exception rolls back; the run row is then marked `failed` with `failure_reason`. **Retry = run the same command again.** Tested by injecting a mid-load failure.
* **Rerun-safe.** Accepted rows are upserted by `event_id`; rejects are keyed by `(source_file, source_line)` and replaced per file; history has a natural primary key and uses `INSERT OR IGNORE`. Reruns add a new `pipeline_run` row but never duplicate data (tested incl. 20k-row rerun).
* **Reference refresh.** `load-ref` of identical content is a no-op; changed content appends a new version (v1 stays queryable).
* **Backfill.** After a refresh, re-run the old event file(s): they are re-validated against the latest versions. Events that became invalid are removed from `event_validated` and appear in `event_reject`; every outcome change is kept in `event_history`. Use `--pin diagnosis_code=1` to reproduce a past result with an older reference version.
* **Restart.** There is no hidden state; files are idempotent so restart means "re-run the files not yet successful in `pipeline_run`".

## Observability

Each run writes `pipeline_run`: `run_id`, `started_at`/`ended_at` (ISO-8601 UTC), `duration_seconds`, `rows_read`, `rows_valid`,
`rows_valid_deprecated`, `rows_rejected`, amounts (cents), `input_sha256`, reference versions used, `notes` (e.g. schema drift),
`failure_reason`, per-stage timings, and `reconciled`. `run_reject_summary` keeps the per-run reject breakdown. Logs are one line per event with the `run_id`.

## Naming conventions

snake_case tables/columns; ISO-8601 UTC timestamps (`YYYY-MM-DDTHH:MM:SSZ`), dates `YYYY-MM-DD`; `*_cents` integers for money;
`run_id = <evt|ref>_<UTC time>_<8 hex>`; `reject_id = sha256(source_file|source_line)[:16]` and `record_hash = sha256(stripped payload)[:16]` are deterministic;
`source_file` is the file's base name. Schema: [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md). Trade-offs: [`docs/DESIGN_NOTES.md`](docs/DESIGN_NOTES.md).

## Test coverage (39 tests)

Happy path · empty batch (header-only OK, zero-byte fails) · missing column / extra column (schema change) · malformed rows · nulls ·
exact duplicates · out-of-order versions in a file · late/stale versions across files · invalid reference dataset/code ·
**domain edge cases**: effective-date boundaries (inclusive/exclusive, to the second), UTC-date conversion of offsets, deprecated-with-replacement,
gaps/reactivation, refresh+backfill, valid→invalid after refresh, pinned versions · partial failure + retry · rerun idempotency ·
reference file validation (overlap, dangling replacement, bad dates/status) · generator determinism · **20k-row run matched row-by-row to independent labels** · CLI exit codes.

Expected answers live in separate files (`data/sample/expected/…`, `<prefix>_expected_outcomes.csv`) and are never read by the pipeline.

## Performance

`python bench.py --rows 100000` (this machine, results in `output/benchmark_results.txt`): ~22k rows/s end to end (100k rows ≈ 4.4 s).
Slowest stages: field validation (≈1.4 s, `datetime`/`Decimal` parsing) and publish (≈1.5 s, SQLite upserts).
**Optimisation implemented:** the reference is loaded once into an in-memory `{(dataset, code): windows}` index so validation does no SQL per row; measured **≈2.2–2.5× faster** across runs than one indexed SELECT per row
(a conservative number: the naive baseline excludes the window logic). Rows are also written with `executemany` in chunks and existing events are fetched in batches of 500 ids.
Next candidates: cache parsed timestamps, bulk-insert into a staging table, stream in chunks.

## Assumptions & known limitations

* Reference snapshots are **full** per dataset: a code missing from a newer snapshot becomes `UNKNOWN_CODE` (expire it with `effective_to` instead of deleting).
* The stale check compares against **accepted** stored events only; a newer version that was rejected is not remembered, so an older valid version may stay as current.
* `event_reject` mirrors the *latest* run per `source_file`; earlier rejects survive only as counts (`run_reject_summary`) and in `event_history`.
* An identical payload re-delivered in a *different* file is treated as a re-validation (not a duplicate); `event_validated.source_file/line` then points to the latest file.
* The input file is held in memory (fine for millions of rows, not billions). Single writer (SQLite `BEGIN IMMEDIATE` serialises loads).
* `source_file` identity is the base name; two different files with the same name share reject/rerun semantics.

## Possible next steps

Chunked/streaming ingestion · Postgres/warehouse target with `MERGE` · scheduler + alerting on `failed` runs and reject-rate thresholds ·
`as_of` reference selection by event time (bi-temporal) · reject-rate and deprecated-usage dashboards · property-based tests.
````

#### docs/DATA_DICTIONARY.md

**File: `docs/DATA_DICTIONARY.md`**

````markdown
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
````

#### docs/DESIGN_NOTES.md

**File: `docs/DESIGN_NOTES.md`**

````markdown
# Design and trade-off note

## Goals (from the brief)
Correctness and data quality first, then clear design, tests, idempotency/recovery/observability, performance reasoning. A complete vertical slice beats breadth.

## Key decisions

| Decision | Why | Trade-off |
|---|---|---|
| **SQLite + stdlib only** | Runs anywhere with `python`; transactional DDL/DML gives real atomicity; SQL reconciliation is natural. | Single writer; not a distributed warehouse. The SQL is portable (upserts → `MERGE`). |
| **Immutable reference versions** (new row set per changed snapshot, content-hash dedupe) | Refresh is idempotent, old results are reproducible (`--pin`), and nothing is silently overwritten. | Storage grows per change (small for reference data). |
| **Effective window = `[from, to)` on the UTC date of `event_ts`** | One unambiguous boundary rule, tested to the second; offsets can't shift the business date unpredictably. | A source that thinks in local dates may need a configurable timezone (next step). |
| **Deprecated ≠ invalid**: accept with warning and replacement hint | Matches how code sets are really retired: use of a deprecated code is a quality signal, not data loss. | Consumers must read `validation_status`. Switchable to reject by a one-line change in `RefIndex.classify`. |
| **One transaction per file** | Partial failure leaves no half-loaded state; retry is just a rerun. | A single bad *run-level* problem (e.g. schema change) blocks the whole file, by design; bad *rows* are quarantined individually. |
| **Reject path = table with raw record + line + deterministic id** | Every rejected row is traceable and reprocessable after a reference fix (backfill). | `event_reject` mirrors the latest run per file; history is in counts + `event_history`. |
| **`updated_at` as the version clock** | Gives a deterministic answer for duplicates and out-of-order/late data without relying on arrival order. | Needs the source to supply it (it's a required column). |
| **Money as integer cents, exact `Decimal` parsing; >2 decimals rejected** | No float drift; aggregates reconcile to the cent. | Stricter than rounding; surfaces upstream issues instead of hiding them. |
| **Reconciliation inside the transaction** | A mismatch rolls the load back, so a published run is always self-consistent. | Slight overhead (two aggregate queries). |
| **Generated labels kept in separate files, built by construction** | Tests compare the pipeline to an oracle that doesn't share its code path. | The generator encodes the same *rules* (as specification), so a wrong spec would be wrong in both; the hand-written sample guards that. |

## Validation strategy
1. Unit tests for pure functions (timestamps, amounts, window classification).
2. Scenario tests for each required edge case (see README).
3. Hand-written sample with hand-derived expected outcomes for **two** reference versions (the refresh changes 3 outcomes).
4. 20k-row generated run compared row-by-row with independent labels, rerun for idempotency.
5. In-pipeline and report-level reconciliation (counts, amounts, FK integrity, traceability, independent input recount).

## Performance reasoning
Measured on 100k rows (~22k rows/s). Time is dominated by per-row parsing (`datetime`, `Decimal`) and the publish step. The implemented optimisation (in-memory reference index) removes per-row SQL from validation (≈2.2–2.5× on that step in isolation); the measured gain is modest because SQLite point lookups are already fast, which is itself a useful finding - the next bottlenecks are parsing and writes, not lookups.
Memory is O(rows in file); for very large files the same stages can be run per chunk, keeping the dedupe map on `event_id` (or deduping in SQL via a staging table).

## Failure handling matrix
| Situation | Behaviour |
|---|---|
| Bad row (malformed, null, invalid value, unknown/expired code) | quarantined with reason + line; file continues |
| Missing required column / zero-byte / unreadable / unknown pin / no reference | run `failed`, nothing published, reason stored |
| Crash mid-write | rollback; run stays `failed`; rerun |
| Reconciliation mismatch | rollback, `RECONCILIATION_FAILED` |
| Invalid reference snapshot | whole refresh rejected; previous version stays current |
| Same file again | identical state; new run row |
| Reference refreshed, same file again | re-validated; changed outcomes moved between tables, history kept |

## Known limitations
See README. The most important: stale detection only knows accepted versions; reject mirror is per latest run; single-process SQLite.
````

#### requirements.txt

**File: `requirements.txt`**

````text
# No third-party dependencies. Python 3.9+ standard library only (sqlite3, csv, decimal, unittest).
````

Optional: `.gitignore` (agar GitHub pe daalna ho):
````text
__pycache__/
*.pyc
*.db
*.db-wal
*.db-shm
logs/
````

---

## STEP 10: Final submission checklist

- [ ] `py -m unittest discover -s tests -t . -v` -> 39 tests OK
- [ ] `py -m codevalidator demo` -> report mein ALL PASS
- [ ] `py bench.py --rows 100000` -> `output\benchmark_results.txt` bana
- [ ] `output\` mein: test_results.txt, validation_report_sample.md, validation_report_100k.md, benchmark_results.txt
- [ ] Folder zip karo (`Right click -> Send to -> Compressed folder`). `__pycache__` aur `.db` files delete kar do.

---

## Troubleshooting (Windows)

| Problem | Fix |
|---|---|
| `py` is not recognized | Python reinstall karo, "Add to PATH" tick karke. Ya `python` use karo. |
| `ModuleNotFoundError: codevalidator` | Command **project root** (`code_validator` folder) se chalao, `codevalidator` ke andar se nahi. `codevalidator\__init__.py` hona chahiye. |
| `IndentationError` | Tabs/spaces mix ho gaye. VS Code: Ctrl+Shift+P -> "Convert Indentation to Spaces". |
| `test_sample_matches_labels` fail | `events_sample.csv` ya expected CSV mein typo, ya `p200 ` ka trailing space hat gaya. Content guide se dobara compare karo. |
| `SyntaxError` ya `NameError` kisi file mein | Copy adhoori hui hai. Wo file poori dobara paste karo. |
| PowerShell script execution error | Hum sirf `py` commands chalate hain; scripts nahi, isliye policy badalne ki zarurat nahi. |
| `database is locked` | Koi purana Python process/VS Code terminal DB khole hai. Terminal band karo, `.db-wal` / `.db-shm` files delete karo. |
| Output mein bahut saare INFO log | Normal hai (logs stderr pe jaate hain). Sirf `run_id=... status=...` lines dekho. |

---

## Samajhne ka short map (interview ke liye)

1. **Reference data** versioned hai: refresh = nayi immutable version; purani kabhi overwrite nahi hoti.
2. **Effective date**: code tab valid jab `effective_from <= event date < effective_to`.
3. **Deprecated** code reject nahi hota, warning + replacement ke saath accept hota hai. Expired/unknown/not-yet-effective reject hote hain.
4. Har reject ek table mein jaata hai: file, line number, raw record, reason (**traceable**).
5. Ek file = ek transaction: fail hua to rollback, dobara chalao (**idempotent + retry-safe**).
6. Reference refresh ke baad purani file dobara chalao = **backfill**; outcome changes `event_history` mein safe rehte hain.
7. Test oracle (`expected_outcomes.csv`) pipeline code se **alag** hai, isliye test sach mein verify karta hai.