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
