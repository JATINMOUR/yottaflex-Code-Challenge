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
