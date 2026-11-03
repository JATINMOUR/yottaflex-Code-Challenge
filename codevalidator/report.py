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
