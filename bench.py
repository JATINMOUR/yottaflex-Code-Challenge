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
