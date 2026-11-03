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
    sample = a.sample_dir
    db = a.db

    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(db + suffix):
            os.remove(db + suffix)

    events = os.path.join(sample, "events_sample.csv")
    reference_dir = os.path.join(sample, "reference")

    print("== 1. load reference v1")
    result = load_reference(db, os.path.join(reference_dir, "reference_v1.csv"))
    if result["status"] != "success":
        print("Reference v1 load failed:", result["failure_reason"])
        return 2

    print("== 2. load events (reference v1)")
    result = load_events(db, events)
    _print_result(result)
    if result.status != "success":
        return 2

    print("== 3. rerun the same file")
    result = load_events(db, events)
    _print_result(result)
    if result.status != "success":
        return 2

    print("== 4. refresh reference to v2 and backfill")
    result = load_reference(db, os.path.join(reference_dir, "reference_v2.csv"))
    if result["status"] != "success":
        print("Reference v2 load failed:", result["failure_reason"])
        return 2

    result = load_events(db, events)
    _print_result(result)
    if result.status != "success":
        return 2

    report = build_report(db, result.run_id, events)
    os.makedirs(os.path.dirname(os.path.abspath(a.report)), exist_ok=True)

    with open(a.report, "w", encoding="utf-8") as file:
        file.write(report)

    print(f"Report written to {a.report}")
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

