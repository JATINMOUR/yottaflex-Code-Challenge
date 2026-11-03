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
