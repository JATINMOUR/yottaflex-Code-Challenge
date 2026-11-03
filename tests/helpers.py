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