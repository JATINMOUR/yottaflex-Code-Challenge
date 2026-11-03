import filecmp
import os
import unittest

from codevalidator import cli, datagen
from codevalidator.db import connect
from codevalidator.pipeline import load_events
from codevalidator.reference import load_reference
from codevalidator.report import reconcile
from tests.helpers import SAMPLE, TmpCase, read_labels


class ScaleTests(TmpCase):
    def test_generator_is_reproducible(self):
        first = datagen.write_dataset(
            os.path.join(self.tmp, "first"),
            n=500,
            seed=7,
        )
        second = datagen.write_dataset(
            os.path.join(self.tmp, "second"),
            n=500,
            seed=7,
        )

        for name in first:
            self.assertTrue(
                filecmp.cmp(first[name], second[name], shallow=False)
            )

        with open(first["events"], encoding="utf-8") as file:
            self.assertNotIn("expected", file.readline())

    def test_large_batch_matches_labels_and_reruns_safely(self):
        folder = os.path.join(self.tmp, "generated")
        files = datagen.write_dataset(folder, n=20000, seed=42)

        load_reference(self.db, files["reference_v1"])
        result = load_events(self.db, files["events"])
        labels = read_labels(files["labels"])

        self.assertEqual(result.status, "success")
        self.assertEqual(result.rows_read, len(labels))
        self.assertEqual(self.outcomes("events.csv"), labels)

        first_state = self.state()
        rerun = load_events(self.db, files["events"])

        self.assertEqual(rerun.status, "success")
        self.assertEqual(self.state(), first_state)

        latest_run = self.scalar(
            "SELECT run_id FROM pipeline_run "
            "WHERE run_type='event_load' ORDER BY rowid DESC LIMIT 1"
        )

        conn = connect(self.db)
        try:
            checks = reconcile(conn, latest_run, files["events"])
        finally:
            conn.close()

        self.assertTrue(
            all(check.passed for check in checks),
            [check for check in checks if not check.passed],
        )

        expected_types = {
            "valid",
            "valid_deprecated",
            "CODE_EXPIRED",
            "CODE_NOT_YET_EFFECTIVE",
            "UNKNOWN_CODE",
            "UNKNOWN_REFERENCE_DATASET",
            "MALFORMED_RECORD",
            "INVALID_TIMESTAMP",
            "INVALID_AMOUNT",
            "MISSING_REQUIRED_FIELD",
            "DUPLICATE_EVENT",
            "SUPERSEDED_VERSION",
        }
        self.assertTrue(expected_types.issubset(set(labels.values())))


class CliTests(TmpCase):
    def test_cli_commands_and_exit_codes(self):
        events = os.path.join(SAMPLE, "events_sample.csv")
        reference = os.path.join(
            SAMPLE,
            "reference",
            "reference_v1.csv",
        )
        report = os.path.join(self.tmp, "report.md")

        self.assertEqual(cli.main(["init", "--db", self.db]), 0)

        self.assertEqual(
            cli.main([
                "load-ref",
                "--db", self.db,
                "--file", reference,
            ]),
            0,
        )

        self.assertEqual(
            cli.main([
                "load-events",
                "--db", self.db,
                "--file", events,
            ]),
            0,
        )

        self.assertEqual(
            cli.main([
                "report",
                "--db", self.db,
                "--input", events,
                "--out", report,
            ]),
            0,
        )

        with open(report, encoding="utf-8") as file:
            self.assertIn("ALL PASS", file.read())

        missing_file = os.path.join(self.tmp, "missing.csv")
        self.assertEqual(
            cli.main([
                "load-events",
                "--db", self.db,
                "--file", missing_file,
            ]),
            2,
        )

        self.assertEqual(
            cli.main([
                "load-events",
                "--db", self.db,
                "--file", events,
                "--pin", "bad",
            ]),
            2,
        )


if __name__ == "__main__":
    unittest.main()