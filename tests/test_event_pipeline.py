import os
import unittest

from codevalidator.db import connect
from codevalidator.pipeline import load_events
from codevalidator.reference import load_reference
from codevalidator.report import reconcile
from tests.helpers import EVENT_HEADER, REF_BASIC, SAMPLE, TmpCase, ev, read_labels


class EventPipelineTests(TmpCase):
    def setUp(self):
        super().setUp()
        load_reference(self.db, self.write("reference.csv", REF_BASIC))

    def run_events(self, text, name="events.csv", **options):
        return load_events(
            self.db,
            self.write(name, text),
            **options,
        )

    def test_valid_and_deprecated_events(self):
        text = (
            EVENT_HEADER + "\n"
            + ev("E1") + "\n"
            + ev("E2", code="D150", ts="2024-06-01T00:00:00Z")
            + "\n"
        )

        result = self.run_events(text)

        self.assertEqual(result.status, "success")
        self.assertEqual(
            (
                result.rows_read,
                result.rows_valid,
                result.rows_valid_deprecated,
                result.rows_rejected,
            ),
            (2, 1, 1, 0),
        )

    def test_header_only_input_succeeds(self):
        result = self.run_events(EVENT_HEADER + "\n")
        self.assertEqual(result.status, "success")
        self.assertEqual(result.rows_read, 0)

    def test_empty_file_fails(self):
        result = self.run_events("")
        self.assertEqual(result.status, "failed")
        failure_reason = result.failure_reason
        if failure_reason is None:
            self.fail("Expected a failure reason")
        self.assertIn("EMPTY_FILE", failure_reason)

    def test_missing_column_fails(self):
        text = (
            "event_id,source_system,event_ts,dataset_name,code,amount\n"
            "E1,s,2025-03-10T09:00:00Z,diagnosis_code,D100,1\n"
        )

        result = self.run_events(text)

        self.assertEqual(result.status, "failed")
        failure_reason = result.failure_reason
        if failure_reason is None:
            self.fail("Expected a failure reason")
        self.assertIn("SCHEMA_MISSING_COLUMNS", failure_reason)

    def test_bad_rows_are_rejected_with_reason(self):
        text = (
            EVENT_HEADER + "\n"
            + "E1,s,2025-03-10T09:00:00Z\n"
            + ev("E2", code="")
            + "\n"
        )

        result = self.run_events(text)

        self.assertEqual(result.rows_rejected, 2)
        self.assertEqual(
            result.reject_breakdown,
            {
                "MALFORMED_RECORD": 1,
                "MISSING_REQUIRED_FIELD": 1,
            },
        )

    def test_duplicate_event_keeps_one_row(self):
        text = EVENT_HEADER + "\n" + ev("E1") + "\n" + ev("E1") + "\n"
        result = self.run_events(text)

        self.assertEqual(result.rows_valid, 1)
        self.assertEqual(result.reject_breakdown, {"DUPLICATE_EVENT": 1})

    def test_old_version_from_later_file_is_rejected(self):
        newer = ev("E1", upd="2025-03-12T00:00:00Z", amt="99.00")
        older = ev("E1", upd="2025-03-11T00:00:00Z", amt="50.00")

        self.run_events(EVENT_HEADER + "\n" + newer + "\n", "first.csv")
        result = self.run_events(EVENT_HEADER + "\n" + older + "\n", "second.csv")

        self.assertEqual(result.reject_breakdown, {"STALE_VERSION": 1})
        self.assertEqual(
            self.scalar(
                "SELECT amount_cents FROM event_validated WHERE event_id='E1'"
            ),
            9900,
        )

    def test_reference_refresh_revalidates_event(self):
        path = self.write(
            "events.csv",
            EVENT_HEADER + "\n" + ev("E1", code="D200") + "\n",
        )

        first = load_events(self.db, path)
        self.assertEqual(first.reject_breakdown, {"UNKNOWN_CODE": 1})

        new_reference = (
            REF_BASIC
            + "diagnosis_code,D200,New code,active,2020-01-01,,\n"
        )
        load_reference(self.db, self.write("reference_v2.csv", new_reference))

        second = load_events(self.db, path)
        self.assertEqual(second.rows_valid, 1)
        self.assertEqual(second.rows_rejected, 0)

    def test_rerunning_input_does_not_duplicate_rows(self):
        text = EVENT_HEADER + "\n" + ev("E1") + "\n"
        path = self.write("events.csv", text)

        first = load_events(self.db, path)
        second = load_events(self.db, path)

        self.assertEqual(first.status, "success")
        self.assertEqual(second.status, "success")
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM event_validated"),
            1,
        )

    def test_failed_write_can_be_retried(self):
        text = EVENT_HEADER + "\n" + "\n".join(
            ev(f"E{i}") for i in range(1, 5)
        ) + "\n"
        path = self.write("events.csv", text)

        failed = load_events(
            self.db,
            path,
            chunk_size=2,
            fail_after_rows=2,
        )
        self.assertEqual(failed.status, "failed")
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM event_validated"),
            0,
        )

        retry = load_events(self.db, path, chunk_size=2)
        self.assertEqual(retry.status, "success")
        self.assertEqual(retry.rows_valid, 4)

    def test_sample_matches_expected_results_and_reconciles(self):
        events = os.path.join(SAMPLE, "events_sample.csv")
        database = os.path.join(self.tmp, "sample.db")
        self.db = database

        load_reference(
            database,
            os.path.join(SAMPLE, "reference", "reference_v1.csv"),
        )
        result = load_events(database, events)

        expected = read_labels(
            os.path.join(
                SAMPLE,
                "expected",
                "events_sample_expected_ref_v1.csv",
            )
        )
        self.assertEqual(self.outcomes("events_sample.csv"), expected)

        conn = connect(database)
        try:
            checks = reconcile(conn, result.run_id, events)
        finally:
            conn.close()

        self.assertTrue(
            all(check.passed for check in checks),
            [check for check in checks if not check.passed],
        )


if __name__ == "__main__":
    unittest.main()
