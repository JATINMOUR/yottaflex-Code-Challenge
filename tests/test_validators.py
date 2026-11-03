import unittest
from datetime import date

from codevalidator import rules
from codevalidator.reference import RefIndex, Window
from codevalidator.validate import RecordError, parse_amount, parse_event, parse_ts


INDEXES = {
    column: position
    for position, column in enumerate(rules.REQUIRED_EVENT_COLUMNS)
}


def make_row(**changes):
    values = {
        "event_id": "E1",
        "source_system": "system_a",
        "event_ts": "2025-03-10T09:00:00Z",
        "updated_at": "2025-03-10T09:05:00Z",
        "dataset_name": "diagnosis_code",
        "code": "d100",
        "amount": "12.50",
    }
    values.update(changes)
    return [values[column] for column in rules.REQUIRED_EVENT_COLUMNS]


class ValidatorTests(unittest.TestCase):
    def test_valid_row_is_normalized(self):
        event = parse_event(2, make_row(), 7, INDEXES)
        self.assertEqual(event.code, "D100")
        self.assertEqual(event.amount_cents, 1250)

    def test_timestamp_requires_timezone(self):
        with self.assertRaises(ValueError):
            parse_ts("2025-03-10T09:00:00")

    def test_timestamp_is_converted_to_utc(self):
        event = parse_event(
            2,
            make_row(event_ts="2025-03-10T01:00:00+05:30"),
            7,
            INDEXES,
        )
        self.assertEqual(event.event_ts, "2025-03-09T19:30:00Z")

    def test_amount_is_converted_to_cents(self):
        self.assertEqual(parse_amount("7.50"), 750)

    def test_invalid_amount_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_amount("-1.00")

    def test_invalid_row_has_reject_reason(self):
        with self.assertRaises(RecordError) as error:
            parse_event(2, make_row(code=""), 7, INDEXES)

        self.assertEqual(error.exception.reason, rules.MISSING_REQUIRED_FIELD)


class ReferenceDateTests(unittest.TestCase):
    def setUp(self):
        active = Window(date(2020, 1, 1), None, "active", None, 1)
        old = Window(
            date(2020, 1, 1),
            date(2025, 1, 1),
            "deprecated",
            "A",
            1,
        )
        future = Window(date(2026, 6, 1), None, "active", None, 1)

        self.index = RefIndex(
            {
                ("codes", "A"): [active],
                ("codes", "OLD"): [old],
                ("codes", "FUTURE"): [future],
            },
            {"codes": 1},
        )

    def test_effective_start_date_is_inclusive(self):
        result = self.index.classify("codes", "FUTURE", date(2026, 6, 1))
        self.assertEqual(result.status, rules.VALID)

    def test_effective_end_date_is_exclusive(self):
        result = self.index.classify("codes", "OLD", date(2025, 1, 1))
        self.assertEqual(result.reason_code, rules.CODE_EXPIRED)

    def test_deprecated_code_has_replacement(self):
        result = self.index.classify("codes", "OLD", date(2024, 1, 1))
        self.assertEqual(result.status, rules.VALID_DEPRECATED)
        self.assertEqual(result.replaced_by, "A")

    def test_unknown_code_is_rejected(self):
        result = self.index.classify("codes", "MISSING", date(2025, 1, 1))
        self.assertEqual(result.reason_code, rules.UNKNOWN_CODE)


if __name__ == "__main__":
    unittest.main()