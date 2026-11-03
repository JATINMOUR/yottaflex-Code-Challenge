import unittest

from codevalidator.reference import load_reference
from tests.helpers import REF_BASIC, REF_HEADER, TmpCase


class ReferenceLoadTests(TmpCase):
    def test_load_reference(self):
        result = load_reference(
            self.db,
            self.write("reference.csv", REF_BASIC),
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_version"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_code"), 3)

    def test_loading_same_reference_twice_does_not_create_version(self):
        path = self.write("reference.csv", REF_BASIC)

        load_reference(self.db, path)
        result = load_reference(self.db, path)

        self.assertFalse(
            result["datasets"]["diagnosis_code"]["created"]
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_version"), 1)

    def test_new_reference_creates_version_and_keeps_old_one(self):
        load_reference(self.db, self.write("v1.csv", REF_BASIC))

        updated_reference = (
            REF_BASIC
            + "diagnosis_code,D200,New code,active,2025-01-01,,\n"
        )
        result = load_reference(
            self.db,
            self.write("v2.csv", updated_reference),
        )

        self.assertTrue(result["datasets"]["diagnosis_code"]["created"])
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM ref_code WHERE ref_version_id=1"),
            3,
        )
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM ref_code WHERE ref_version_id=2"),
            4,
        )

    def test_invalid_reference_is_not_saved(self):
        bad_rows = [
            "diagnosis_code,X1,d,retired,2020-01-01,,",
            "diagnosis_code,X1,d,active,2020-13-01,,",
            "diagnosis_code,X1,d,deprecated,2020-01-01,2019-01-01,",
            "diagnosis_code,X1,d,deprecated,2020-01-01,2021-01-01,NOPE",
            "diagnosis_code,D100,d,active,2022-01-01,,",
            "diagnosis_code,D100,d,active,2020-01-01,,",
            "diagnosis_code,X1",
        ]

        for number, row in enumerate(bad_rows):
            with self.subTest(row=number):
                result = load_reference(
                    self.db,
                    self.write(f"bad_{number}.csv", REF_BASIC + row + "\n"),
                )
                self.assertEqual(result["status"], "failed")
                self.assertIn(
                    "REFERENCE_VALIDATION_FAILED",
                    result["failure_reason"],
                )

        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ref_version"), 0)

    def test_missing_columns_and_empty_reference_fail(self):
        missing_columns = load_reference(
            self.db,
            self.write("missing.csv", "dataset_name,code\nx,y\n"),
        )
        self.assertIn("SCHEMA_MISSING_COLUMNS", missing_columns["failure_reason"])

        empty_file = load_reference(
            self.db,
            self.write("empty.csv", ""),
        )
        self.assertIn("EMPTY_FILE", empty_file["failure_reason"])

        header_only = load_reference(
            self.db,
            self.write("header.csv", REF_HEADER + "\n"),
        )
        self.assertIn("EMPTY_REFERENCE", header_only["failure_reason"])


if __name__ == "__main__":
    unittest.main()