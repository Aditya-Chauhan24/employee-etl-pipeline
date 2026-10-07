import unittest

from sqlalchemy.exc import (
    OperationalError,
    IntegrityError,
    DataError,
    ProgrammingError,
)

from pipeline import classify_database_error


class TestErrorClassification(unittest.TestCase):

    def test_operational_error_is_retryable(self):
        error = OperationalError(
            "SELECT 1",
            {},
            Exception("database unavailable")
        )

        result = classify_database_error(error)

        self.assertEqual(result, "retryable")

    def test_integrity_error_is_non_retryable(self):
        error = IntegrityError(
            "INSERT",
            {},
            Exception("duplicate key")
        )

        result = classify_database_error(error)

        self.assertEqual(result, "non_retryable")

    def test_data_error_is_non_retryable(self):
        error = DataError(
            "INSERT",
            {},
            Exception("invalid data")
        )

        result = classify_database_error(error)

        self.assertEqual(result, "non_retryable")

    def test_programming_error_is_non_retryable(self):
        error = ProgrammingError(
            "SELECT",
            {},
            Exception("invalid SQL")
        )

        result = classify_database_error(error)

        self.assertEqual(result, "non_retryable")


if __name__ == "__main__":
    unittest.main()