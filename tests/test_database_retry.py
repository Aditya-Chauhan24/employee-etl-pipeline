import unittest
from unittest.mock import Mock, patch

from sqlalchemy.exc import OperationalError

from pipeline import execute_database_with_retry


class TestDatabaseRetry(unittest.TestCase):

    def test_retryable_operation_retries_three_times(self):
        operation = Mock(
            side_effect=OperationalError(
                "SELECT 1",
                {},
                Exception("database unavailable")
            )
        )

        with patch("pipeline.time.sleep") as mock_sleep:
            with self.assertRaises(OperationalError):
                execute_database_with_retry(
                    operation,
                    "TEST DATABASE LOAD",
                    max_retries=3
            )

        self.assertEqual(operation.call_count, 3)

        self.assertEqual(
            mock_sleep.call_count,
            2
        )

        mock_sleep.assert_any_call(2)
        mock_sleep.assert_any_call(4)
        
    def test_successful_operation_runs_once(self):
        operation = Mock(return_value="success")

        result = execute_database_with_retry(
            operation,
            "TEST DATABASE LOAD",
            max_retries=3
        )

        self.assertEqual(result is not None, True)
        self.assertEqual(operation.call_count, 1)

    def test_non_retryable_error_stops_immediately(self):
        from sqlalchemy.exc import IntegrityError

        operation = Mock(
            side_effect=IntegrityError(
                "INSERT",
                {},
                Exception("duplicate key")
            )
        )

        with self.assertRaises(IntegrityError):
            execute_database_with_retry(
                operation,
                "TEST DATABASE LOAD",
                max_retries=3
            )

        self.assertEqual(operation.call_count, 1)


if __name__ == "__main__":
    unittest.main()