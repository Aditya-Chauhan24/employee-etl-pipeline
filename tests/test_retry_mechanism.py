from unittest.mock import patch, Mock

from sqlalchemy.exc import OperationalError, IntegrityError

from pipeline import execute_database_with_retry


def test_retryable_error_then_success():

    operation = Mock()

    operation.side_effect = [
        OperationalError(
            "connection failed",
            {},
            Exception("connection refused")
        ),
        OperationalError(
            "connection failed",
            {},
            Exception("connection refused")
        ),
        None
    ]

    mock_engine = Mock()
    mock_begin = mock_engine.begin

    mock_connection_context = Mock()
    mock_connection_context.__enter__ = Mock(return_value=Mock())
    mock_connection_context.__exit__ = Mock(return_value=None)

    mock_begin.return_value = mock_connection_context

    with patch("pipeline.get_engine", return_value=mock_engine):

        with patch("pipeline.time.sleep"):

            execute_database_with_retry(
                operation,
                "TEST RETRY",
                max_retries=3
            )

    assert operation.call_count == 3
    assert mock_begin.call_count == 3


def test_non_retryable_error_stops_immediately():

    operation = Mock()

    operation.side_effect = IntegrityError(
        "duplicate key",
        {},
        Exception("duplicate key violation")
    )

    mock_engine = Mock()
    mock_begin = mock_engine.begin

    mock_connection_context = Mock()
    mock_connection_context.__enter__ = Mock(return_value=Mock())
    mock_connection_context.__exit__ = Mock(return_value=None)

    mock_begin.return_value = mock_connection_context

    with patch("pipeline.get_engine", return_value=mock_engine):

        with patch("pipeline.time.sleep"):

            try:
                execute_database_with_retry(
                    operation,
                    "TEST NON RETRYABLE",
                    max_retries=3
                )
            except IntegrityError:
                pass

    assert operation.call_count == 1
    assert mock_begin.call_count == 1 
