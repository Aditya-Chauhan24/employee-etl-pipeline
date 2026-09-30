from sqlalchemy.exc import (
    OperationalError,
    IntegrityError,
    DataError,
    ProgrammingError,
)

from pipeline import classify_database_error


def test_operational_error_is_retryable():

    error = OperationalError(
        "connection failed",
        {},
        Exception("connection refused")
    )

    result = classify_database_error(error)

    assert result == "retryable"


def test_integrity_error_is_non_retryable():

    error = IntegrityError(
        "duplicate key",
        {},
        Exception("duplicate key violation")
    )

    result = classify_database_error(error)

    assert result == "non_retryable"


def test_data_error_is_non_retryable():

    error = DataError(
        "invalid data",
        {},
        Exception("invalid numeric value")
    )

    result = classify_database_error(error)

    assert result == "non_retryable"


def test_programming_error_is_non_retryable():

    error = ProgrammingError(
        "invalid SQL",
        {},
        Exception("syntax error")
    )

    result = classify_database_error(error)

    assert result == "non_retryable"