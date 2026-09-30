from pipeline import validate_employee


def test_valid_employee():
    employee = {
        "id": 101,
        "name": "Rahul",
        "age": 24,
        "salary": 70000,
        "experience": 3,
        "joining_year": 2023,
        "performance_score": 8.2,
        "department": "Engineering"
    }

    is_valid, errors = validate_employee(employee)

    assert is_valid is True
    assert errors == []


def test_invalid_salary():
    employee = {
        "id": 101,
        "name": "Rahul",
        "age": 24,
        "salary": -50000,
        "experience": 3,
        "joining_year": 2023,
        "performance_score": 8.2,
        "department": "Engineering"
    }

    is_valid, errors = validate_employee(employee)

    assert is_valid is False
    assert "Salary must be greater than 0" in errors


def test_invalid_age():
    employee = {
        "id": 101,
        "name": "Rahul",
        "age": 17,
        "salary": 70000,
        "experience": 3,
        "joining_year": 2023,
        "performance_score": 8.2,
        "department": "Engineering"
    }

    is_valid, errors = validate_employee(employee)

    assert is_valid is False
    assert "Age must be at least 18" in errors


def test_invalid_performance_score():
    employee = {
        "id": 101,
        "name": "Rahul",
        "age": 24,
        "salary": 70000,
        "experience": 3,
        "joining_year": 2023,
        "performance_score": 11,
        "department": "Engineering"
    }

    is_valid, errors = validate_employee(employee)

    assert is_valid is False
    assert "Performance score must be between 0 and 10" in errors


def test_missing_department():
    employee = {
        "id": 101,
        "name": "Rahul",
        "age": 24,
        "salary": 70000,
        "experience": 3,
        "joining_year": 2023,
        "performance_score": 8.2,
        "department": ""
    }

    is_valid, errors = validate_employee(employee)

    assert is_valid is False
    assert "Department is empty" in errors

def test_multiple_validation_errors():
    employee = {
        "id": -1,
        "name": "",
        "age": 16,
        "salary": -50000,
        "experience": -2,
        "joining_year": 1800,
        "performance_score": 15,
        "department": ""
    }

    is_valid, errors = validate_employee(employee)

    assert is_valid is False

    assert "ID must be greater than 0" in errors
    assert "Name is empty" in errors
    assert "Age must be at least 18" in errors
    assert "Salary must be greater than 0" in errors
    assert "Experience cannot be negative" in errors
    assert "Joining year must be >= 1900" in errors
    assert "Performance score must be between 0 and 10" in errors
    assert "Department is empty" in errors