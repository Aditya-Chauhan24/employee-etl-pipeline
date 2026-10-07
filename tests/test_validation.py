import unittest

from pipeline import validate_employee


class TestEmployeeValidation(unittest.TestCase):

    def valid_employee(self):
        return {
            "id": 101,
            "name": "Rahul",
            "age": 24,
            "salary": 70000,
            "experience": 3,
            "joining_year": 2023,
            "performance_score": 8.2,
            "department": "Engineering",
        }

    def test_valid_employee(self):
        employee = self.valid_employee()

        is_valid, errors = validate_employee(employee)

        self.assertTrue(is_valid)
        self.assertEqual(errors, [])

    def test_invalid_id(self):
        employee = self.valid_employee()
        employee["id"] = 0

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn("ID must be greater than 0", errors)

    def test_empty_name(self):
        employee = self.valid_employee()
        employee["name"] = ""

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn("Name is empty", errors)

    def test_invalid_age(self):
        employee = self.valid_employee()
        employee["age"] = 17

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn("Age must be at least 18", errors)

    def test_invalid_salary(self):
        employee = self.valid_employee()
        employee["salary"] = -80000

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn("Salary must be greater than 0", errors)

    def test_negative_experience(self):
        employee = self.valid_employee()
        employee["experience"] = -1

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn("Experience cannot be negative", errors)

    def test_invalid_joining_year(self):
        employee = self.valid_employee()
        employee["joining_year"] = 1899

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn("Joining year must be >= 1900", errors)

    def test_invalid_performance_score(self):
        employee = self.valid_employee()
        employee["performance_score"] = 11

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn(
            "Performance score must be between 0 and 10",
            errors,
        )

    def test_empty_department(self):
        employee = self.valid_employee()
        employee["department"] = ""

        is_valid, errors = validate_employee(employee)

        self.assertFalse(is_valid)
        self.assertIn("Department is empty", errors)


if __name__ == "__main__":
    unittest.main()