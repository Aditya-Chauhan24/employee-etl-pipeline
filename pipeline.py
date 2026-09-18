# CSV → Cleaning → Validation → Filtering → Aggregation → Sorting → Exporting.

import csv
import time
import logging
import os

from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

start_time = time.perf_counter()


# ----------------------------------
# LOGGING CONFIGURATION
# ----------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

logging.info("========== PIPELINE STARTED ==========")
logging.info("")


# ----------------------------------
# DATABASE CONNECTION
# ----------------------------------

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

try:
    with engine.begin() as connection:
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER,
            name VARCHAR(100),
            age INTEGER,
            gender VARCHAR(20),
            department VARCHAR(50),
            job_title VARCHAR(100),
            salary INTEGER,
            experience INTEGER,
            joining_year INTEGER,
            performance_score FLOAT,
            employment_status VARCHAR(20),
            location VARCHAR(100),
            validation_error TEXT
        )
    """))

    logging.info("Employees table created successfully")

except Exception:
    logging.critical(
        "PostgreSQL connection or table creation failed",
        exc_info=True
    )
    raise

# ----------------------------------
# FOLDER CONFIGURATION
# ----------------------------------

INPUT_DIR = Path("input")
EXTRACT_DIR = Path("output/extracted")

INPUT_FILE = INPUT_DIR / "employees_5000.csv"
EXTRACTED_FILE = EXTRACT_DIR / "employees_5000.csv"

EXTRACT_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------------
# STEP 1: EXTRACT
# ----------------------------------

logging.info("EXTRACT started")

stage_start = time.perf_counter()

try:

    # ----------------------------------
    # READ SOURCE FILE
    # ----------------------------------

    with open(
        INPUT_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        reader = csv.DictReader(file)

        employees = list(reader)

    # ----------------------------------
    # CHECK FOR EMPTY FILE
    # ----------------------------------

    if not employees:

        logging.warning(
            f"{INPUT_FILE} contains no records. "
            "Pipeline stopped."
        )

        logging.info("")
        logging.info(
            "========== PIPELINE EXITED =========="
        )

        raise SystemExit(0)


except FileNotFoundError:

    logging.critical(
        f"Input file not found: {INPUT_FILE}",
        exc_info=True
    )

    raise


except Exception:

    logging.error(
        "EXTRACT failed",
        exc_info=True
    )

    raise


stage_time = time.perf_counter() - stage_start

logging.info(
    f"EXTRACT completed in "
    f"{stage_time:.4f} seconds"
)

logging.info("")
# ----------------------------------
# STEP 2: CLEANING
# ----------------------------------
logging.info("CLEANING started")

stage_start = time.perf_counter()

try:
    for row in employees:

        # ID
        try:
            row["id"] = int(row["id"])
        except ValueError:
            logging.warning(
                f"Invalid ID: {row.get('id')}"
            )
            row["id"] = None

        # AGE
        try:
            row["age"] = int(row["age"])
        except ValueError:
            logging.warning(
                f"Invalid age for employee {row.get('id')}"
            )
            row["age"] = None

        # SALARY
        try:
            row["salary"] = int(row["salary"])
        except ValueError:
            logging.warning(
                f"Invalid salary for employee {row.get('id')}"
            )
            row["salary"] = None

        # EXPERIENCE
        try:
            row["experience"] = int(row["experience"])
        except ValueError:
            logging.warning(
                f"Invalid experience for employee {row.get('id')}"
            )
            row["experience"] = None

        # JOINING YEAR
        try:
            row["joining_year"] = int(row["joining_year"])
        except ValueError:
            logging.warning(
                f"Invalid joining year for employee {row.get('id')}"
            )
            row["joining_year"] = None

        # PERFORMANCE SCORE
        try:
            row["performance_score"] = float(
                row["performance_score"]
            )
        except ValueError:
            logging.warning(
                f"Invalid performance score "
                f"for employee {row.get('id')}"
            )
            row["performance_score"] = None

    logging.info("Data cleaning completed")

except KeyError:
    logging.error(
        "Required column missing from CSV",
        exc_info=True
    )
    raise

stage_time = time.perf_counter() - stage_start

logging.info(
    f"CLEANING completed in "
    f"{stage_time:.4f} seconds"
)

logging.info("")

# ----------------------------------
# STEP 3: VALIDATION
# ----------------------------------

logging.info("VALIDATION started")

stage_start = time.perf_counter()

valid_records = []
invalid_records = []

for row in employees:

    validation_errors = []

    # ID
    if row["id"] is None:
        validation_errors.append(
            "ID is invalid"
        )
    elif row["id"] <= 0:
        validation_errors.append(
            "ID must be greater than 0"
        )

    # NAME
    if not row["name"].strip():
        validation_errors.append(
            "Name cannot be empty"
        )

    # AGE
    if row["age"] is None:
        validation_errors.append(
            "Age is invalid"
        )
    elif row["age"] < 18:
        validation_errors.append(
            "Age must be at least 18"
        )

    # SALARY
    if row["salary"] is None:
        validation_errors.append(
            "Salary is invalid"
        )
    elif row["salary"] <= 0:
        validation_errors.append(
            "Salary must be greater than 0"
        )

    # EXPERIENCE
    if row["experience"] is None:
        validation_errors.append(
            "Experience is invalid"
        )
    elif row["experience"] < 0:
        validation_errors.append(
            "Experience cannot be negative"
        )

    # JOINING YEAR
    if row["joining_year"] is None:
        validation_errors.append(
            "Joining year is invalid"
        )
    elif row["joining_year"] < 1900:
        validation_errors.append(
            "Joining year must be >= 1900"
        )

    # PERFORMANCE SCORE
    if row["performance_score"] is None:
        validation_errors.append(
            "Performance score is invalid"
        )
    elif not 0 <= row["performance_score"] <= 10:
        validation_errors.append(
            "Performance score must be between 0 and 10"
        )

    # DEPARTMENT
    if not row["department"].strip():
        validation_errors.append(
            "Department cannot be empty"
        )

    # FINAL VALIDATION RESULT
    if not validation_errors:

        row["validation_error"] = ""

        valid_records.append(row)

    else:

        row["validation_error"] = "; ".join(
            validation_errors
        )

        invalid_records.append(row)

        logging.warning(
            f"Invalid employee: "
            f"ID={row['id']} | "
            f"Reason={row['validation_error']}"
        )

stage_time = time.perf_counter() - stage_start

logging.info(
    f"VALIDATION completed: "
    f"{len(valid_records)} valid, "
    f"{len(invalid_records)} invalid, "
    f"in {stage_time:.4f} seconds"
)

logging.info("")

# ----------------------------------
# STEP 4: DEDUPLICATION
# ----------------------------------

logging.info("DEDUPLICATION started")

stage_start = time.perf_counter()

unique_records = []
duplicate_records = []

seen_ids = set()

for row in valid_records:

    employee_id = row["id"]

    if employee_id in seen_ids:

        duplicate_records.append(row)

    else:

        seen_ids.add(employee_id)
        unique_records.append(row)

stage_time = time.perf_counter() - stage_start

logging.info(
    f"DEDUPLICATION completed: "
    f"{len(unique_records)} unique, "
    f"{len(duplicate_records)} duplicates, "
    f"in {stage_time:.4f} seconds"
)
logging.info(f"{len(duplicate_records)} duplicate records exported to duplicate_employees.csv")

logging.info("")

# ----------------------------------
# STEP 5: FILTERING
# ----------------------------------

logging.info("FILTERING started")

stage_start = time.perf_counter()

department_employees = {
    "Engineering": [],
    "Finance": [],
    "Marketing": [],
    "Sales": [],
    "HR": []
}

for employee in unique_records:
    department = employee["department"]

    if department in department_employees:
        department_employees[department].append(employee)
stage_time = time.perf_counter() - stage_start

logging.info(
    f"Engineering employees: "
    f"{len(department_employees['Engineering'])}"
)

logging.info(
    f"Finance employees: "
    f"{len(department_employees['Finance'])}"
)

logging.info(
    f"Marketing employees: "
    f"{len(department_employees['Marketing'])}"
)

logging.info(
    f"Sales employees: "
    f"{len(department_employees['Sales'])}"
)

logging.info(
    f"HR employees: "
    f"{len(department_employees['HR'])}"
)

logging.info(
    f"FILTERING completed: "
    f"in {stage_time:.4f} seconds"
)

logging.info("")

# ----------------------------------
# STEP 5: AGGREGATION
# ----------------------------------
logging.info("AGGREGATION started")

stage_start = time.perf_counter()

department_stats = {}

for department, employees in department_employees.items():

    if not employees:
        logging.warning(
            f"No employees available for {department}"
        )
        continue

    # -------------------------------
    # Basic counts
    # -------------------------------

    employee_count = len(employees)

    active_count = sum(
        1
        for employee in employees
        if employee["employment_status"].strip().lower() == "active"
    )

    inactive_count = sum(
        1
        for employee in employees
        if employee["employment_status"].strip().lower() != "active"
    )

    # -------------------------------
    # Salary statistics
    # -------------------------------

    salaries = [
        employee["salary"]
        for employee in employees
    ]

    total_salary = sum(salaries)
    average_salary = total_salary / employee_count
    maximum_salary = max(salaries)
    minimum_salary = min(salaries)

    # -------------------------------
    # Age statistics
    # -------------------------------

    ages = [
        employee["age"]
        for employee in employees
    ]

    average_age = sum(ages) / employee_count
    maximum_age = max(ages)
    minimum_age = min(ages)

    # -------------------------------
    # Experience statistics
    # -------------------------------

    experiences = [
        employee["experience"]
        for employee in employees
    ]

    total_experience = sum(experiences)
    average_experience = total_experience / employee_count
    maximum_experience = max(experiences)
    minimum_experience = min(experiences)

    # -------------------------------
    # Performance statistics
    # -------------------------------

    performance_scores = [
        employee["performance_score"]
        for employee in employees
    ]

    average_performance = (
        sum(performance_scores) / employee_count
    )

    maximum_performance = max(
        performance_scores
    )

    minimum_performance = min(
        performance_scores
    )

    # -------------------------------
    # Store department statistics
    # -------------------------------

    department_stats[department] = {
        "employee_count": employee_count,

        "active_count": active_count,
        "inactive_count": inactive_count,

        "total_salary": total_salary,
        "average_salary": average_salary,
        "maximum_salary": maximum_salary,
        "minimum_salary": minimum_salary,

        "average_age": average_age,
        "maximum_age": maximum_age,
        "minimum_age": minimum_age,

        "average_experience": average_experience,
        "maximum_experience": maximum_experience,
        "minimum_experience": minimum_experience,

        "average_performance": average_performance,
        "maximum_performance": maximum_performance,
        "minimum_performance": minimum_performance
    }


# ----------------------------------
# LOG DEPARTMENT STATISTICS
# ----------------------------------

for department, stats in department_stats.items():

    logging.info(
        f"========== {department} =========="
    )

    logging.info(
        f"Employee count: "
        f"{stats['employee_count']}"
    )

    logging.info(
        f"Active employees: "
        f"{stats['active_count']}"
    )

    logging.info(
        f"Inactive employees: "
        f"{stats['inactive_count']}"
    )

    logging.info(
        f"Total salary: "
        f"{stats['total_salary']:.2f}"
    )

    logging.info(
        f"Average salary: "
        f"{stats['average_salary']:.2f}"
    )

    logging.info(
        f"Maximum salary: "
        f"{stats['maximum_salary']:.2f}"
    )

    logging.info(
        f"Minimum salary: "
        f"{stats['minimum_salary']:.2f}"
    )

    logging.info(
        f"Average age: "
        f"{stats['average_age']:.2f}"
    )

    logging.info(
        f"Maximum age: "
        f"{stats['maximum_age']}"
    )

    logging.info(
        f"Minimum age: "
        f"{stats['minimum_age']}"
    )

    logging.info(
        f"Average experience: "
        f"{stats['average_experience']:.2f}"
    )

    logging.info(
        f"Maximum experience: "
        f"{stats['maximum_experience']}"
    )

    logging.info(
        f"Minimum experience: "
        f"{stats['minimum_experience']}"
    )

    logging.info(
        f"Average performance: "
        f"{stats['average_performance']:.2f}"
    )

    logging.info(
        f"Maximum performance: "
        f"{stats['maximum_performance']:.2f}"
    )

    logging.info(
        f"Minimum performance: "
        f"{stats['minimum_performance']:.2f}"
    )

    logging.info("")


stage_time = time.perf_counter() - stage_start

logging.info(
    f"AGGREGATION completed in "
    f"{stage_time:.4f} seconds"
)

logging.info("")
# ----------------------------------
# STEP 6: SORTING
# ----------------------------------

logging.info("SORTING started")

stage_start = time.perf_counter()

sorted_department_employees = {}

for department, employees in department_employees.items():

    if not employees:
        logging.warning(
            f"No employees available for {department}"
        )
        continue

    sorted_employees = sorted(
        employees,
        key=lambda employee: employee["salary"],
        reverse=True
    )

    sorted_department_employees[department] = sorted_employees

stage_time = time.perf_counter() - stage_start

logging.info(
    f"SORTING completed in "
    f"{stage_time:.4f} seconds"
)

logging.info("")

# ----------------------------------
# STEP 7: HIGHEST PAID
# ----------------------------------

logging.info("HIGHEST PAID started")

stage_start = time.perf_counter()

highest_paid_employees = {}

for department, employees in sorted_department_employees.items():

    if not employees:
        logging.warning(
            f"No employees available for {department}"
        )
        continue

    highest_paid = employees[0]

    highest_paid_employees[department] = highest_paid

    logging.info(
        f"{department} - Highest paid employee: "
        f"{highest_paid['name']} "
        f"({highest_paid['salary']})"
    )

stage_time = time.perf_counter() - stage_start

logging.info(
    f"HIGHEST PAID completed in "
    f"{stage_time:.4f} seconds"
)

logging.info("")

# ----------------------------------
# STEP 8: EXPORT
# ----------------------------------

logging.info("EXPORT started")

stage_start = time.perf_counter()

try:

    # ----------------------------------
    # EXPORT FOLDER CONFIGURATION
    # ----------------------------------

    EXPORT_DIR = Path("output/employees")
    INVALID_DIR = Path("output/invalid")
    DUPLICATE_DIR = Path("output/duplicates")

    # Create folders if they don't exist
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    INVALID_DIR.mkdir(parents=True, exist_ok=True)
    DUPLICATE_DIR.mkdir(parents=True, exist_ok=True)

    # ----------------------------------
    # FIELD NAMES
    # ----------------------------------

    fieldnames = [
        "id",
        "name",
        "age",
        "gender",
        "department",
        "job_title",
        "salary",
        "experience",
        "joining_year",
        "performance_score",
        "employment_status",
        "location",
        "validation_error"
    ]

    # ----------------------------------
    # EXPORT SORTED EMPLOYEES
    # ----------------------------------

    for department, employees in sorted_department_employees.items():

        filename = (
            f"{department.lower()}_{INPUT_FILE.name}"
        )

        output_file = EXPORT_DIR / filename

        with open(
            output_file,
            "w",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames
            )

            writer.writeheader()
            writer.writerows(employees)

        logging.info(
            f"{department} EXPORT completed: "
            f"{len(employees)} records -> {output_file}"
        )

    # ----------------------------------
    # EXPORT INVALID EMPLOYEES
    # ----------------------------------

    invalid_file = INVALID_DIR / "invalid_employees.csv"

    with open(
        invalid_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(invalid_records)

    logging.info(
        f"Invalid employee EXPORT completed: "
        f"{len(invalid_records)} records -> {invalid_file}"
    )

    # ----------------------------------
    # EXPORT DUPLICATE EMPLOYEES
    # ----------------------------------

    duplicate_file = DUPLICATE_DIR / "duplicate_employees.csv"

    with open(
        duplicate_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(duplicate_records)

    logging.info(
        f"Duplicate employee EXPORT completed: "
        f"{len(duplicate_records)} records -> {duplicate_file}"
    )

    # ----------------------------------
    # EXPORT COMPLETED
    # ----------------------------------

    stage_time = time.perf_counter() - stage_start

    logging.info(
        f"EXPORT completed in "
        f"{stage_time:.4f} seconds"
    )

    logging.info("")


except Exception:

    logging.error(
        "EXPORT failed",
        exc_info=True
    )

    logging.info("")

    raise
# ----------------------------------
# STEP 9: LOAD INTO POSTGRESQL
# ----------------------------------

logging.info("LOAD started")

stage_start = time.perf_counter()

try:

    with engine.begin() as connection:

        connection.execute(
            text("""
                INSERT INTO employees (
                    id,
                    name,
                    age,
                    gender,
                    department,
                    job_title,
                    salary,
                    experience,
                    joining_year,
                    performance_score,
                    employment_status,
                    location,
                    validation_error
                )
                VALUES (
                    :id,
                    :name,
                    :age,
                    :gender,
                    :department,
                    :job_title,
                    :salary,
                    :experience,
                    :joining_year,
                    :performance_score,
                    :employment_status,
                    :location,
                    :validation_error
                )

                ON CONFLICT (id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    age = EXCLUDED.age,
                    gender = EXCLUDED.gender,
                    department = EXCLUDED.department,
                    job_title = EXCLUDED.job_title,
                    salary = EXCLUDED.salary,
                    experience = EXCLUDED.experience,
                    joining_year = EXCLUDED.joining_year,
                    performance_score = EXCLUDED.performance_score,
                    employment_status = EXCLUDED.employment_status,
                    location = EXCLUDED.location,
                    validation_error = EXCLUDED.validation_error
            """),
            unique_records
        )

    stage_time = time.perf_counter() - stage_start

    logging.info(
        f"LOAD completed: "
        f"{len(unique_records)} records loaded/updated in PostgreSQL "
        f"in {stage_time:.4f} seconds"
    )

except Exception:
    logging.error(
        "LOAD failed",
        exc_info=True
    )
    raise

logging.info("")
# ----------------------------------
# PIPELINE COMPLETED
# ----------------------------------

end_time = time.perf_counter()

pipeline_time = end_time - start_time

logging.info(
    "========== PIPELINE COMPLETED =========="
)

logging.info("")

logging.info(
    f"Total execution time: "
    f"{pipeline_time:.4f} seconds"
)

