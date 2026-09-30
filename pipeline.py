# ============================================================
# EMPLOYEE ETL PIPELINE
# ============================================================

import csv
from collections import Counter
import time
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (
    OperationalError,
    IntegrityError,
    DataError,
    ProgrammingError,
    DBAPIError,
)


# ============================================================
# 1. INITIAL CONFIGURATION
# ============================================================

MAX_INVALID_PERCENTAGE = 5

# Performance monitoring thresholds
MAX_PIPELINE_TIME_SECONDS = 60
MAX_DATABASE_LOAD_TIME_SECONDS = 10

# Retry configuration
MAX_DB_RETRIES = 3
BASE_RETRY_DELAY_SECONDS = 2

# Log rotation configuration
MAX_LOG_BYTES = 5 * 1024 * 1024
BACKUP_LOG_COUNT = 5


# ============================================================
# 2. LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


# ============================================================
# 3. DATABASE CONNECTION
# ============================================================

DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)


# ============================================================
# 4. PATH CONFIGURATION
# ============================================================

INPUT_DIR = Path("input")

OUTPUT_DIR = Path("output")

LOG_DIR = Path("logs")

EXTRACT_DIR = OUTPUT_DIR / "extracted"
EMPLOYEE_DIR = OUTPUT_DIR / "employees"
INVALID_DIR = OUTPUT_DIR / "invalid"
DUPLICATE_DIR = OUTPUT_DIR / "duplicates"
REPORT_DIR = OUTPUT_DIR / "reports"


# ============================================================
# INPUT FILE
# ============================================================

INPUT_FILE = INPUT_DIR / "employees_5000.csv"


# ============================================================
# OUTPUT FILES
# ============================================================

EXTRACTED_FILE = EXTRACT_DIR / "employees_5000.csv"

INVALID_FILE = INVALID_DIR / "invalid_employees.csv"

DUPLICATE_FILE = DUPLICATE_DIR / "duplicate_employees.csv"

SUMMARY_FILE = REPORT_DIR / "pipeline_summary.csv"

DEPARTMENT_SUMMARY_FILE = REPORT_DIR / "department_summary.csv"

VALIDATION_ERROR_FILE = REPORT_DIR / "validation_errors.csv"


# ============================================================
# CREATE DIRECTORIES
# ============================================================

for directory in [
    INPUT_DIR,
    OUTPUT_DIR,
    LOG_DIR,
    EXTRACT_DIR,
    EMPLOYEE_DIR,
    INVALID_DIR,
    DUPLICATE_DIR,
    REPORT_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)


# ============================================================
# 5. LOGGING CONFIGURATION
# ============================================================

LOG_FILE = LOG_DIR / "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        RotatingFileHandler(
            LOG_FILE,
            maxBytes=MAX_LOG_BYTES,
            backupCount=BACKUP_LOG_COUNT,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)


# ============================================================
# 6. LOGGING HELPERS
# ============================================================


def log_section(title):
    """
    Print a clean section heading in the log.
    """

    logging.info("")
    logging.info("=" * 70)
    logging.info(title)
    logging.info("=" * 70)


def log_stage_complete(stage, elapsed):
    """
    Log completion time for a pipeline stage.
    """

    logging.info(f"[{stage}] completed in {elapsed:.4f} seconds")


def send_alert(message, level="WARNING"):
    """
    Alert hook.

    For this local ETL project, alerts are written to the log.
    Later this function can be connected to email, Slack,
    Teams, PagerDuty, etc.
    """

    if level.upper() == "ERROR":
        logging.error(f"ALERT | {message}")
    else:
        logging.warning(f"ALERT | {message}")


def monitor_stage(stage, elapsed, threshold=None):
    """
    Record performance information and optionally raise an alert
    when a stage exceeds its configured threshold.
    """

    logging.info(f"PERFORMANCE | stage={stage} | duration={elapsed:.4f}s")

    if threshold is not None and elapsed > threshold:
        send_alert(
            f"{stage} exceeded performance threshold | "
            f"duration={elapsed:.4f}s | "
            f"threshold={threshold:.4f}s"
        )


# ============================================================
# 7. CSV FIELD DEFINITIONS
# ============================================================

FIELDNAMES = [
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
    "validation_error",
]


# ============================================================
# 8. DEPARTMENTS
# ============================================================

DEPARTMENTS = [
    "Engineering",
    "Finance",
    "Marketing",
    "Sales",
    "HR",
]


# ============================================================
# 9. HELPER FUNCTION - EXPORT CSV
# ============================================================


def export_csv(file_path, records, fieldnames):
    """
    Export records to a CSV file.
    """

    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()

        writer.writerows(records)


# ============================================================
# 10. HELPER FUNCTION - DATABASE COMPARISON
# ============================================================


def compare_with_database(unique_records, existing_records_by_id):
    """
    Compare CSV records with existing database records.

    Returns:
        new_records
        changed_records
        unchanged_records
    """

    new_records = []

    changed_records = []

    unchanged_records = []

    # Fields that determine whether
    # an existing employee has changed.

    compare_fields = [
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
        "validation_error",
    ]

    # --------------------------------------------------------
    # Compare each CSV record
    # --------------------------------------------------------

    for record in unique_records:
        employee_id = record["id"]

        # ----------------------------------------------------
        # NEW RECORD
        # ----------------------------------------------------

        if employee_id not in existing_records_by_id:
            new_records.append(record)

            continue

        # ----------------------------------------------------
        # EXISTING RECORD
        # ----------------------------------------------------

        existing_record = existing_records_by_id[employee_id]

        changed = False

        # ----------------------------------------------------
        # CHECK ALL FIELDS
        # ----------------------------------------------------

        for field in compare_fields:
            csv_value = record.get(field)

            db_value = existing_record.get(field)

            if csv_value != db_value:
                changed = True

                break

        # ----------------------------------------------------
        # CLASSIFY RECORD
        # ----------------------------------------------------

        if changed:
            changed_records.append(record)

        else:
            unchanged_records.append(record)

    return (new_records, changed_records, unchanged_records)


# ============================================================
# 11. DATABASE ENGINE
# ============================================================

def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


# ============================================================
# DATABASE ERROR CLASSIFICATION
# ============================================================


def classify_database_error(error):
    """
    Classify database errors into:
        - retryable
        - non_retryable
        - unknown

    Retryable errors are generally temporary connection/
    availability problems.

    Non-retryable errors generally require fixing SQL/data/schema.
    """

    if isinstance(error, OperationalError):
        return "retryable"

    if isinstance(
        error,
        (
            IntegrityError,
            DataError,
            ProgrammingError,
        ),
    ):
        return "non_retryable"

    if isinstance(error, DBAPIError):
        if getattr(error, "connection_invalidated", False):
            return "retryable"

        return "non_retryable"

    return "unknown"


# ============================================================
# BATCH DATABASE LOAD
# ============================================================


def load_batch(connection, records):
    """
    Insert/update all records in one SQLAlchemy batch.
    """

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
        records,
    )


# ============================================================
# DATABASE TRANSACTION + RETRY
# ============================================================


def execute_database_with_retry(operation, operation_name, max_retries=MAX_DB_RETRIES):
    """
    Execute a complete database operation with retries.

    Every retry creates a NEW transaction/connection.
    A failed transaction is rolled back before the next attempt.
    """

    for attempt in range(1, max_retries + 1):
        transaction_start = time.perf_counter()

        try:
            logging.info(f"{operation_name} attempt {attempt}/{max_retries} started.")

            # NEW transaction and connection for every attempt.
            with get_engine().begin() as connection:
                operation(connection)

            transaction_elapsed = time.perf_counter() - transaction_start

            logging.info(f"{operation_name} transaction committed | attempt={attempt}")

            return transaction_elapsed

        except Exception as error:
            error_type = classify_database_error(error)

            logging.exception(
                f"{operation_name} failed | "
                f"attempt={attempt}/{max_retries} | "
                f"classification={error_type}"
            )

            if error_type != "retryable":
                send_alert(
                    f"Non-retryable database error during "
                    f"{operation_name}: "
                    f"{type(error).__name__}: {error}",
                    level="ERROR",
                )

                raise

            if attempt == max_retries:
                send_alert(
                    f"{operation_name} failed after {max_retries} attempts: {error}",
                    level="ERROR",
                )

                raise

            delay = BASE_RETRY_DELAY_SECONDS**attempt

            logging.warning(
                f"Retryable database error detected during "
                f"{operation_name}. Fresh transaction will be "
                f"created after {delay} seconds."
            )

            time.sleep(delay)

    raise RuntimeError("Unexpected database retry state.")


# ============================================================
# DATABASE SETUP
# ============================================================


def setup_database(connection):
    """
    Create the employees table and required indexes.
    """

    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY,
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
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS
            idx_employees_department
            ON employees(department)
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS
            idx_employees_status
            ON employees(employment_status)
            """
        )
    )

    connection.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS
            idx_employees_salary
            ON employees(salary)
            """
        )
    )


# ============================================================
# DATABASE LOAD WITH RETRY
# ============================================================


def load_with_retry(records, max_retries=MAX_DB_RETRIES):
    """
    Load records using the common database retry mechanism.
    """

    transaction_elapsed = execute_database_with_retry(
        lambda connection: load_batch(connection, records),
        "DATABASE LOAD",
        max_retries=max_retries,
    )

    logging.info(f"Database transaction committed | records={len(records)}")

    monitor_stage("DATABASE LOAD", transaction_elapsed, MAX_DATABASE_LOAD_TIME_SECONDS)

    return transaction_elapsed


# ============================================================
# VALIDATION HELPER
# ============================================================


def validate_employee(row):
    """
    Validate a single employee record.

    Returns:
        tuple:
            is_valid (bool)
            errors (list)
    """

    errors = []

    # --------------------------------------------------------
    # ID validation
    # --------------------------------------------------------

    if row.get("id") is None:
        errors.append("Invalid ID")

    elif row["id"] <= 0:
        errors.append("ID must be greater than 0")

    # --------------------------------------------------------
    # Name validation
    # --------------------------------------------------------

    if not row.get("name"):
        errors.append("Name is empty")

    # --------------------------------------------------------
    # Age validation
    # --------------------------------------------------------

    if row.get("age") is None:
        errors.append("Invalid age")

    elif row["age"] < 18:
        errors.append("Age must be at least 18")

    # --------------------------------------------------------
    # Salary validation
    # --------------------------------------------------------

    if row.get("salary") is None:
        errors.append("Invalid salary")

    elif row["salary"] <= 0:
        errors.append("Salary must be greater than 0")

    # --------------------------------------------------------
    # Experience validation
    # --------------------------------------------------------

    if row.get("experience") is None:
        errors.append("Invalid experience")

    elif row["experience"] < 0:
        errors.append("Experience cannot be negative")

    # --------------------------------------------------------
    # Joining year validation
    # --------------------------------------------------------

    if row.get("joining_year") is None:
        errors.append("Invalid joining year")

    elif row["joining_year"] < 1900:
        errors.append("Joining year must be >= 1900")

    # --------------------------------------------------------
    # Performance validation
    # --------------------------------------------------------

    if row.get("performance_score") is None:
        errors.append("Invalid performance score")

    elif not (0 <= row["performance_score"] <= 10):
        errors.append("Performance score must be between 0 and 10")

    # --------------------------------------------------------
    # Department validation
    # --------------------------------------------------------

    if not row.get("department"):
        errors.append("Department is empty")

    # --------------------------------------------------------
    # Validation result
    # --------------------------------------------------------

    is_valid = len(errors) == 0

    return is_valid, errors


# ============================================================


def main():
    """Run the complete employee ETL pipeline."""

    start_time = time.perf_counter()
    pipeline_start_time = datetime.now()
    pipeline_status = "SUCCESS"

    # 12. PIPELINE START
    # ============================================================

    log_section("EMPLOYEE ETL PIPELINE - START")

    logging.info(f"Input file : {INPUT_FILE}")

    logging.info(f"Database   : {DB_NAME}")

    logging.info(f"Start time : {pipeline_start_time}")


    # ============================================================
    # STEP 1 - DATABASE SETUP
    # ============================================================

    log_section("STEP 1 - DATABASE SETUP")

    stage_start = time.perf_counter()

    try:
        database_setup_time = execute_database_with_retry(
            setup_database, "DATABASE SETUP", max_retries=MAX_DB_RETRIES
        )

        logging.info("Database table 'employees' is ready.")

        logging.info("Index ready: idx_employees_department")

        logging.info("Index ready: idx_employees_status")

        logging.info("Index ready: idx_employees_salary")

    except Exception as error:
        pipeline_status = "FAILED"

        logging.exception(f"Database setup failed: {error}")

        raise


    elapsed = time.perf_counter() - stage_start

    monitor_stage("DATABASE SETUP", elapsed)

    log_stage_complete("DATABASE SETUP", elapsed)


    # ============================================================
    # STEP 2 - EXTRACT
    # ============================================================

    log_section("STEP 2 - EXTRACT")

    stage_start = time.perf_counter()

    employees = []

    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            employees = list(reader)

        # --------------------------------------------------------
        # Empty file check
        # --------------------------------------------------------

        if not employees:
            logging.warning(f"{INPUT_FILE} contains no records.")

            pipeline_status = "FAILED"

            raise ValueError("Input CSV contains no records.")

        logging.info(f"Records extracted: {len(employees)}")

    except FileNotFoundError:
        pipeline_status = "FAILED"

        logging.exception(f"Input file not found: {INPUT_FILE}")

        raise

    except Exception as error:
        pipeline_status = "FAILED"

        logging.exception(f"Extraction failed: {error}")

        raise


    elapsed = time.perf_counter() - stage_start

    monitor_stage("EXTRACT", elapsed)

    log_stage_complete("EXTRACT", elapsed)


    # ============================================================
    # STEP 3 - CLEANING
    # ============================================================

    log_section("STEP 3 - CLEANING")

    stage_start = time.perf_counter()

    numeric_integer_fields = [
        "id",
        "age",
        "salary",
        "experience",
        "joining_year",
    ]

    numeric_float_fields = ["performance_score"]

    cleaned_records = []


    for row in employees:
        cleaned_row = {}

        # --------------------------------------------------------
        # Clean string fields
        # --------------------------------------------------------

        for key, value in row.items():
            if value is None:
                cleaned_row[key] = ""

            else:
                cleaned_row[key] = value.strip()

        # --------------------------------------------------------
        # Convert integer fields
        # --------------------------------------------------------

        for field in numeric_integer_fields:
            try:
                cleaned_row[field] = int(cleaned_row[field])

            except (ValueError, TypeError):
                cleaned_row[field] = None

                logging.warning(
                    f"Invalid integer value | "
                    f"field={field} | "
                    f"value={cleaned_row.get(field)}"
                )

        # --------------------------------------------------------
        # Convert float fields
        # --------------------------------------------------------

        for field in numeric_float_fields:
            try:
                cleaned_row[field] = float(cleaned_row[field])

            except (ValueError, TypeError):
                cleaned_row[field] = None

                logging.warning(f"Invalid float value | field={field}")

        # --------------------------------------------------------
        # Make sure validation_error exists
        # --------------------------------------------------------

        cleaned_row["validation_error"] = ""

        cleaned_records.append(cleaned_row)


    logging.info(f"Records cleaned: {len(cleaned_records)}")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("CLEANING", elapsed)

    log_stage_complete("CLEANING", elapsed)


    # ============================================================
    # STEP 4 - VALIDATION
    # ============================================================

    log_section("STEP 4 - VALIDATION")

    stage_start = time.perf_counter()

    valid_records = []

    invalid_records = []

    validation_errors = []


    for row in cleaned_records:
        # --------------------------------------------------------
        # Validate employee
        # --------------------------------------------------------

        is_valid, errors = validate_employee(row)

        # --------------------------------------------------------
        # Store validation result
        # --------------------------------------------------------

        if not is_valid:
            error_message = "; ".join(errors)

            row["validation_error"] = error_message

            invalid_records.append(row)

            validation_errors.append(
                {
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "validation_error": error_message,
                }
            )

        else:
            row["validation_error"] = ""

            valid_records.append(row)


    # ============================================================
    # VALIDATION STATISTICS
    # ============================================================

    total_records = len(cleaned_records)

    valid_count = len(valid_records)

    invalid_count = len(invalid_records)


    if total_records > 0:
        valid_percentage = (valid_count / total_records) * 100

        invalid_percentage = (invalid_count / total_records) * 100

    else:
        valid_percentage = 0

        invalid_percentage = 0


    logging.info(f"Valid records   : {valid_count}")

    logging.info(f"Invalid records : {invalid_count}")

    logging.info(f"Valid rate      : {valid_percentage:.2f}%")

    logging.info(f"Invalid rate    : {invalid_percentage:.2f}%")


    # ============================================================
    # VALIDATION ERROR SUMMARY
    # ============================================================

    if invalid_records:
        error_counter = Counter()

        for row in invalid_records:
            errors = row["validation_error"].split("; ")

            for error in errors:
                error_counter[error] += 1

        logging.info("Validation error summary:")

        for error, count in error_counter.items():
            logging.info(f"  {error}: {count}")

    else:
        logging.info("Validation errors: none")


    # ============================================================
    # DATA QUALITY THRESHOLD
    # ============================================================

    if invalid_percentage > MAX_INVALID_PERCENTAGE:
        pipeline_status = "FAILED"

        logging.error(
            f"Invalid percentage "
            f"{invalid_percentage:.2f}% exceeds "
            f"maximum allowed "
            f"{MAX_INVALID_PERCENTAGE:.2f}%"
        )

        raise ValueError("Data quality threshold exceeded.")

    else:
        logging.info(f"Data quality: PASS | Threshold: {MAX_INVALID_PERCENTAGE:.2f}%")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("VALIDATION", elapsed)

    log_stage_complete("VALIDATION", elapsed)


    # ============================================================
    # STEP 5 - DEDUPLICATION
    # ============================================================

    log_section("STEP 5 - DEDUPLICATION")

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


    unique_records_count = len(unique_records)

    duplicate_count = len(duplicate_records)


    logging.info(f"Unique records    : {unique_records_count}")

    logging.info(f"Duplicate records : {duplicate_count}")


    if duplicate_records:
        logging.info("Duplicate records will be exported during EXPORT.")

    else:
        logging.info("No duplicate records detected.")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("DEDUPLICATION", elapsed)

    log_stage_complete("DEDUPLICATION", elapsed)


    # ============================================================
    # STEP 6 - FILTERING
    # ============================================================

    log_section("STEP 6 - FILTERING")

    stage_start = time.perf_counter()


    department_employees = {department: [] for department in DEPARTMENTS}


    for row in unique_records:
        department = row.get("department")

        if department in department_employees:
            department_employees[department].append(row)


    # ------------------------------------------------------------
    # Department counts
    # ------------------------------------------------------------

    for department in DEPARTMENTS:
        count = len(department_employees[department])

        logging.info(f"{department:<12} : {count}")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("FILTERING", elapsed)

    log_stage_complete("FILTERING", elapsed)


    # ============================================================
    # STEP 7 - AGGREGATION
    # ============================================================

    log_section("STEP 7 - AGGREGATION")

    stage_start = time.perf_counter()

    department_summary = []


    for department in DEPARTMENTS:
        records = department_employees[department]

        if not records:
            logging.info(f"{department:<12} | employees=0")

            continue

        salaries = [row["salary"] for row in records if row.get("salary") is not None]

        ages = [row["age"] for row in records if row.get("age") is not None]

        experiences = [
            row["experience"] for row in records if row.get("experience") is not None
        ]

        performances = [
            row["performance_score"]
            for row in records
            if row.get("performance_score") is not None
        ]

        active_count = sum(1 for row in records if row.get("employment_status") == "Active")

        inactive_count = sum(
            1 for row in records if row.get("employment_status") == "Inactive"
        )

        # --------------------------------------------------------
        # Calculations
        # --------------------------------------------------------

        employee_count = len(records)

        total_salary = sum(salaries)

        average_salary = total_salary / len(salaries) if salaries else 0

        maximum_salary = max(salaries) if salaries else 0

        minimum_salary = min(salaries) if salaries else 0

        average_age = sum(ages) / len(ages) if ages else 0

        maximum_age = max(ages) if ages else 0

        minimum_age = min(ages) if ages else 0

        average_experience = sum(experiences) / len(experiences) if experiences else 0

        maximum_experience = max(experiences) if experiences else 0

        minimum_experience = min(experiences) if experiences else 0

        average_performance = sum(performances) / len(performances) if performances else 0

        maximum_performance = max(performances) if performances else 0

        minimum_performance = min(performances) if performances else 0

        # --------------------------------------------------------
        # Department summary record
        # --------------------------------------------------------

        summary = {
            "department": department,
            "employee_count": employee_count,
            "active_count": active_count,
            "inactive_count": inactive_count,
            "total_salary": total_salary,
            "average_salary": round(average_salary, 2),
            "maximum_salary": maximum_salary,
            "minimum_salary": minimum_salary,
            "average_age": round(average_age, 2),
            "maximum_age": maximum_age,
            "minimum_age": minimum_age,
            "average_experience": round(average_experience, 2),
            "maximum_experience": maximum_experience,
            "minimum_experience": minimum_experience,
            "average_performance": round(average_performance, 2),
            "maximum_performance": maximum_performance,
            "minimum_performance": minimum_performance,
        }

        department_summary.append(summary)

        # --------------------------------------------------------
        # Compact logging
        # --------------------------------------------------------

        logging.info(
            f"{department:<12} | "
            f"employees={employee_count} | "
            f"active={active_count} | "
            f"inactive={inactive_count} | "
            f"avg_salary={average_salary:.2f} | "
            f"avg_age={average_age:.2f} | "
            f"avg_exp={average_experience:.2f} | "
            f"avg_perf={average_performance:.2f}"
        )


    elapsed = time.perf_counter() - stage_start

    monitor_stage("AGGREGATION", elapsed)

    log_stage_complete("AGGREGATION", elapsed)


    # ============================================================
    # STEP 8 - SORTING
    # ============================================================

    log_section("STEP 8 - SORTING")

    stage_start = time.perf_counter()


    for department in DEPARTMENTS:
        department_employees[department].sort(key=lambda row: row["salary"], reverse=True)


    logging.info(f"Sorted {unique_records_count} records by salary DESC.")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("SORTING", elapsed)

    log_stage_complete("SORTING", elapsed)


    # ============================================================
    # STEP 9 - HIGHEST PAID EMPLOYEE
    # ============================================================

    log_section("STEP 9 - HIGHEST PAID EMPLOYEE")

    stage_start = time.perf_counter()


    for department in DEPARTMENTS:
        records = department_employees[department]

        if not records:
            logging.info(f"{department:<12} | no employees")

            continue

        highest_paid = records[0]

        logging.info(
            f"{department:<12} | {highest_paid['name']} | salary={highest_paid['salary']}"
        )


    elapsed = time.perf_counter() - stage_start

    monitor_stage("HIGHEST PAID", elapsed)

    log_stage_complete("HIGHEST PAID", elapsed)


    # ============================================================
    # STEP 10 - EXPORT
    # ============================================================

    log_section("STEP 10 - EXPORT")

    stage_start = time.perf_counter()


    # ------------------------------------------------------------
    # Export department files
    # ------------------------------------------------------------

    for department in DEPARTMENTS:
        records = department_employees[department]

        department_file = EMPLOYEE_DIR / f"{department.lower()}_employees_5000.csv"

        export_csv(department_file, records, FIELDNAMES)

        logging.info(f"Exported {len(records):>5} records | {department_file}")


    # ------------------------------------------------------------
    # Export invalid records
    # ------------------------------------------------------------

    export_csv(INVALID_FILE, invalid_records, FIELDNAMES)

    logging.info(f"Exported {len(invalid_records):>5} invalid records | {INVALID_FILE}")


    # ------------------------------------------------------------
    # Export duplicate records
    # ------------------------------------------------------------

    export_csv(DUPLICATE_FILE, duplicate_records, FIELDNAMES)

    logging.info(
        f"Exported {len(duplicate_records):>5} duplicate records | {DUPLICATE_FILE}"
    )


    # ------------------------------------------------------------
    # Export validation errors
    # ------------------------------------------------------------

    validation_error_fields = ["id", "name", "validation_error"]


    export_csv(VALIDATION_ERROR_FILE, validation_errors, validation_error_fields)

    logging.info(
        f"Exported {len(validation_errors):>5} validation errors | {VALIDATION_ERROR_FILE}"
    )


    # ------------------------------------------------------------
    # Export department summary
    # ------------------------------------------------------------

    if department_summary:
        department_fields = [
            "department",
            "employee_count",
            "active_count",
            "inactive_count",
            "total_salary",
            "average_salary",
            "maximum_salary",
            "minimum_salary",
            "average_age",
            "maximum_age",
            "minimum_age",
            "average_experience",
            "maximum_experience",
            "minimum_experience",
            "average_performance",
            "maximum_performance",
            "minimum_performance",
        ]

        export_csv(DEPARTMENT_SUMMARY_FILE, department_summary, department_fields)

        logging.info(f"Exported department summary | {DEPARTMENT_SUMMARY_FILE}")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("EXPORT", elapsed)

    log_stage_complete("EXPORT", elapsed)


    # ============================================================
    # STEP 11 - INCREMENTAL LOAD
    # ============================================================

    log_section("STEP 11: INCREMENTAL LOAD")

    stage_start = time.perf_counter()

    try:
        # --------------------------------------------------------
        # Read existing database records
        # --------------------------------------------------------

        with get_engine.connect() as connection:
            result = connection.execute(
                text("""
                    SELECT
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
                    FROM employees
                """)
            )

            existing_records_by_id = {row.id: dict(row._mapping) for row in result}

        logging.info(f"Existing database records: {len(existing_records_by_id)}")

        # --------------------------------------------------------
        # Compare incoming data with database
        # --------------------------------------------------------

        (new_records, changed_records, unchanged_records) = compare_with_database(
            unique_records, existing_records_by_id
        )

        logging.info(f"New records: {len(new_records)}")

        logging.info(f"Changed records: {len(changed_records)}")

        logging.info(f"Unchanged records: {len(unchanged_records)}")

        # --------------------------------------------------------
        # Records that actually need loading
        # --------------------------------------------------------

        records_to_load = new_records + changed_records

        logging.info(f"Records to load: {len(records_to_load)}")

        # --------------------------------------------------------
        # Batch load with transaction + retry
        # --------------------------------------------------------

        database_load_time = 0

        if records_to_load:
            database_load_time = load_with_retry(
                records_to_load, max_retries=MAX_DB_RETRIES
            )

            logging.info(f"Batch loaded {len(records_to_load)} records.")

        else:
            logging.info("No new or changed records. Database is already up to date.")

        elapsed = time.perf_counter() - stage_start

        monitor_stage("INCREMENTAL LOAD", elapsed)

        log_stage_complete("INCREMENTAL LOAD", elapsed)

    except Exception as error:
        logging.exception(f"Incremental load failed: {error}")

        pipeline_status = "FAILED"

        raise


    # ============================================================
    # STEP 12 - POST-LOAD VALIDATION
    # ============================================================

    log_section("STEP 12 - POST-LOAD VALIDATION")

    stage_start = time.perf_counter()


    with get_engine.connect() as connection:
        result = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM employees
                """
            )
        )

        database_row_count = result.scalar()


    expected_row_count = unique_records_count


    if database_row_count == expected_row_count:
        post_load_validation = "PASS"

        logging.info(f"Database rows : {database_row_count}")

        logging.info(f"Expected rows : {expected_row_count}")

        logging.info("Post-load validation: PASS")

    else:
        post_load_validation = "FAILED"

        pipeline_status = "FAILED"

        logging.error(f"Database rows : {database_row_count}")

        logging.error(f"Expected rows : {expected_row_count}")

        logging.error("Post-load validation: FAILED")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("POST-LOAD VALIDATION", elapsed)

    log_stage_complete("POST-LOAD VALIDATION", elapsed)


    # ============================================================
    # STEP 13 - DATA QUALITY SUMMARY
    # ============================================================

    log_section("STEP 13 - DATA QUALITY SUMMARY")

    stage_start = time.perf_counter()


    duplicate_percentage = duplicate_count / total_records * 100 if total_records > 0 else 0


    unchanged_percentage = (
        len(unchanged_records) / unique_records_count * 100
        if unique_records_count > 0
        else 0
    )


    logging.info(f"Total records      : {total_records}")

    logging.info(f"Valid records      : {valid_count}")

    logging.info(f"Invalid records    : {invalid_count}")

    logging.info(f"Duplicate records  : {duplicate_count}")

    logging.info(f"Unique records     : {unique_records_count}")

    logging.info(f"New records        : {len(new_records)}")

    logging.info(f"Changed records    : {len(changed_records)}")

    logging.info(f"Unchanged records  : {len(unchanged_records)}")

    logging.info(f"Duplicate rate     : {duplicate_percentage:.2f}%")

    logging.info(f"Unchanged rate     : {unchanged_percentage:.2f}%")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("DATA QUALITY SUMMARY", elapsed)

    log_stage_complete("DATA QUALITY SUMMARY", elapsed)


    # ============================================================
    # STEP 14 - REPORT GENERATION
    # ============================================================

    log_section("STEP 14 - REPORT GENERATION")

    stage_start = time.perf_counter()


    pipeline_end_time = datetime.now()

    execution_time = time.perf_counter() - start_time


    # ------------------------------------------------------------
    # Pipeline-level performance monitoring
    # ------------------------------------------------------------

    monitor_stage("TOTAL PIPELINE", execution_time, MAX_PIPELINE_TIME_SECONDS)


    if pipeline_status == "FAILED":
        send_alert("Pipeline finished with FAILED status.", level="ERROR")


    # ============================================================
    # PIPELINE SUMMARY
    # ============================================================

    summary_record = {
        "run_timestamp": datetime.now(),
        "pipeline_start_time": (pipeline_start_time),
        "pipeline_end_time": (pipeline_end_time),
        "pipeline_status": (pipeline_status),
        "total_records": (total_records),
        "valid_records": (valid_count),
        "valid_percentage": round(valid_percentage, 2),
        "invalid_records": (invalid_count),
        "invalid_percentage": round(invalid_percentage, 2),
        "duplicate_records": (duplicate_count),
        "duplicate_percentage": round(duplicate_percentage, 2),
        "unique_records": (unique_records_count),
        "new_records": (len(new_records)),
        "changed_records": (len(changed_records)),
        "unchanged_records": (len(unchanged_records)),
        "unchanged_percentage": round(unchanged_percentage, 2),
        "records_loaded": (len(records_to_load)),
        "database_row_count": (database_row_count),
        "post_load_validation": (post_load_validation),
        "execution_time_seconds": round(execution_time, 4),
        "database_load_time_seconds": round(database_load_time, 4),
        "pipeline_performance_status": (
            "SLOW" if execution_time > MAX_PIPELINE_TIME_SECONDS else "PASS"
        ),
    }


    summary_fields = list(summary_record.keys())


    export_csv(SUMMARY_FILE, [summary_record], summary_fields)


    logging.info(f"Pipeline summary exported | {SUMMARY_FILE}")


    elapsed = time.perf_counter() - stage_start

    monitor_stage("REPORT GENERATION", elapsed)

    log_stage_complete("REPORT GENERATION", elapsed)


    # ============================================================
    # FINAL PIPELINE SUMMARY
    # ============================================================

    log_section("PIPELINE SUMMARY")


    logging.info(f"Status            : {pipeline_status}")

    logging.info(f"Records processed : {total_records}")

    logging.info(f"Valid             : {valid_count}")

    logging.info(f"Invalid           : {invalid_count}")

    logging.info(f"Duplicates        : {duplicate_count}")

    logging.info(f"New               : {len(new_records)}")

    logging.info(f"Changed           : {len(changed_records)}")

    logging.info(f"Unchanged         : {len(unchanged_records)}")

    logging.info(f"Database loaded   : {len(records_to_load)}")

    logging.info(f"Database rows     : {database_row_count}")

    logging.info(f"Execution time    : {execution_time:.4f} seconds")

    logging.info(f"Database load time: {database_load_time:.4f} seconds")

    logging.info(
        f"Performance status: "
        f"{'SLOW' if execution_time > MAX_PIPELINE_TIME_SECONDS else 'PASS'}"
    )


    # ============================================================
    # PIPELINE END
    # ============================================================

    log_section("EMPLOYEE ETL PIPELINE - END")

    logging.info(f"Pipeline finished at: {pipeline_end_time}")

    logging.info(f"Final status: {pipeline_status}")


if __name__ == "__main__":
    main()
