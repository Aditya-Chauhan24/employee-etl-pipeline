import os
import unittest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


load_dotenv()


DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)


class TestDatabaseRollback(unittest.TestCase):

    def test_transaction_rolls_back_after_failure(self):

        test_id = 999999

        # --------------------------------------------------
        # Make sure test record does not already exist
        # --------------------------------------------------

        with engine.begin() as connection:

            connection.execute(
                text(
                    "DELETE FROM employees "
                    "WHERE id = :id"
                ),
                {"id": test_id}
            )

        # --------------------------------------------------
        # Start transaction
        # --------------------------------------------------

        try:

            with engine.begin() as connection:

                # First operation succeeds
                connection.execute(
                    text(
                        """
                        INSERT INTO employees
                        (
                            id,
                            name,
                            age,
                            salary
                        )
                        VALUES
                        (
                            :id,
                            :name,
                            :age,
                            :salary
                        )
                        """
                    ),
                    {
                        "id": test_id,
                        "name": "Rollback Test",
                        "age": 25,
                        "salary": 50000
                    }
                )

                # Second operation deliberately fails
                connection.execute(
                    text(
                        """
                        INSERT INTO employees
                        (
                            id,
                            name,
                            age,
                            salary
                        )
                        VALUES
                        (
                            :id,
                            :name,
                            :age,
                            :salary
                        )
                        """
                    ),
                    {
                        "id": test_id,
                        "name": "Duplicate Test",
                        "age": 30,
                        "salary": 60000
                    }
                )

        except Exception:
            # Expected failure
            pass

        # --------------------------------------------------
        # Verify rollback
        # --------------------------------------------------

        with engine.connect() as connection:

            result = connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM employees
                    WHERE id = :id
                    """
                ),
                {"id": test_id}
            )

            count = result.scalar()

        # --------------------------------------------------
        # Assert
        # --------------------------------------------------

        self.assertEqual(
            count,
            0,
            "Transaction was not rolled back."
        )


if __name__ == "__main__":
    unittest.main()
