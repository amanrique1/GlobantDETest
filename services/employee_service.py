from typing import Dict, Any, List

from sqlalchemy.exc import IntegrityError, OperationalError, DatabaseError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session

from models.db_models import Employee
from services.utils import process_csv
from utils.constants import *
from utils.log_manager import get_logger

# Ensure type safety
logger = get_logger()

async def create_employees_csv(file_content: bytes, db: Session) -> int:
    """
    Creates employees from a CSV file.

    Args:
        file_content (bytes): The content of the CSV file.
        db (Session): The SQLAlchemy database session.

    Returns:
        int: The number of employees created successfully.
    """
    records = await process_csv(file_content, Employee.__table__.columns.keys())
    return await create_employees(records, db)


async def create_employees(data: List[Dict[str, Any]], db: Session) -> int:
    """
    Creates employees in the database in batches.

    Args:
        data (List[Dict[str, Any]]): A list of dictionaries, where each dictionary
                                    represents a employee record.
        db (Session): The SQLAlchemy database session.

    Returns:
        int: The number of employees created successfully.

    Raises:
        Exception: If a duplicate record is found or an error occurs during processing.
    """
    try:
        nullable_columns = [column.key for column in inspect(Employee).columns if column.nullable]
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            # Replace empty values in nullable columns for None
            employees = [
                Employee(**{
                    key: (None if isinstance(value, str) and value.strip() == "" and key in nullable_columns else value)
                    for key, value in item.items()
                })
                for item in batch
            ]
            db.bulk_save_objects(employees)
            db.flush()  # Flush after each batch to persist to database

        db.commit()  # Commit all changes at the end
        return len(data)

    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error occurred: {e.orig}")
        error_message = str(e.orig).lower()

        if "foreign key violation" in error_message or "foreign key constraint" in error_message:
            raise Exception(FOREIGN_KEY_VIOLATION_MSG)
        elif "duplicate key value violates unique constraint" in error_message:
            raise Exception(UNIQUE_CONSTRAINT_VIOLATION_MSG)
        raise Exception(GENERIC_ERROR_MSG)

    except (OperationalError, DatabaseError) as e:
        db.rollback()
        logger.error(f"Database error occurred: {e.orig}")
        raise Exception(GENERIC_ERROR_MSG)

    except Exception as e:
        db.rollback()
        logger.error(f"An unexpected error occurred: {e}")
        raise Exception(GENERIC_ERROR_MSG)