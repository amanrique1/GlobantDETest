from typing import Dict, Any, List

from sqlalchemy.exc import IntegrityError, OperationalError, DatabaseError
from sqlalchemy.orm import Session

from models.db_models import Department
from services.utils import process_csv
from utils.constants import *
from utils.log_manager import get_logger

# Ensure type safety
logger = get_logger()

async def create_departments_csv(file_content: bytes, db: Session) -> int:
    """
    Creates departments from a CSV file.

    Args:
        file_content (bytes): The content of the CSV file.
        db (Session): The SQLAlchemy database session.

    Returns:
        int: The number of departments created successfully.
    """
    records = await process_csv(file_content, Department.__table__.columns.keys())
    return await create_departments(records, db)


async def create_departments(data: List[Dict[str, Any]], db: Session) -> int:
    """
    Creates departments in the database in batches.

    Args:
        data (List[Dict[str, Any]]): A list of dictionaries, where each dictionary
                                    represents a department record.
        db (Session): The SQLAlchemy database session.

    Returns:
        int: The number of departments created successfully.

    Raises:
        Exception: If a duplicate record is found or an error occurs during processing.
    """
    try:
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            departments = [Department(**item) for item in batch]
            db.bulk_save_objects(departments)
            db.flush()  # Flush after each batch to persist to database

        db.commit()  # Commit all changes at the end
        return len(data)

    except IntegrityError as e:
        db.rollback()
        logger.info(f"Integrity error occurred: {e.orig}")
        if "duplicate key value violates unique constraint" in str(e.orig):
            raise Exception(UNIQUE_CONSTRAINT_VIOLATION_MSG)
        raise Exception(GENERIC_ERROR_MSG)

    except (OperationalError, DatabaseError) as e:
        db.rollback()
        logger.info(f"Database error occurred: {e.orig}")
        raise Exception(GENERIC_ERROR_MSG)

    except Exception as e:
        db.rollback()
        logger.info(f"An unexpected error occurred: {e}")
        raise Exception(GENERIC_ERROR_MSG)