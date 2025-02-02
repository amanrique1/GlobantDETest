from typing import Dict, Any, List

from sqlalchemy.exc import IntegrityError, OperationalError, DatabaseError
from sqlalchemy.orm import Session

from models.db_models import Job
from services.utils import process_csv
from utils.constants import *
from utils.log_manager import SingletonLogger

# Ensure type safety
logger = SingletonLogger().get_logger()

async def create_jobs_csv(file_content: bytes, db: Session) -> int:
    """
    Creates jobs from a CSV file.

    Args:
        file_content (bytes): The content of the CSV file.
        db (Session): The SQLAlchemy database session.

    Returns:
        int: The number of jobs created successfully.
    """
    records = await process_csv(file_content, Job.__table__.columns.keys())
    return await create_jobs(records, db)


async def create_jobs(data: List[Dict[str, Any]], db: Session) -> int:
    """
    Creates jobs in the database in batches.

    Args:
        data (List[Dict[str, Any]]): A list of dictionaries, where each dictionary
                                    represents a job record.
        db (Session): The SQLAlchemy database session.

    Returns:
        int: The number of jobs created successfully.

    Raises:
        Exception: If a duplicate record is found or an error occurs during processing.
    """
    try:
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            jobs = [Job(**item) for item in batch]
            db.bulk_save_objects(jobs)
            db.flush()  # Flush after each batch to persist to database

        db.commit()  # Commit all changes at the end
        return len(data)

    except TypeError as e:
        db.rollback()
        logger.error(f"Type error occurred: {e}")
        raise Exception(DATA_TYPE_ERROR_MSG)

    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error occurred: {e.orig}")
        if "duplicate key value violates unique constraint" in str(e.orig):
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