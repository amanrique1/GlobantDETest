from typing import Dict, Any, List
from sqlalchemy.orm import Session

from models.db_models import Job
from services.utils import process_csv
from utils.constants import *
from utils.decorators import db_operation
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

@db_operation
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
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        jobs = [Job(**item) for item in batch]
        db.bulk_save_objects(jobs)
        db.flush()  # Flush after each batch to persist to database

    db.commit()  # Commit all changes at the end
    return len(data)