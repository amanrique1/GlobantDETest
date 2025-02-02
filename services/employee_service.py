from typing import Dict, Any, List

from sqlalchemy.exc import IntegrityError, OperationalError, DatabaseError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session

from models.db_models import Employee
from services.utils import process_csv
from utils.constants import *
from utils.decorators import db_operation
from utils.log_manager import SingletonLogger

# Ensure type safety
logger = SingletonLogger().get_logger()

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

@db_operation
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
