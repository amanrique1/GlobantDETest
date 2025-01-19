from typing import Dict, Any, List

from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError, DatabaseError
from sqlalchemy.orm import Session
from sqlalchemy import text

from models.db_models import Department
from services.utils import process_csv
from utils.constants import *
from utils.log_manager import get_logger

# Ensure type safety
logger = get_logger()

# Get queries dir
QUERY_DIR = os.path.join(os.path.dirname(__file__), 'queries')

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

async def get_quarter_hires(db: Session) -> List[Dict[str, Any]]:
    """
    Executes the SQL query to fetch the number of hires per quarter from the database for the 2021 year.

    Args:
        db (Session): SQLAlchemy database session used to interact with the database.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the results of the query.
                              Each dictionary represents a row in the result set.

    Raises:
        Exception: If errors occur. Specific cases are catched and logged
    """
    query_file = os.path.join(QUERY_DIR, 'quarters_hires.sql')

    try:
        # Read the query from the file
        with open(query_file, 'r') as file:
            query = file.read()
    except FileNotFoundError:
        logger.error(f"Query file {query_file} not found.")
        raise Exception(GENERIC_ERROR_MSG)
    except IOError as e:
        logger.error(f"Error reading the query file {query_file}: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    try:
        # Execute the query
        result = db.execute(text(query))

        # Convert the result into a list of dictionaries (explicit column mapping)
        column_names = result.keys()  # Retrieve column names from the query result

        return [dict(zip(column_names, row)) for row in result]

    except OperationalError as e:
        logger.error(f"Database connection issue: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    except TimeoutError as e:
        logger.error(f"Query execution timed out: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error while executing the query: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    except Exception as e:
        logger.debug(type(e))
        logger.error(f"Unexpected error while executing the query: {e}")
        raise Exception(GENERIC_ERROR_MSG)

async def get_hires_over_avg(db: Session) -> List[Dict[str, Any]]:
    """
    List of ids, name and number of employees hired of each department that hired more
    employees than the mean of employees hired in 2021 for all the departments.

    Args:
        db (Session): SQLAlchemy database session used to interact with the database.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the results of the query.
                              Each dictionary represents a row in the result set.

    Raises:
        Exception: If errors occur. Specific cases are catched and logged
    """
    query_file = os.path.join(QUERY_DIR, 'hires_over_avg.sql')

    try:
        # Read the query from the file
        with open(query_file, 'r') as file:
            query = file.read()
    except FileNotFoundError:
        logger.error(f"Query file {query_file} not found.")
        raise Exception(GENERIC_ERROR_MSG)
    except IOError as e:
        logger.error(f"Error reading the query file {query_file}: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    try:
        # Execute the query
        result = db.execute(text(query))

        # Convert the result into a list of dictionaries (explicit column mapping)
        column_names = result.keys()  # Retrieve column names from the query result

        return [dict(zip(column_names, row)) for row in result]

    except OperationalError as e:
        logger.error(f"Database connection issue: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    except TimeoutError as e:
        logger.error(f"Query execution timed out: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error while executing the query: {e}")
        raise Exception(GENERIC_ERROR_MSG)

    except Exception as e:
        logger.error(f"Unexpected error while executing the query: {e}")
        raise Exception(GENERIC_ERROR_MSG)