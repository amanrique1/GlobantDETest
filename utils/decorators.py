from sqlalchemy.exc import IntegrityError, OperationalError, DatabaseError
from sqlalchemy.orm import Session
import functools
from typing import Any, Callable, TypeVar
from utils.constants import *
from utils.log_manager import SingletonLogger

logger = SingletonLogger().get_logger()

# Type variable for the return type of the decorated function
T = TypeVar('T')

def db_operation(func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> T:
        # Find AsyncSession in arguments (args or kwargs)
        db = next((arg for arg in args if isinstance(arg, Session)), None) or \
             kwargs.get('db')

        if db is None:
            raise ValueError("No DB session object found in arguments.")

        try:
            result = await func(*args, **kwargs)
            db.flush()
            return result

        except TypeError as e:
            db.rollback()
            logger.error(f"Type error occurred in {func.__name__}: {str(e)}")
            raise Exception(DATA_TYPE_ERROR_MSG)

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
            logger.error(f"Database error occurred in {func.__name__}: {str(e.orig)}")
            raise Exception(GENERIC_ERROR_MSG)

        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
            raise Exception(GENERIC_ERROR_MSG)
    return wrapper