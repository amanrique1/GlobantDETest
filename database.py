import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Database connection building from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the database engine
engine = create_engine(DB_URL)

# Create a session local for session management
# autocommit = false for manual commits
# autoflush = false for manual flush, thinking on the batch operations
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a base class for declarative class definitions (models)
Base = declarative_base()

def get_db():
    """
    Creates and returns a database session.

    Yields:
        A database session object.

    The session is automatically closed after the request is finished.
    """
    db = SessionLocal()
    try:
        # Return the session and close it after the request is finished
        # Yield is used to allow the function to be a generator, and be used as a context manager
        yield db
    finally:
        # Close the session after the request is finished
        db.close()