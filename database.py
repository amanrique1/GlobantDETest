import os
import json

from google.cloud.secretmanager import SecretManagerServiceClient

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_utils import database_exists, create_database

# Database connection building from secrets manager

client = SecretManagerServiceClient()
name = "projects/770855044102/secrets/postgresdb-cred/versions/latest"
response = client.access_secret_version(request={"name": name})
payload = response.payload.data.decode("UTF-8")

secret = json.loads(payload)
DB_USER = secret["DB_USER"]
DB_PASSWORD = secret["DB_PASSWORD"]
DB_HOST = secret["DB_HOST"]
DB_PORT = secret["DB_PORT"]
DB_NAME = os.getenv("DB_NAME")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the database engine
engine = create_engine(DB_URL)

# Create database if doesn't exist
if not database_exists(engine.url):
    create_database(engine.url)

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