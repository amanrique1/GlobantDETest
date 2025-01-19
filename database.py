import os
import json

import boto3
from botocore.exceptions import ClientError

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_utils import database_exists, create_database

# Database connection building from secrets manager
secret_name = "globant-db"
region_name = "us-east-1"

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

try:
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
except ClientError as e:
    raise e

secret = json.loads(get_secret_value_response['SecretString'])
DB_USER = secret["username"]
DB_PASSWORD = secret["password"]
DB_HOST = secret["host"]
DB_PORT = secret["port"]
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