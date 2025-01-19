import pytest

from models.db_models import *
from database import engine, Base, SessionLocal

from sqlalchemy import delete

@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """
    Create and drop the test database schema.
    """
    Base.metadata.create_all(bind=engine)  # Create all tables
    yield  # Tests run here
    Base.metadata.drop_all(bind=engine)  # Drop all tables after tests

# Fixture to provide a database session for tests
@pytest.fixture
def db():
    """
    Provide a clean database session for each test.
    """
    # Create a new session for the test
    session = SessionLocal()
    try:
        yield session
    finally:
        #Clean tables after session usage
        session.execute(delete(Employee))
        session.execute(delete(Job))
        session.execute(delete(Department))
        session.commit()
        # Close the session after the test
        session.close()