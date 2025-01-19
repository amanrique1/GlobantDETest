import pytest

from sqlalchemy.orm import Session

from models.db_models import Department
from services.department_service import create_departments, create_departments_csv
from utils.constants import (
    UNIQUE_CONSTRAINT_VIOLATION_MSG,
    DATA_TYPE_ERROR_MSG,
    BATCH_SIZE)
from tests.generator import (
    get_valid_departments,
    get_invalid_departments_id,
    get_invalid_departments_name,
    list_of_dicts_to_csv_bytes,
    list_of_dicts_to_json_bytes)

@pytest.mark.asyncio
async def test_create_departments_successfully(db: Session):
    """
    Tests successful creation of departments and verifies their count and existence in the database.
    """
    size = int(BATCH_SIZE*1.6)
    valid_departments = get_valid_departments(size)
    limit = int(BATCH_SIZE * 0.5)

    # Under BATCH_SIZE scenario
    created_count = await create_departments(valid_departments[:limit], db)
    assert created_count == limit

    # Over BATCH_SIZE scenario
    created_count = await create_departments(valid_departments[limit:], db)
    assert created_count == size-limit

    # Verify the departments were added
    departments = db.query(Department).all()
    assert len(departments) == size
    assert {d.id for d in departments} == {d["id"] for d in valid_departments}

@pytest.mark.asyncio
async def test_create_departments_successfully_csv(db: Session):
    """
    Tests successful creation of departments and verifies their count and existence in the database. Based on a file content
    """
    size = int(BATCH_SIZE*1.6)
    valid_departments = get_valid_departments(size)
    columns = valid_departments[0].keys()
    limit = int(BATCH_SIZE * 0.5)

    # Under BATCH_SIZE scenario
    created_count = await create_departments_csv(list_of_dicts_to_csv_bytes(valid_departments[:limit], columns), db)
    assert created_count == limit

    # Over BATCH_SIZE scenario
    created_count = await create_departments_csv(list_of_dicts_to_csv_bytes(valid_departments[limit:], columns), db)
    assert created_count == size-limit

    # Verify the departments were added
    departments = db.query(Department).all()
    assert len(departments) == size
    assert {d.id for d in departments} == {d["id"] for d in valid_departments}

@pytest.mark.asyncio
async def test_create_departments_with_duplicate_primary_key(db: Session):
    """
    Tests creating departments with duplicate primary keys and verifies that it raises an Exception caused by IntegrityError.
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_departments = get_invalid_departments_id(size)

    with pytest.raises(Exception) as excinfo:
        await create_departments(invalid_departments, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_departments = get_invalid_departments_id(size)
    with pytest.raises(Exception) as excinfo:
        await create_departments(invalid_departments, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no departments were added
    departments = db.query(Department).all()
    assert len(departments) == 0

@pytest.mark.asyncio
async def test_create_departments_with_duplicate_primary_key_csv(db: Session):
    """
    Tests creating departments with duplicate primary keys and verifies that it raises an Exception caused by IntegrityError.
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_departments = get_invalid_departments_id(size)
    columns = invalid_departments[0].keys()

    with pytest.raises(Exception) as excinfo:
        await create_departments_csv(list_of_dicts_to_csv_bytes(invalid_departments, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_departments = get_invalid_departments_id(size)
    with pytest.raises(Exception) as excinfo:
        await create_departments_csv(list_of_dicts_to_csv_bytes(invalid_departments, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no departments were added
    departments = db.query(Department).all()
    assert len(departments) == 0


@pytest.mark.asyncio
async def test_create_departments_with_duplicate_name(db: Session):
    """
    Tests creating departments with duplicate names and verifies that it raises an IntegrityError.
    """
    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_departments = get_invalid_departments_name(size)

    with pytest.raises(Exception) as excinfo:
        await create_departments(invalid_departments, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_departments = get_invalid_departments_name(size)
    with pytest.raises(Exception) as excinfo:
        await create_departments(invalid_departments, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no departments were added
    departments = db.query(Department).all()
    assert len(departments) == 0

@pytest.mark.asyncio
async def test_create_departments_with_duplicate_name_csv(db: Session):
    """
    Tests creating departments with duplicate names and verifies that it raises an IntegrityError. Based on csv file
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_departments = get_invalid_departments_name(size)
    columns = invalid_departments[0].keys()

    with pytest.raises(Exception) as excinfo:
        await create_departments_csv(list_of_dicts_to_csv_bytes(invalid_departments, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_departments = get_invalid_departments_name(size)
    with pytest.raises(Exception) as excinfo:
        await create_departments_csv(list_of_dicts_to_csv_bytes(invalid_departments, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no departments were added
    departments = db.query(Department).all()
    assert len(departments) == 0

@pytest.mark.asyncio
async def test_create_departments_with_wrong_format(db: Session):
    """
    Tests creating departments with json structure instead of csv.
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_departments = get_invalid_departments_name(size)

    with pytest.raises(Exception) as excinfo:
        await create_departments_csv(list_of_dicts_to_json_bytes(invalid_departments), db)
    # Assert: Verify the exception message
    assert DATA_TYPE_ERROR_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_departments = get_invalid_departments_name(size)
    with pytest.raises(Exception) as excinfo:
        await create_departments_csv(list_of_dicts_to_json_bytes(invalid_departments), db)
    # Assert: Verify the exception message
    assert DATA_TYPE_ERROR_MSG == str(excinfo.value)

    # Verify no departments were added
    departments = db.query(Department).all()
    assert len(departments) == 0
