import pytest

from sqlalchemy.orm import Session

from models.db_models import Job
from services.job_service import create_jobs, create_jobs_csv
from utils.constants import (
    UNIQUE_CONSTRAINT_VIOLATION_MSG,
    DATA_TYPE_ERROR_MSG,
    BATCH_SIZE)
from tests.generator import (
    get_valid_jobs,
    get_invalid_jobs_id,
    get_invalid_jobs_name,
    list_of_dicts_to_csv_bytes,
    list_of_dicts_to_json_bytes)

@pytest.mark.asyncio
async def test_create_jobs_successfully(db: Session):
    """
    Tests successful creation of jobs and verifies their count and existence in the database.
    """
    size = int(BATCH_SIZE*1.6)
    valid_jobs = get_valid_jobs(size)
    limit = int(BATCH_SIZE * 0.5)

    # Under BATCH_SIZE scenario
    created_count = await create_jobs(valid_jobs[:limit], db)
    assert created_count == limit

    # Over BATCH_SIZE scenario
    created_count = await create_jobs(valid_jobs[limit:], db)
    assert created_count == size-limit

    # Verify the jobs were added
    jobs = db.query(Job).all()
    assert len(jobs) == size
    assert {d.id for d in jobs} == {d["id"] for d in valid_jobs}


@pytest.mark.asyncio
async def test_create_jobs_with_duplicate_primary_key(db: Session):
    """
    Tests creating jobs with duplicate primary keys and verifies that it raises an Exception caused by IntegrityError.
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_jobs = get_invalid_jobs_id(size)

    with pytest.raises(Exception) as excinfo:
        await create_jobs(invalid_jobs, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_jobs = get_invalid_jobs_id(size)
    with pytest.raises(Exception) as excinfo:
        await create_jobs(invalid_jobs, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no jobs were added
    jobs = db.query(Job).all()
    assert len(jobs) == 0


@pytest.mark.asyncio
async def test_create_jobs_with_duplicate_name(db: Session):
    """
    Tests creating jobs with duplicate names and verifies that it raises an IntegrityError.
    """
    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_jobs = get_invalid_jobs_name(size)

    with pytest.raises(Exception) as excinfo:
        await create_jobs(invalid_jobs, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_jobs = get_invalid_jobs_name(size)
    with pytest.raises(Exception) as excinfo:
        await create_jobs(invalid_jobs, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no jobs were added
    jobs = db.query(Job).all()
    assert len(jobs) == 0


@pytest.mark.asyncio
async def test_create_jobs_successfully_csv(db: Session):
    """
    Tests successful creation of jobs and verifies their count and existence in the database. Based on a file content
    """
    size = int(BATCH_SIZE*1.6)
    valid_jobs = get_valid_jobs(size)
    columns = valid_jobs[0].keys()
    limit = int(BATCH_SIZE * 0.5)

    # Under BATCH_SIZE scenario
    created_count = await create_jobs_csv(list_of_dicts_to_csv_bytes(valid_jobs[:limit], columns), db)
    assert created_count == limit

    # Over BATCH_SIZE scenario
    created_count = await create_jobs_csv(list_of_dicts_to_csv_bytes(valid_jobs[limit:], columns), db)
    assert created_count == size-limit

    # Verify the jobs were added
    jobs = db.query(Job).all()
    assert len(jobs) == size
    assert {d.id for d in jobs} == {d["id"] for d in valid_jobs}


@pytest.mark.asyncio
async def test_create_jobs_with_duplicate_name_csv(db: Session):
    """
    Tests creating jobs with duplicate names and verifies that it raises an IntegrityError. Based on csv file
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_jobs = get_invalid_jobs_name(size)
    columns = invalid_jobs[0].keys()

    with pytest.raises(Exception) as excinfo:
        await create_jobs_csv(list_of_dicts_to_csv_bytes(invalid_jobs, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_jobs = get_invalid_jobs_name(size)
    with pytest.raises(Exception) as excinfo:
        await create_jobs_csv(list_of_dicts_to_csv_bytes(invalid_jobs, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no jobs were added
    jobs = db.query(Job).all()
    assert len(jobs) == 0


@pytest.mark.asyncio
async def test_create_jobs_with_duplicate_primary_key_csv(db: Session):
    """
    Tests creating jobs with duplicate primary keys and verifies that it raises an Exception caused by IntegrityError.
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_jobs = get_invalid_jobs_id(size)
    columns = invalid_jobs[0].keys()

    with pytest.raises(Exception) as excinfo:
        await create_jobs_csv(list_of_dicts_to_csv_bytes(invalid_jobs, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_jobs = get_invalid_jobs_id(size)
    with pytest.raises(Exception) as excinfo:
        await create_jobs_csv(list_of_dicts_to_csv_bytes(invalid_jobs, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no jobs were added
    jobs = db.query(Job).all()
    assert len(jobs) == 0


@pytest.mark.asyncio
async def test_create_jobs_with_wrong_format(db: Session):
    """
    Tests creating jobs with json structure instead of csv.
    """

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_jobs = get_invalid_jobs_name(size)

    with pytest.raises(Exception) as excinfo:
        await create_jobs_csv(list_of_dicts_to_json_bytes(invalid_jobs), db)
    # Assert: Verify the exception message
    assert DATA_TYPE_ERROR_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_jobs = get_invalid_jobs_name(size)
    with pytest.raises(Exception) as excinfo:
        await create_jobs_csv(list_of_dicts_to_json_bytes(invalid_jobs), db)
    # Assert: Verify the exception message
    assert DATA_TYPE_ERROR_MSG == str(excinfo.value)

    # Verify no jobs were added
    jobs = db.query(Job).all()
    assert len(jobs) == 0
