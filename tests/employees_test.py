import pytest

from sqlalchemy.orm import Session

from models.db_models import Employee
from services.employee_service import create_employees, create_employees_csv
from services.department_service import create_departments
from services.job_service import create_jobs
from utils.constants import (
    UNIQUE_CONSTRAINT_VIOLATION_MSG,
    DATA_TYPE_ERROR_MSG,
    FOREIGN_KEY_VIOLATION_MSG,
    BATCH_SIZE)
from tests.generator import (
    get_valid_employees,
    get_valid_jobs,
    get_valid_departments,
    get_invalid_employees_id,
    list_of_dicts_to_csv_bytes,
    list_of_dicts_to_json_bytes)
from utils.log_manager import get_logger

logger = get_logger()

@pytest.mark.asyncio
async def test_create_employees_successfully(db: Session):
    """
    Tests successful creation of employees and verifies their count and existence in the database.
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)

    # Create necessary jobs and departments
    await create_departments(depts, db)
    await create_jobs(jobs, db)

    # Under BATCH_SIZE scenario
    created_count = await create_employees(valid_employees[:limit], db)
    assert created_count == limit

    # Over BATCH_SIZE scenario
    created_count = await create_employees(valid_employees[limit:], db)
    assert created_count == size-limit

    # Verify the employees were added
    employees = db.query(Employee).all()
    assert len(employees) == size
    assert {d.id for d in employees} == {d["id"] for d in valid_employees}


@pytest.mark.asyncio
async def test_create_employees_with_duplicate_primary_key(db: Session):
    """
    Tests creating employees with duplicate primary keys and verifies that it raises an Exception caused by IntegrityError.
    """

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]

    # Create necessary jobs and departments
    await create_departments(depts, db)
    await create_jobs(jobs, db)

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_employees = get_invalid_employees_id(size,dept_ids,job_ids)

    with pytest.raises(Exception) as excinfo:
        await create_employees(invalid_employees, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_employees = get_invalid_employees_id(size,dept_ids,job_ids)

    with pytest.raises(Exception) as excinfo:
        await create_employees(invalid_employees, db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no employees were added
    employees = db.query(Employee).all()
    assert len(employees) == 0


@pytest.mark.asyncio
async def test_create_employees_no_jobs(db: Session):
    """
    Tests creation of employees when jobs don't exist
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)

    # Create departments
    await create_departments(depts, db)

    with pytest.raises(Exception) as excinfo:
        await create_employees(valid_employees[:limit], db)
        # Assert: Verify the exception message
    assert FOREIGN_KEY_VIOLATION_MSG == str(excinfo.value)


@pytest.mark.asyncio
async def test_create_employees_no_dept(db: Session):
    """
    Tests creation of employees when jobs don't exist.
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)

    # Create departments
    await create_jobs(jobs, db)

    with pytest.raises(Exception) as excinfo:
        await create_employees(valid_employees[:limit], db)
        # Assert: Verify the exception message
    assert FOREIGN_KEY_VIOLATION_MSG == str(excinfo.value)


@pytest.mark.asyncio
async def test_create_employees_no_dept_no_job(db: Session):
    """
    Tests creation of employees when jobs don't exist.
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)

    with pytest.raises(Exception) as excinfo:
        await create_employees(valid_employees[:limit], db)
        # Assert: Verify the exception message
    assert FOREIGN_KEY_VIOLATION_MSG == str(excinfo.value)


@pytest.mark.asyncio
async def test_create_employees_successfully_csv(db: Session):
    """
    Tests successful creation of employees and verifies their count and existence in the database. Based on a file content
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)
    columns = valid_employees[0].keys()

    # Create necessary jobs and departments
    await create_departments(depts, db)
    await create_jobs(jobs, db)

    # Under BATCH_SIZE scenario
    created_count = await create_employees_csv(list_of_dicts_to_csv_bytes(valid_employees[:limit],columns), db)
    assert created_count == limit

    # Over BATCH_SIZE scenario
    created_count = await create_employees_csv(list_of_dicts_to_csv_bytes(valid_employees[limit:],columns), db)
    assert created_count == size-limit

    # Verify the employees were added
    employees = db.query(Employee).all()
    assert len(employees) == size
    assert {d.id for d in employees} == {d["id"] for d in valid_employees}


@pytest.mark.asyncio
async def test_create_employees_with_duplicate_primary_key_csv(db: Session):
    """
    Tests creating employees with duplicate primary keys and verifies that it raises an Exception caused by IntegrityError. Based on csv content
    """

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]

    # Create necessary jobs and departments
    await create_departments(depts, db)
    await create_jobs(jobs, db)

    # Under BATCH_SIZE scenario
    size = int(BATCH_SIZE*0.5)
    invalid_employees = get_invalid_employees_id(size,dept_ids,job_ids)
    columns = invalid_employees[0].keys()

    with pytest.raises(Exception) as excinfo:
        await create_employees_csv(list_of_dicts_to_csv_bytes(invalid_employees, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    size = int(BATCH_SIZE*1.5)
    invalid_employees = get_invalid_employees_id(size,dept_ids,job_ids)

    with pytest.raises(Exception) as excinfo:
        await create_employees_csv(list_of_dicts_to_csv_bytes(invalid_employees, columns), db)
    # Assert: Verify the exception message
    assert UNIQUE_CONSTRAINT_VIOLATION_MSG == str(excinfo.value)

    # Verify no employees were added
    employees = db.query(Employee).all()
    assert len(employees) == 0


@pytest.mark.asyncio
async def test_create_employees_no_dept_csv(db: Session):
    """
    Tests creation of employees when jobs don't exist. Based on a file content
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)
    columns = valid_employees[0].keys()

    # Create departments
    await create_jobs(jobs, db)

    with pytest.raises(Exception) as excinfo:
        await create_employees_csv(list_of_dicts_to_csv_bytes(valid_employees[:limit],columns), db)
        # Assert: Verify the exception message
    assert FOREIGN_KEY_VIOLATION_MSG == str(excinfo.value)


@pytest.mark.asyncio
async def test_create_employees_no_jobs_csv(db: Session):
    """
    Tests creation of employees when jobs don't exist. Based on a file content
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)
    columns = valid_employees[0].keys()

    # Create departments
    await create_departments(depts, db)

    with pytest.raises(Exception) as excinfo:
        await create_employees_csv(list_of_dicts_to_csv_bytes(valid_employees[:limit],columns), db)
        # Assert: Verify the exception message
    assert FOREIGN_KEY_VIOLATION_MSG == str(excinfo.value)


@pytest.mark.asyncio
async def test_create_employees_no_dept_no_job_csv(db: Session):
    """
    Tests creation of employees when jobs don't exist. Based on a file content
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)
    columns = valid_employees[0].keys()

    with pytest.raises(Exception) as excinfo:
        await create_employees_csv(list_of_dicts_to_csv_bytes(valid_employees[:limit],columns), db)
        # Assert: Verify the exception message
    assert FOREIGN_KEY_VIOLATION_MSG == str(excinfo.value)


@pytest.mark.asyncio
async def test_create_employees_wrong_format(db: Session):
    """
    Tests successful creation of employees and verifies their count and existence in the database. Based on a file content
    """
    size = int(BATCH_SIZE*1.6)
    limit = int(BATCH_SIZE * 0.5)

    # Get employees to create
    jobs = get_valid_jobs(30)
    job_ids = [j["id"] for j in jobs]
    depts = get_valid_departments(10)
    dept_ids = [d["id"] for d in depts]
    valid_employees = get_valid_employees(size,dept_ids,job_ids)
    columns = valid_employees[0].keys()

    # Create necessary jobs and departments
    await create_departments(depts, db)
    await create_jobs(jobs, db)

    # Under BATCH_SIZE scenario
    with pytest.raises(Exception) as excinfo:
        await create_employees_csv(list_of_dicts_to_json_bytes(valid_employees[:limit]), db)
    # Assert: Verify the exception message
    assert DATA_TYPE_ERROR_MSG == str(excinfo.value)

    # Over BATCH_SIZE scenario
    with pytest.raises(Exception) as excinfo:
        await create_employees_csv(list_of_dicts_to_json_bytes(valid_employees[limit:]), db)
    # Assert: Verify the exception message
    assert DATA_TYPE_ERROR_MSG == str(excinfo.value)

    # Verify the employees were added
    employees = db.query(Employee).all()
    assert len(employees) == 0