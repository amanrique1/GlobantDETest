from typing import List

from database import get_db
from schemas.schemas import EmployeeCreate
from services.employee_service import create_employees, create_employees_csv

from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, UploadFile

employee_router = APIRouter(
    prefix="/employees",
    tags=["Employees"],
    responses={404: {"description": "Not found"}}
)

@employee_router.post("/upload", description="Upload CSV to create employees")
async def upload_csv(
    file: UploadFile,
    db: Session = Depends(get_db)
):
    """
    Uploads a CSV file containing employee data and creates employees in the database.

    Args:
        file: The uploaded CSV file containing employee records.
        db: A SQLAlchemy database session dependency.

    Raises:
        HTTPException: 400 Bad Request if the uploaded file is not a CSV or an error occurs during processing.
    """

    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    content = await file.read()
    try:
        await create_employees_csv(content, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail= str(e))

@employee_router.post("/batch", description="Create employees from a list")
async def batch_insert(
    data: List[EmployeeCreate],
    db: Session = Depends(get_db)
):
    """
    Creates employees in the database from a list of dictionaries representing employee data.

    Args:
        data: A list of EmployeeCreate objects representing employee data.
        db: A SQLAlchemy database session dependency.

    Returns:
        The number of employees created successfully.

    Raises:
        HTTPException: If an error occurs during employee creation.
    """
    try:
        return await create_employees(data, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail= str(e))