from typing import List

from database import get_db
from schemas.schemas import DepartmentCreate
from services.department_service import create_departments, create_departments_csv

from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, UploadFile

dept_router = APIRouter(
    prefix="/departments",
    tags=["Departments"],
    responses={404: {"description": "Not found"}}
)

@dept_router.post("/upload", description="Upload CSV to create departments")
async def upload_csv(
    file: UploadFile,
    db: Session = Depends(get_db)
):
    """
    Uploads a CSV file containing department data and creates departments in the database.

    Args:
        file: The uploaded CSV file containing department records.
        db: A SQLAlchemy database session dependency.

    Raises:
        HTTPException: 400 Bad Request if the uploaded file is not a CSV or an error occurs during processing.
    """

    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    content = await file.read()
    try:
        await create_departments_csv(content, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail= str(e))

@dept_router.post("/batch", description="Create departments from a list")
async def batch_insert(
    data: List[DepartmentCreate],
    db: Session = Depends(get_db)
):
    """
    Creates departments in the database from a list of dictionaries representing department data.

    Args:
        data: A list of DepartmentCreate objects representing department data.
        db: A SQLAlchemy database session dependency.

    Returns:
        The number of departments created successfully.

    Raises:
        HTTPException: If an error occurs during department creation.
    """
    try:
        return await create_departments(data, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail= str(e))