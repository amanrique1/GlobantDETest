from typing import List

from database import get_db
from schemas.schemas import JobCreate
from services.job_service import create_jobs, create_jobs_csv

from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, UploadFile

job_router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
    responses={404: {"description": "Not found"}}
)

@job_router.post("/upload", description="Upload CSV to create jobs")
async def upload_csv(
    file: UploadFile,
    db: Session = Depends(get_db)
):
    """
    Uploads a CSV file containing job data and creates jobs in the database.

    Args:
        file: The uploaded CSV file containing job records.
        db: A SQLAlchemy database session dependency.

    Raises:
        HTTPException: 400 Bad Request if the uploaded file is not a CSV or an error occurs during processing.
    """

    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    content = await file.read()
    try:
        await create_jobs_csv(content, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail= str(e))

@job_router.post("/batch", description="Create jobs from a list")
async def batch_insert(
    data: List[JobCreate],
    db: Session = Depends(get_db)
):
    """
    Creates jobs in the database from a list of dictionaries representing job data.

    Args:
        data: A list of JobCreate objects representing job data.
        db: A SQLAlchemy database session dependency.

    Returns:
        The number of jobs created successfully.

    Raises:
        HTTPException: If an error occurs during job creation.
    """
    try:
        return await create_jobs(data, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail= str(e))