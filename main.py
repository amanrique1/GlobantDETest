import os
import logging
from fastapi import FastAPI, UploadFile, HTTPException
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(
    title="Employees management API",
    description="API for querying employees, departments, and jobs",
    version="1.0.0"
)
BATCH_SIZE = os.getenv('BATCH_SIZE', 1000)

@app.post("/{table_name}/upload")
async def upload_csv(
    table_name: str,
    file: UploadFile
):
    """Upload a CSV file and insert its records into the database."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    content = await file.read()
    return {}

@app.post("/{table_name}/batch")
async def batch_insert(
    table_name: str,
    data: List[Dict[str, Any]]
):
    """Insert a batch of records into the database."""
    if len(data) > BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Batch size cannot exceed {BATCH_SIZE} records"
        )
    return {}

@app.get("/hello")
async def hello():
    return {"message": "Hello, World!"}

@app.on_event("startup")
async def startup_event():
    """Run startup tasks."""
    logger.info("Starting API")

@app.on_event("shutdown")
async def shutdown_event():
    """Run cleanup tasks."""
    logger.info("Shutting down API")

@app.get("/health")
def health_check():
    return {"status": "ok"}