from models import db_models
from database import engine, get_db
from routers.department_router import dept_router

from utils.log_manager import get_logger

from fastapi import FastAPI

# Create all database tables (if they don't exist)
db_models.Base.metadata.create_all(bind=engine)

# Get the logger instance
logger = get_logger()

# Initialize the FastAPI application
app = FastAPI(
    title="Employees management API",
    description="API for querying employees, departments, and jobs",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """
    Event handler that logs a message when the API starts.
    """
    logger.info("Starting API")

@app.on_event("shutdown")
async def shutdown_event():
    """
    Event handler that logs a message when the API shuts down.
    """
    logger.info("Shutting down API")

@app.get("/health")
def health_check():
    """
    Simple endpoint that returns a health check status.

    Returns:
        A dictionary containing a "status" key with the value "ok".
    """
    return {"status": "ok"}

# Include the department router for department-related functionalities
app.include_router(router=dept_router)