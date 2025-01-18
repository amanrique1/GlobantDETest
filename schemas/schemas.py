from pydantic import BaseModel
from datetime import datetime

class DepartmentCreate(BaseModel):
    """
    Represents data for creating a new Department.
    """
    id: int
    department: str

class JobCreate(BaseModel):
    """
    Represents data for creating a new Job.
    """
    id: int
    job: str

class EmployeeCreate(BaseModel):
    """
    Represents data for creating a new Employee.
    """
    id: int
    name: str
    datetime: datetime
    department_id: int
    job_id: int