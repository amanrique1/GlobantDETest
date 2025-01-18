from database import Base

from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime

class Department(Base):
    """
    Represents a department within a company.
    """
    __tablename__ = "departments"

    # Unique identifier (primary key) for the department
    id = Column(Integer, primary_key=True, index=True)

    # Name of the department (must be unique and cannot be empty)
    name =  Column(String, unique=True, nullable=False)

    # Relationship with the Employee model
    # Represents a one-to-many relationship: A Department can have many Employees
    employees = relationship("Employee", back_populates="department")

class Job(Base):
    """
    Represents a job title within a company.
    """
    __tablename__ = "jobs"

    # Unique identifier (primary key) for the job
    id = Column(Integer, primary_key=True, index=True)

    # Title of the job (cannot be empty)
    job = Column(String, nullable=False)

    # Relationship with the Employee model
    # Represents a one-to-many relationship: A Job can have many Employees
    employees = relationship("Employee", back_populates="job")

class Employee(Base):
    """
    Represents an employee within a company.
    """
    __tablename__ = "employees"

    # Unique identifier (primary key) for the employee
    id = Column(Integer, primary_key=True, index=True)

    # Name of the employee (cannot be empty)
    name = Column(String, nullable=False)

    # Date and time the employee record was created
    datetime = Column(DateTime)

    # Foreign key referencing the ID of the department the employee belongs to
    department_id = Column(Integer, ForeignKey("departments.id"))

    # Foreign key referencing the ID of the job the employee holds
    job_id = Column(Integer, ForeignKey("jobs.id"))

    # Relationship with the Department model
    # Represents a many-to-one relationship: An Employee belongs to one Department
    department = relationship("Department", back_populates="employees")

    # Relationship with the Job model
    # Represents a many-to-one relationship: An Employee holds one Job
    job = relationship("Job", back_populates="employees")