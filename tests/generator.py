import io
import csv
import json

from typing import List, Dict, Any

def list_of_dicts_to_csv_bytes(data: List[Dict[str, Any]], fieldnames: List[str]) -> bytes:
    """
    Convert a list of dictionaries to CSV file content as bytes.

    :param data: List of dictionaries where each dictionary represents a row of data.
    :param fieldnames: List of column headers to use for the CSV file.
    :return: The CSV file content encoded as bytes.
    """
    # Create a buffer for binary data
    output = io.BytesIO()
    # Wrap the buffer with a text-based wrapper for CSV writing
    text_wrapper = io.TextIOWrapper(output, encoding='utf-8', newline='')

    # Initialize the CSV writer with the specified fieldnames
    writer = csv.DictWriter(text_wrapper, fieldnames=fieldnames)
    writer.writerows(data)  # Write the rows of data

    # Ensure all text is flushed to the buffer and reset the buffer position
    text_wrapper.flush()
    output.seek(0)

    return output.read()

def list_of_dicts_to_json_bytes(data: List[Dict[str, Any]]):
    """
    Converts a list of dictionaries to json bytes.

    Args:
        data (list): The list of dictionaries to convert.

    Returns:
        bytes: The converted data in json bytes format.
    """

    return json.dumps(data).encode('utf-8')

def get_valid_departments(size: int) -> List[Dict[str, Any]]:
    """
    Generate a list of valid department dictionaries.

    :param size: Number of departments to generate.
    :return: A list of dictionaries with unique department IDs and names.
    """
    return [{"id": i, "department": f"department{i}"} for i in range(size)]

def get_invalid_departments_id(size: int) -> List[Dict[str, Any]]:
    """
    Generate a list of departments with a duplicate ID to simulate invalid data.

    :param size: Number of unique departments to generate before adding a duplicate.
    :return: A list of dictionaries where one dictionary contains a duplicate ID.
    """
    departments = [{"id": i, "department": f"department{i}"} for i in range(size)]
    departments.append({"id": 1, "department": "department"})  # Duplicate ID
    return departments

def get_invalid_departments_name(size: int) -> List[Dict[str, Any]]:
    """
    Generate a list of departments with a duplicate name to simulate invalid data.

    :param size: Number of unique departments to generate before adding a duplicate name.
    :return: A list of dictionaries where one dictionary contains a duplicate name.
    """
    departments = [{"id": i, "department": f"department{i}"} for i in range(size)]
    departments.append({"id": size + 1, "department": "department1"})  # Duplicate name
    return departments

def get_valid_jobs(size: int) -> List[Dict[str, Any]]:
    """
    Generate a list of valid job dictionaries.

    :param size: Number of jobs to generate.
    :return: A list of dictionaries with unique job IDs and names.
    """
    return [{"id": i, "job": f"job{i}"} for i in range(size)]

def get_invalid_jobs_id(size: int) -> List[Dict[str, Any]]:
    """
    Generate a list of jobs with a duplicate ID to simulate invalid data.

    :param size: Number of unique jobs to generate before adding a duplicate.
    :return: A list of dictionaries where one dictionary contains a duplicate ID.
    """
    jobs = [{"id": i, "job": f"job{i}"} for i in range(size)]
    jobs.append({"id": 1, "job": "job"})  # Duplicate ID
    return jobs

def get_invalid_jobs_name(size: int) -> List[Dict[str, Any]]:
    """
    Generate a list of jobs with a duplicate name to simulate invalid data.

    :param size: Number of unique jobs to generate before adding a duplicate name.
    :return: A list of dictionaries where one dictionary contains a duplicate name.
    """
    jobs = [{"id": i, "job": f"job{i}"} for i in range(size)]
    jobs.append({"id": size + 1, "job": "job1"})  # Duplicate name
    return jobs