import csv
from io import StringIO
from typing import List, Dict, Any
from utils.constants import UNICODE_DECODE_ERROR_MSG, CSV_ERROR_MSG, GENERIC_ERROR_MSG
from utils.log_manager import SingletonLogger

# Ensure type safety
logger = SingletonLogger().get_logger()

async def process_csv(file_content: bytes, columns: List[str]) -> List[Dict[str, Any]]:
    """
    Processes a CSV file and returns a list of dictionaries.

    Args:
        file_content: The content of the CSV file as bytes.
        columns: A list of column names expected in the CSV file.

    Returns:
        A list of dictionaries, where each dictionary represents a row of data
        from the CSV file, with keys corresponding to the column names.

    Raises:
        UnicodeDecodeError: If the file content cannot be decoded with UTF-8.
        csv.Error: If an error occurs during CSV parsing.
        Exception: For any other unexpected errors.
    """
    try:
        # Decode the bytes to a UTF-8 encoded string
        csv_str = file_content.decode('utf-8')

        # Ensure the CSV string has the expected header row
        first_line = ','.join(columns)
        if first_line not in csv_str:
            csv_str = first_line + '\n' + csv_str

        # Create an in-memory file-like object from the CSV string
        csv_file = StringIO(csv_str)

        # Read the CSV data using csv.DictReader
        csv_reader = csv.DictReader(csv_file)
        return list(csv_reader)

    except UnicodeDecodeError as e:
        logger.error(f"Could not decode file content: {e}")
        raise Exception(UNICODE_DECODE_ERROR_MSG)
    except csv.Error as e:
        logger.error(f"CSV parsing error: {e}")
        raise Exception(CSV_ERROR_MSG)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise Exception(GENERIC_ERROR_MSG)