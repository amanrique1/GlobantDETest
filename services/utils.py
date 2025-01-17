import csv
from typing import List
from io import StringIO
from typing import Dict

async def process_csv(file_content: bytes, columns: List[str]) -> Dict:
    try:
        # Convert the CSV file content to a string
        csv_str = file_content.decode('utf-8')
        # Check if the CSV string contains the columns, add them if not
        first_line = ','.join(columns)
        if first_line not in csv_str:
            csv_str = first_line + '\n' + csv_str
        # Use StringIO to simulate a file-like object for the CSV string
        csv_file = StringIO(csv_str)
        # Use csv.DictReader to read the CSV data and convert it to a list of dictionaries
        csv_reader = csv.DictReader(csv_file)
        return list(csv_reader)
    except Exception as e:
        raise e