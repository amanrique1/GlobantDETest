import os

# Define environment variables
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))

# Define exception messages
GENERIC_ERROR_MSG = "An error occurred while processing the request, please try again later"
UNIQUE_CONSTRAINT_VIOLATION_MSG = "Duplicate record found"
UNICODE_DECODE_ERROR_MSG = "An error occured reading the file, check file invalid characters"
CSV_ERROR_MSG = "There was an error processing the CSV file. Please check the file for any formatting issues, such as incorrect commas or quotes."