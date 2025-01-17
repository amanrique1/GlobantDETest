import os
from services.utils import process_csv
from sqlalchemy.orm import Session
from models.db_models import Department
from typing import Dict, Any

BATCH_SIZE = os.getenv('BATCH_SIZE', 1000)

async def create_departments(file_content: bytes, db: Session) -> Dict[str, Any]:
    try:
        records = await process_csv(file_content, Department.__table__.columns.keys())
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            departments = [Department(**item) for item in batch]
            db.bulk_save_objects(departments)

            # Flush the session after each batch to ensure it is written to the DB, even if not committed
            db.flush()

        # Commit all changes after processing all batches
        db.commit()

        return len(records)

    except Exception as e:
        db.rollback()  # Rollback any changes if there's an error
        raise e