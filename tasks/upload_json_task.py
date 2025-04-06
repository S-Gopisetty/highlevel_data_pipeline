import os
import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
from airflow.operators.python import PythonOperator
from src.s3_upload.upload_to_s3_json import run as upload_json
from src.utils.logger import setup_dual_logger

def run_upload():
    logger = setup_dual_logger("upload_to_s3_json")
    logger.info("Upload to S3 task started via Airflow...")
    upload_json()
    logger.info("Upload to S3 task completed.")

def upload_json_task():
    return PythonOperator(
        task_id="upload_to_s3_json",
        python_callable=run_upload
    )
