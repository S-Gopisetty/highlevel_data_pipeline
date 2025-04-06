import os
import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
from airflow.operators.python import PythonOperator
from src.archiving.archive_json_files_in_s3 import run as archive_json
from src.utils.logger import setup_dual_logger

def run_archive():
    logger = setup_dual_logger("archive_json_files_in_s3")
    logger.info("Archive task started via Airflow...")
    archive_json()
    logger.info("Archive task completed.")

def archive_json_task():
    return PythonOperator(
        task_id="archive_json_files_in_s3",
        python_callable=run_archive
    )
