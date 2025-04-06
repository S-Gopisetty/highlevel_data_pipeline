import os
import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from airflow.operators.python import PythonOperator
from src.ingestion.ingest_json_to_parquet import run as ingest_json
from src.utils.logger import setup_dual_logger

def run_ingest():
    logger = setup_dual_logger("ingest_json_to_parquet")
    logger.info("Ingest task started via Airflow...")
    ingest_json()
    logger.info("Ingest task completed.")

def ingest_json_task():
    return PythonOperator(
        task_id="ingest_json_to_parquet",
        python_callable=run_ingest
    )
