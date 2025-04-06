import os
import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
from airflow.operators.python import PythonOperator
from src.transformation.transform_parquet_data_json import run as transform_json
from src.utils.logger import setup_dual_logger

def run_transform():
    logger = setup_dual_logger("transform_parquet_data_json")
    logger.info("Transform task started via Airflow...")
    transform_json()
    logger.info("Transform task completed.")

def transform_json_task():
    return PythonOperator(
        task_id="transform_parquet_data_json",
        python_callable=run_transform
    )
