import os
import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    
from airflow.operators.python import PythonOperator
from src.loading.load_to_warehouse_json import run as load_json
from src.utils.logger import setup_dual_logger

def run_load():
    logger = setup_dual_logger("load_to_snowflake_json")
    logger.info("Snowflake load task started via Airflow...")
    load_json()
    logger.info("Snowflake load task completed.")

def load_json_task():
    return PythonOperator(
        task_id="load_to_snowflake_json",
        python_callable=run_load
    )
