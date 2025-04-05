import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import logging
from airflow.operators.python import PythonOperator
from src.ingestion.export_mysql_to_parquet import export_mysql_to_parquet_chunks
from src.loading.load_to_warehouse_mysql import run as load_to_snowflake_with_deletes

# === Logging Setup ===
os.makedirs("logs", exist_ok=True)
log_file_path = os.path.join("logs", "weekly_deletion_task.log")

def setup_dual_logger(log_path):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = True

    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(log_path) for h in logger.handlers):
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# === Task Functions ===
def run_weekly_export_deletes():
    logger = setup_dual_logger(log_file_path)
    logger.info("📤 Weekly Export (full delete detection) started...")
    export_mysql_to_parquet_chunks(full_delete_check=True)
    logger.info("✅ Weekly Export completed.")

def run_snowflake_merge_and_delete():
    logger = setup_dual_logger(log_file_path)
    logger.info("🧹 Running Snowflake loader (merge + delete)...")
    load_to_snowflake_with_deletes()
    logger.info("✅ Snowflake load & deletion completed.")

# === Airflow Operator Wrappers ===
def export_with_deletion_task():
    return PythonOperator(
        task_id="export_with_full_deletion",
        python_callable=run_weekly_export_deletes
    )

def load_snowflake_deletion_task():
    return PythonOperator(
        task_id="merge_and_delete_in_snowflake",
        python_callable=run_snowflake_merge_and_delete
    )
