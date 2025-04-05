from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
from src.ingestion.export_mysql_to_parquet import export_mysql_to_parquet_chunks

def export_wrapper():
    log_path = "logs/export_mysql_to_parquet_airflow.log"
    os.makedirs("logs", exist_ok=True)

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    export_mysql_to_parquet_chunks()

def export_mysql_to_parquet_task():
    return PythonOperator(
        task_id="export_mysql_to_parquet",
        python_callable=export_wrapper
    )
