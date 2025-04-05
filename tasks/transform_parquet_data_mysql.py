from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
from src.transformation.transform_parquet_data_mysql import run

def transform_wrapper():
    log_path = "logs/transform_parquet_data_mysql_airflow.log"
    os.makedirs("logs", exist_ok=True)

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    run()

def transform_parquet_data_mysql_task():
    return PythonOperator(
        task_id="transform_parquet_data_mysql",
        python_callable=transform_wrapper
    )
