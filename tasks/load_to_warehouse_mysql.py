from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
from src.loading.load_to_warehouse_mysql import run

def load_wrapper():
    log_path = "logs/load_to_warehouse_mysql_airflow.log"
    os.makedirs("logs", exist_ok=True)

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    run()

def load_to_warehouse_mysql_task():
    return PythonOperator(
        task_id="load_to_warehouse_mysql",
        python_callable=load_wrapper
    )
