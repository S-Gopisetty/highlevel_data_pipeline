from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
from src.s3_upload.upload_to_s3_mysql import run

def upload_wrapper():
    log_path = "logs/upload_to_s3_mysql_airflow.log"
    os.makedirs("logs", exist_ok=True)

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    run()

def upload_to_s3_mysql_task():
    return PythonOperator(
        task_id="upload_to_s3_mysql",
        python_callable=upload_wrapper
    )
