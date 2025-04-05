from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
from src.archiving.archive_mysql_files_in_s3 import run

def archive_wrapper():
    log_path = "logs/archive_mysql_files_in_s3_airflow.log"
    os.makedirs("logs", exist_ok=True)

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    run()

def archive_mysql_files_in_s3_task():
    return PythonOperator(
        task_id="archive_mysql_files_in_s3",
        python_callable=archive_wrapper
    )
