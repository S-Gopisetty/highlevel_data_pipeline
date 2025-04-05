import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from datetime import datetime, timedelta

from tasks.export_mysql_to_parquet import export_mysql_to_parquet_task
from tasks.transform_parquet_data_mysql import transform_parquet_data_mysql_task
from tasks.upload_to_s3_mysql import upload_to_s3_mysql_task
from tasks.load_to_warehouse_mysql import load_to_warehouse_mysql_task
from tasks.archive_mysql_files_in_s3 import archive_mysql_files_in_s3_task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="mysql_to_snowflake_pipeline",
    default_args=default_args,
    description="Pipeline: Export MySQL → Transform → Upload to S3 → Load to Snowflake → Archive",
    start_date=datetime(2025, 4, 5),
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    catchup=False,
    max_active_runs=1,
    tags=["mysql", "snowflake", "parquet"],
) as dag:

    export_task = export_mysql_to_parquet_task()
    transform_task = transform_parquet_data_mysql_task()
    upload_task = upload_to_s3_mysql_task()
    load_task = load_to_warehouse_mysql_task()
    archive_task = archive_mysql_files_in_s3_task()

    export_task >> transform_task >> upload_task >> load_task >> archive_task
