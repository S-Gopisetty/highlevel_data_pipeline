import os
import sys
from datetime import datetime, timedelta
from airflow import DAG

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Import task functions
from tasks.ingest_json_task import ingest_json_task
from tasks.transform_json_task import transform_json_task
from tasks.upload_json_task import upload_json_task
from tasks.load_json_task import load_json_task
from tasks.archive_json_task import archive_json_task

# === DAG Default Arguments ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# === DAG Definition ===
with DAG(
    dag_id="json_to_snowflake_weekly_pipeline",
    description="Weekly pipeline to process Swiggy restaurant JSON → S3 → Snowflake",
    schedule_interval="0 2 * * SUN",  # Every Sunday at 2:00 AM
    start_date=datetime(2025, 4, 5),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["swiggy", "json", "snowflake", "weekly"],
) as dag:

    ingest = ingest_json_task()
    transform = transform_json_task()
    upload = upload_json_task()
    load = load_json_task()
    archive = archive_json_task()

    # Task Flow
    ingest >> transform >> upload >> load >> archive
