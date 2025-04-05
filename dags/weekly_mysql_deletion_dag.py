import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from airflow import DAG
from datetime import datetime, timedelta
from tasks.weekly_delete_cdc import export_with_deletion_task, load_snowflake_deletion_task

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weekly_mysql_to_snowflake_deletion_sync",
    description="Weekly job to sync deletes between MySQL and Snowflake",
    default_args=default_args,
    schedule_interval="0 2 * * SUN",  # Every Sunday at 2:00 AM
    start_date=datetime(2025, 4, 5),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "deletion", "snowflake"],
) as dag:

    export_task = export_with_deletion_task()
    load_task = load_snowflake_deletion_task()

    export_task >> load_task
