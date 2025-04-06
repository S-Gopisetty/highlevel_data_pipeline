import os
import time
from dotenv import load_dotenv
import snowflake.connector
import logging
import pandas as pd
from datetime import datetime

# === Load Environment Variables ===
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
SNOWFLAKE_STAGE_SWIGGY = os.getenv("SNOWFLAKE_STAGE_SWIGGY", "swiggy_stage")

# === Logging Setup ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "load_to_snowflake_json.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# === Load Metrics CSV ===
metrics_file = os.path.join(log_dir, "load_metrics_snowflake.csv")
if not os.path.exists(metrics_file):
    pd.DataFrame(columns=[
        "timestamp", "table", "rows_loaded", "rows_processed", "duration_sec", "status"
    ]).to_csv(metrics_file, index=False)

# === COPY INTO SQL ===
COPY_SQL = f"""
COPY INTO restaurants
FROM @{SNOWFLAKE_STAGE_SWIGGY}
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\\.parquet';
"""

# === Backup SQL ===
BACKUP_SQL = """
CREATE OR REPLACE TABLE restaurants_backup AS
SELECT * FROM restaurants;
"""

# === Main Loader ===
def run():
    start_time = time.time()
    now = datetime.utcnow().isoformat()
    rows_loaded = rows_processed = 0
    status = "Success"

    try:
        logging.info("🔗 Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        cursor = conn.cursor()

        # Step 1: Backup restaurants table
        logging.info("Backing up current `restaurants` table to `restaurants_backup`...")
        cursor.execute(BACKUP_SQL)
        logging.info("Backup completed successfully.")

        # Step 2: Truncate current restaurants table
        logging.info("Truncating `restaurants` table...")
        cursor.execute("TRUNCATE TABLE restaurants")

        # Step 3: Load from stage
        logging.info("Loading Parquet files from S3 stage into `restaurants`...")
        cursor.execute(COPY_SQL)
        results = cursor.fetchall()

        rows_loaded = sum(row[2] for row in results if isinstance(row, tuple) and isinstance(row[2], int))
        rows_processed = sum(row[4] for row in results if isinstance(row, tuple) and isinstance(row[4], int))

        logging.info(f"Total rows loaded: {rows_loaded}")
        logging.info(f"Total rows processed: {rows_processed}")

        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Snowflake load failed: {e}")
        status = "Failed"

    # Step 4: Log metrics
    duration = round(time.time() - start_time, 2)
    df_log = pd.DataFrame([{
        "timestamp": now,
        "table": "restaurants",
        "rows_loaded": rows_loaded,
        "rows_processed": rows_processed,
        "duration_sec": duration,
        "status": status
    }])
    df_log.to_csv(metrics_file, mode='a', header=False, index=False)

    logging.info(f"Load metrics logged to {metrics_file}")
    logging.info(f"Load job completed in {duration} seconds.")

# === Entry Point ===
if __name__ == "__main__":
    run()
