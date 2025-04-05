import os
from dotenv import load_dotenv
import snowflake.connector
import logging
from datetime import datetime
import pandas as pd

# === Load Environment Variables ===
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "amazon_stage")  # Default

# === Logging Setup ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "load_to_snowflake_reviews.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# === SQL Codes ===
CREATE_TEMP_TABLE = """
CREATE OR REPLACE TEMP TABLE reviews_stage (
    Id INT,
    ProductId STRING,
    UserId STRING,
    ProfileName STRING,
    HelpfulnessNumerator INT,
    HelpfulnessDenominator INT,
    Score INT,
    Time TIMESTAMP_NTZ,
    Summary STRING,
    Text STRING,
    created_at TIMESTAMP_NTZ
);
"""

COPY_INTO_STAGE = f"""
COPY INTO reviews_stage
FROM @{SNOWFLAKE_STAGE}
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\\.parquet';
"""

MERGE_SQL = """
MERGE INTO reviews t
USING reviews_stage s
ON t.Id = s.Id

WHEN MATCHED AND (
    t.ProductId IS DISTINCT FROM s.ProductId OR
    t.UserId IS DISTINCT FROM s.UserId OR
    t.ProfileName IS DISTINCT FROM s.ProfileName OR
    t.HelpfulnessNumerator IS DISTINCT FROM s.HelpfulnessNumerator OR
    t.HelpfulnessDenominator IS DISTINCT FROM s.HelpfulnessDenominator OR
    t.Score IS DISTINCT FROM s.Score OR
    t.Time IS DISTINCT FROM s.Time OR
    t.Summary IS DISTINCT FROM s.Summary OR
    t.Text IS DISTINCT FROM s.Text OR
    t.created_at IS DISTINCT FROM s.created_at
)
THEN UPDATE SET
    ProductId = s.ProductId,
    UserId = s.UserId,
    ProfileName = s.ProfileName,
    HelpfulnessNumerator = s.HelpfulnessNumerator,
    HelpfulnessDenominator = s.HelpfulnessDenominator,
    Score = s.Score,
    Time = s.Time,
    Summary = s.Summary,
    Text = s.Text,
    created_at = s.created_at

WHEN NOT MATCHED THEN
INSERT (
    Id, ProductId, UserId, ProfileName,
    HelpfulnessNumerator, HelpfulnessDenominator, Score,
    Time, Summary, Text, created_at
)
VALUES (
    s.Id, s.ProductId, s.UserId, s.ProfileName,
    s.HelpfulnessNumerator, s.HelpfulnessDenominator, s.Score,
    s.Time, s.Summary, s.Text, s.created_at
);
"""

# === Delete Logic ===
def process_deletes(conn, parquet_path="data/deletion_history/deleted_reviews.parquet", batch_size=1000):
    if not os.path.exists(parquet_path):
        logging.info("No deletion file found. Skipping deletes.")
        return

    df = pd.read_parquet(parquet_path)
    if df.empty or "Id" not in df.columns:
        logging.info("Deletion file is empty or missing 'Id' column.")
        return

    ids = df["Id"].dropna().astype(int).unique().tolist()
    if not ids:
        logging.info("No valid IDs to delete.")
        return

    total_deleted = 0
    with conn.cursor() as cursor:
        for i in range(0, len(ids), batch_size):
            chunk = ids[i:i+batch_size]
            placeholders = ",".join(str(i) for i in chunk)
            delete_sql = f"DELETE FROM reviews WHERE Id IN ({placeholders})"
            cursor.execute(delete_sql)
            deleted = cursor.rowcount
            total_deleted += deleted
            logging.info(f"Deleted {deleted} rows in batch {i//batch_size + 1}")

    logging.info(f"Total rows deleted from Snowflake: {total_deleted}")

    # === Archive the file after processing ===
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_dir = "data/deletion_history/archive"
    os.makedirs(archive_dir, exist_ok=True)
    archive_path = os.path.join(archive_dir, f"deleted_reviews_{ts}.parquet")
    os.rename(parquet_path, archive_path)
    logging.info(f"Archived deletion log → {archive_path}")

# === Main Loader ===
def run():
    try:
        start_time = datetime.utcnow()
        logging.info(f"Load started at: {start_time.isoformat()}")

        logging.info("Connecting to Snowflake...")
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

        logging.info("Creating temporary staging table...")
        cursor.execute(CREATE_TEMP_TABLE)

        logging.info(f"Copying data from @{SNOWFLAKE_STAGE} into reviews_stage...")
        cursor.execute(COPY_INTO_STAGE)

        logging.info("Running MERGE INTO from staging table...")
        cursor.execute(MERGE_SQL)
        logging.info("MERGE completed.")

        logging.info("Processing deletes from deletion_history...")
        process_deletes(conn)

        cursor.close()
        conn.close()

        end_time = datetime.utcnow()
        logging.info(f"Load completed at: {end_time.isoformat()}")
        logging.info(f"Duration: {(end_time - start_time).total_seconds()} seconds")

    except Exception as e:
        logging.error(f"Snowflake review load failed: {e}")
        raise

if __name__ == "__main__":
    run()
