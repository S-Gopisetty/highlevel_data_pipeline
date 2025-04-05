import pymysql
import os
import time
import logging
from datetime import datetime
import csv
import sys

# --- Path setup ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
sys.path.append(PROJECT_ROOT)

from src.utils.db_config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
)

# --- Logging setup ---
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'load_csv_to_mysql.log')),
        logging.StreamHandler()
    ]
)

def load_csv_to_mysql():
    start_time = time.time()
    start_ts = datetime.utcnow()
    logging.info(f"Load started at: {start_ts}")

    csv_path = os.path.abspath("data/sqlite_exports/reviews.csv")
    temp_path = os.path.abspath("data/sqlite_exports/temp_reviews.csv")

    if not os.path.exists(csv_path):
        logging.error(f"CSV file not found: {csv_path}")
        return

    logging.info(f"Using CSV file: {csv_path}")

    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            local_infile=True,
            autocommit=True
        )

        with conn.cursor() as cursor:
            # 1. Get max existing Id
            cursor.execute("SELECT MAX(Id) FROM reviews")
            max_id = cursor.fetchone()[0] or 0
            logging.info(f"Max existing Id in MySQL: {max_id}")

            # 2. Read and filter new rows
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                new_rows = [row for row in reader if int(row["Id"]) > max_id]

            num_new_rows = len(new_rows)
            logging.info(f"Rows to load: {num_new_rows}")

            if num_new_rows == 0:
                logging.info("No new rows to load. Exiting.")
                return

            # 3. Write new rows to temp CSV
            with open(temp_path, "w", newline='', encoding='utf-8') as tmpfile:
                writer = csv.DictWriter(tmpfile, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(new_rows)

            # 4. Load from temp CSV using LOAD DATA
            load_sql = f"""
            LOAD DATA LOCAL INFILE '{temp_path}'
            INTO TABLE reviews
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS;
            """
            cursor.execute(load_sql)

            # 5. Log metrics
            cursor.execute("""
                INSERT INTO review_ingest_metrics (batch_time, inserted_count, updated_count, deleted_count)
                VALUES (%s, %s, %s, %s)
            """, (datetime.utcnow(), num_new_rows, 0, 0))

        end_time = time.time()
        end_ts = datetime.utcnow()
        duration = round(end_time - start_time, 2)

        logging.info(f"Loaded {num_new_rows} new rows into MySQL from {csv_path}")
        logging.info(f"Load completed at: {end_ts}")
        logging.info(f"Duration: {duration} seconds")

        # Cleanup temp file
        os.remove(temp_path)

    except Exception as e:
        logging.error(f"Load failed: {e}")
        raise

if __name__ == "__main__":
    load_csv_to_mysql()
