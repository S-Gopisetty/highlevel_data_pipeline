import sqlite3
import pandas as pd
from datetime import datetime
import os
import time
import logging

# --- Logging setup ---
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'export_sqlite_to_csv.log')),
        logging.StreamHandler()
    ]
)

def export_sqlite_reviews():
    start_time = time.time()
    start_ts = datetime.utcnow()
    logging.info(f"Export started at: {start_ts}")

    try:
        # Ensure the export folder exists
        os.makedirs("data/sqlite_exports", exist_ok=True)

        # Connect to SQLite
        conn = sqlite3.connect("data/database.sqlite")

        # Read the Reviews table
        df = pd.read_sql_query("SELECT * FROM Reviews", conn)
        df["created_at"] = pd.Timestamp.utcnow()

        # Export to CSV
        csv_path = "data/sqlite_exports/reviews.csv"
        df.to_csv(csv_path, index=False)
        conn.close()

        end_time = time.time()
        end_ts = datetime.utcnow()
        duration = round(end_time - start_time, 2)

        logging.info(f"Exported {len(df)} rows to {csv_path}")
        logging.info(f"Export completed at: {end_ts}")
        logging.info(f"Duration: {duration} seconds")

    except Exception as e:
        logging.error(f"Export failed: {e}")
        raise

if __name__ == "__main__":
    export_sqlite_reviews()
