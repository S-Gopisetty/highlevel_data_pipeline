import pandas as pd
import os
import time
import logging
import hashlib
from datetime import datetime
import pymysql
import sys

# --- Path setup ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
sys.path.append(PROJECT_ROOT)

from src.utils.db_config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
)
from src.utils.suppress_warnings import suppress_sqlalchemy_warnings
suppress_sqlalchemy_warnings()

# --- Logging setup ---
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'export_mysql_to_parquet.log')),
        logging.StreamHandler()
    ]
)

# --- Helpers ---
def hash_row(row, exclude_cols=["created_at"]):
    row_data = tuple(str(row[col]) for col in row.index if col not in exclude_cols)
    return hashlib.md5(str(row_data).encode()).hexdigest()

def load_shadow(path):
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame(columns=["Id", "row_hash"])

def append_to_deletion_log(deletes, path):
    if not deletes:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(deletes)
    df["deleted_at"] = datetime.utcnow()
    df["reason"] = "CDC - not found in snapshot"
    if os.path.exists(path):
        existing = pd.read_parquet(path)
        df = pd.concat([existing, df], ignore_index=True)
    df.to_parquet(path, index=False)
    logging.info(f"Appended {len(deletes)} deletes to {path}")

# --- Main Export Function ---
def export_mysql_to_parquet_chunks(chunk_size=10000, lookback_window=5000, full_delete_check=False):
    start_time = time.time()
    logging.info(f"Export started at: {datetime.utcnow()}")

    export_dir = "data/mysql_exports_cdc"
    insert_dir = os.path.join(export_dir, "inserts")
    update_dir = os.path.join(export_dir, "updates")
    base_shadow = "data/shadow/reviews_shadow.parquet"
    weekly_shadow = "data/shadow/weekly_reviews_shadow.parquet"
    delete_audit_path = "data/deletion_history/deleted_reviews.parquet"

    os.makedirs(insert_dir, exist_ok=True)
    os.makedirs(update_dir, exist_ok=True)
    os.makedirs(os.path.dirname(base_shadow), exist_ok=True)

    try:
        # Connect
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, database=MYSQL_DB,
            cursorclass=pymysql.cursors.DictCursor
        )

        # Tracker value (for non-weekly runs)
        with conn.cursor() as cursor:
            cursor.execute("SELECT last_max_review_id FROM review_ingest_tracker WHERE id = 1")
            result = cursor.fetchone()
            last_max_id = result["last_max_review_id"] if result else 0
            logging.info(f"Last max ID: {last_max_id}")

        # Query
        if full_delete_check:
            query = "SELECT * FROM reviews ORDER BY Id"
            logging.info("Full delete detection enabled.")
        else:
            cutoff_id = max(0, last_max_id - lookback_window)
            query = f"SELECT * FROM reviews WHERE Id >= {cutoff_id} ORDER BY Id"
            logging.info(f"Using cutoff ID: {cutoff_id}")

        # Load data
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)

        if df.empty or "Id" not in df.columns:
            logging.warning("No data or missing Id column.")
            return

        df["Id"] = pd.to_numeric(df["Id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["Id"])
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["row_hash"] = df.apply(hash_row, axis=1)
        df.set_index("Id", inplace=True)

        # Load shadow
        shadow_path = weekly_shadow if full_delete_check else base_shadow
        df_shadow = load_shadow(shadow_path)
        df_shadow.set_index("Id", inplace=True)
        shadow_hashes = df_shadow["row_hash"].to_dict()

        # CDC Detection
        inserts, updates, deletes = [], [], []

        for idx, row in df.iterrows():
            if idx not in shadow_hashes:
                inserts.append(row)
            elif shadow_hashes[idx] != row["row_hash"]:
                updates.append(row)

        # Delete detection
        if full_delete_check:
            for sid in shadow_hashes:
                if sid not in df.index:
                    deletes.append({"Id": sid})
        else:
            lookback_shadow = df_shadow[df_shadow.index >= cutoff_id]
            for sid in lookback_shadow.index:
                if sid not in df.index:
                    deletes.append({"Id": sid})
            logging.info(f"Detected {len(deletes)} deletes in lookback window.")

        # Remove insert-delete conflicts
        insert_ids = {int(r.name) for r in inserts}
        delete_ids = {int(d["Id"]) for d in deletes}
        conflicting_ids = insert_ids & delete_ids
        inserts = [r for r in inserts if int(r.name) not in conflicting_ids]
        deletes = [d for d in deletes if int(d["Id"]) not in conflicting_ids]

        if conflicting_ids:
            logging.info(f"Skipped {len(conflicting_ids)} insert-delete conflicts.")

        # Save transformed data
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        def save_chunks(df_list, path_dir, name):
            if not df_list: return
            df_final = pd.DataFrame(df_list).reset_index()
            for i in range(0, len(df_final), chunk_size):
                chunk = df_final.iloc[i:i + chunk_size]
                out_path = os.path.join(path_dir, f"{name}_chunk_{i//chunk_size}_{timestamp}.parquet")
                chunk.to_parquet(out_path, index=False)
                logging.info(f"Saved {name} chunk {i//chunk_size + 1}: {out_path}")

        save_chunks(inserts, insert_dir, "inserts")
        save_chunks(updates, update_dir, "updates")

        # Append delete audit
        append_to_deletion_log(deletes, delete_audit_path)

        # Save shadow
        df_shadow_new = df[["row_hash"]].reset_index()
        df_shadow_new.to_parquet(shadow_path, index=False)
        logging.info(f"Shadow file updated: {shadow_path}")

        # Update tracker
        if not full_delete_check:
            new_max_id = int(df.index.max())
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE review_ingest_tracker SET last_max_review_id = %s WHERE id = 1",
                    (new_max_id,)
                )
                conn.commit()
            logging.info(f"Tracker updated to max_id = {new_max_id}")

        duration = round(time.time() - start_time, 2)
        logging.info(f"Done: Inserts={len(inserts)}, Updates={len(updates)}, Deletes={len(deletes)} in {duration}s")
        conn.close()

    except Exception as e:
        logging.error(f"Export failed: {e}")
        raise

# CLI
if __name__ == "__main__":
    export_mysql_to_parquet_chunks(full_delete_check=False)
