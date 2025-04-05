import pandas as pd
from datetime import datetime
import os
import logging
import shutil
from zipfile import ZipFile
from filelock import FileLock

# --- Paths ---
base_dir = "data/mysql_exports_cdc"
output_dir = "data/transformed_review_chunks"
archive_dir = "data/transformed_review_chunks_archived"
lock_file = "transform_reviews.lock"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(archive_dir, exist_ok=True)
os.makedirs("logs", exist_ok=True)

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/transform_reviews.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# --- Helpers ---
def normalize_time(unix_timestamp):
    try:
        return datetime.utcfromtimestamp(int(unix_timestamp)).isoformat()
    except:
        return None

def clean_text_field(text):
    if pd.isnull(text) or str(text).strip() == "":
        return "N/A"
    return str(text).strip().capitalize()

def transform_parquet_files(subdir: str):
    input_dir = os.path.join(base_dir, subdir)
    files = sorted([f for f in os.listdir(input_dir) if f.endswith(".parquet")])
    logging.info(f"Transforming {len(files)} files from {input_dir}")

    if not files:
        return

    chunk_count = 0
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    zip_filename = f"{subdir}_archive_{timestamp}.zip"
    zip_path = os.path.join(archive_dir, zip_filename)

    with ZipFile(zip_path, 'w') as zipf:
        for file in files:
            path = os.path.join(input_dir, file)
            df = pd.read_parquet(path)

            # Log columns
            logging.info(f"Columns in {file}: {df.columns.tolist()}")

            # Rename index to Id
            if "index" in df.columns and "Id" not in df.columns:
                df.rename(columns={"index": "Id"}, inplace=True)
                logging.info(f"Renamed 'index' column to 'Id' in {file}")

            if "Id" not in df.columns:
                logging.warning(f"Skipping {file} → missing 'Id' column.")
                continue

            # --- Transformations ---
            df = df.drop_duplicates(subset=["UserId", "ProductId", "ProfileName"], keep="first")
            df["Summary"] = df["Summary"].apply(clean_text_field)
            df["Text"] = df["Text"].apply(clean_text_field)
            df["ProfileName"] = df["ProfileName"].apply(clean_text_field)

            if "Time" in df.columns:
                df["Time"] = df["Time"].apply(normalize_time)

            if "created_at" in df.columns:
                df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

            ordered_cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
                            'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text', 'created_at']
            df = df[[col for col in ordered_cols if col in df.columns]]

            out_file = f"{subdir}_transformed_{chunk_count}_{timestamp}.parquet"
            out_path = os.path.join(output_dir, out_file)
            df.to_parquet(out_path, index=False, engine="pyarrow")
            logging.info(f"Transformed: {file} → {out_file}")
            chunk_count += 1

            # Add source file to ZIP and delete original
            zipf.write(path, arcname=file)
            os.remove(path)
            logging.info(f"Archived + deleted: {file}")

    logging.info(f"All originals zipped → {zip_path}")

def run():
    for folder in ["inserts", "updates"]:
        folder_path = os.path.join(base_dir, folder)
        if os.path.exists(folder_path):
            transform_parquet_files(folder)
        else:
            logging.warning(f"Folder not found: {folder_path}")

    logging.info("All transformation steps completed successfully.")

# --- CLI ---
if __name__ == "__main__":
    with FileLock(lock_file):
        run()
