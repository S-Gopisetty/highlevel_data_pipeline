import boto3
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import zipfile
import time

# === Load environment variables from .env ===
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX", "swiggy/parquet/")

# === Validate .env ===
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME]):
    print("Missing or invalid environment variables. Please check your .env file.")
    exit(1)

# === Logging Setup ===
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'upload_to_s3_json.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# === Upload Script ===
def run():
    start_time = time.time()

    parquet_dir = "data/restaurant_parquet_transformed/"
    archive_dir = "data/restaurant_parquet_archived"
    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    archive_name = f"uploaded_parquet_{timestamp}.zip"
    archive_path = os.path.join(archive_dir, archive_name)

    os.makedirs(archive_dir, exist_ok=True)

    if not os.path.exists(parquet_dir):
        logging.error(f"Parquet directory does not exist: {parquet_dir}")
        return

    parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
    if not parquet_files:
        logging.warning("No Parquet files found to upload.")
        return

    logging.info(f"Found {len(parquet_files)} files to upload to S3...")

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    except Exception as e:
        logging.error(f"Failed to initialize S3 client: {e}")
        return

    uploaded_files = []
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in parquet_files:
            local_path = os.path.join(parquet_dir, file)
            s3_key = os.path.join(S3_PREFIX, file)

            try:
                s3.upload_file(local_path, S3_BUCKET_NAME, s3_key)
                logging.info(f"Uploaded {file} → s3://{S3_BUCKET_NAME}/{s3_key}")
                uploaded_files.append(file)

                # Archive the file
                zipf.write(local_path, arcname=file)

                # Delete the file after upload
                os.remove(local_path)
                logging.info(f"Deleted local file: {file}")

            except Exception as e:
                logging.error(f"Failed to upload {file}: {str(e)}")

    # Final Summary
    duration = round(time.time() - start_time, 2)
    logging.info(f"Upload completed in {duration} seconds.")
    logging.info(f"Total files uploaded: {len(uploaded_files)}")
    logging.info(f"Archived uploaded files to: {archive_path}")

# === Entry Point ===
if __name__ == '__main__':
    run()
