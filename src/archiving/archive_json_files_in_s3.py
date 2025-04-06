import os
import boto3
import zipfile
import tempfile
import logging
from datetime import datetime
from dotenv import load_dotenv

# === Load AWS credentials ===
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX", "swiggy/parquet/")  # source folder
ARCHIVE_PREFIX = os.getenv("S3_JSON_PREFIX", "swiggy/archives/")  # destination folder

# === Logging Setup ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "archive_s3_parquet.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def run(delete_after_zip=True, upload_archive_to_s3=True):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    logging.info("Scanning S3 for .parquet files...")

    parquet_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(".parquet"):
                parquet_keys.append(key)

    if not parquet_keys:
        logging.warning("No parquet files found to archive.")
        return

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    zip_filename = f"parquet_archive_{timestamp}.zip"
    local_zip_path = os.path.join("data", "archives", zip_filename)
    os.makedirs(os.path.dirname(local_zip_path), exist_ok=True)

    logging.info(f"Archiving {len(parquet_keys)} files into {zip_filename}...")

    with zipfile.ZipFile(local_zip_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        for key in parquet_keys:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                s3.download_file(Bucket=S3_BUCKET_NAME, Key=key, Filename=tmp.name)
                archive.write(tmp.name, arcname=os.path.basename(key))
                logging.info(f"Added {key} to archive.")
                os.unlink(tmp.name)

    logging.info(f"Archive created: {local_zip_path}")

    if upload_archive_to_s3:
        s3_key_archive = os.path.join(ARCHIVE_PREFIX, zip_filename)
        s3.upload_file(local_zip_path, S3_BUCKET_NAME, s3_key_archive)
        logging.info(f"Uploaded archive to S3: s3://{S3_BUCKET_NAME}/{s3_key_archive}")

    if delete_after_zip:
        logging.info("Deleting original parquet files from S3...")
        for key in parquet_keys:
            s3.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
        logging.info("Original parquet files deleted.")

# === Entry point ===
if __name__ == "__main__":
    run()
