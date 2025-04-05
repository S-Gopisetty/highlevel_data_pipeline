import boto3
import os
from dotenv import load_dotenv
import logging
from pathlib import Path

# === Load environment variables from project root .env ===
load_dotenv()

# Fetching .env values
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_REVIEW_PREFIX", "swiggy/reviews_parquet/")

# === Debug Print to Verify .env ===
print(f"AWS_REGION: {AWS_REGION}")
print(f"S3_BUCKET_NAME: {S3_BUCKET_NAME}")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME]):
    print("Missing or invalid environment variables. Please check your .env file.")
    exit(1)

# === Setup Logging ===
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'upload_to_s3_sqlite.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# === Main Upload Function ===
def run():
    parquet_dir = "data/transformed_review_chunks/"
    
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
        logging.error(f"Failed to initialize boto3 client: {e}")
        return

    for file in parquet_files:
        local_path = os.path.join(parquet_dir, file)
        s3_key = os.path.join(S3_PREFIX, file)

        try:
            s3.upload_file(local_path, S3_BUCKET_NAME, s3_key)
            logging.info(f"Uploaded {file} → s3://{S3_BUCKET_NAME}/{s3_key}")

            # Delete local file after successful upload
            os.remove(local_path)
            logging.info(f"Deleted local file: {local_path}")

        except Exception as e:
            logging.error(f"Failed to upload {file}: {str(e)}")

    logging.info("All files uploaded to S3 and local copies deleted.")

# === CLI ===
if __name__ == '__main__':
    run()
