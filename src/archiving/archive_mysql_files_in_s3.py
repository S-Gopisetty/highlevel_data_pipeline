import boto3
import os
import zipfile
import logging
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime

# === Load environment variables from project root .env ===
load_dotenv()

# Fetching .env values
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_REVIEW_PREFIX", "swiggy/reviews_parquet/")
S3_ARCHIVE_PREFIX = os.getenv("S3_ARCHIVE_PREFIX", "swiggy/reviews_parquet/archive")

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
        logging.FileHandler(os.path.join(log_dir, 'archive_to_s3.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# === Main Archive Function ===
def run():
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # List files in the S3 bucket folder (prefix)
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_PREFIX)

    # Get the list of files to be archived
    if 'Contents' not in response:
        logging.warning("No files found in the specified S3 prefix.")
        return

    s3_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]

    if not s3_files:
        logging.warning("No parquet files found to archive.")
        return

    logging.info(f"Found {len(s3_files)} files to archive to S3...")

    # Generate the archive filename with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_filename = f"reviews_{timestamp}.zip"
    archive_s3_path = f"{S3_ARCHIVE_PREFIX}/{archive_filename}"

    # Create a BytesIO buffer to store the zip file in memory
    with BytesIO() as zip_buffer:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # For each file in the list, download it from S3 and add to the zip
            for s3_file in s3_files:
                file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_file)
                file_content = file_obj['Body'].read()
                zipf.writestr(os.path.basename(s3_file), file_content)  # Write the file to the zip

        # Upload the zip file to S3
        zip_buffer.seek(0)  # Go to the start of the buffer before uploading
        try:
            s3.upload_fileobj(zip_buffer, S3_BUCKET_NAME, archive_s3_path)
            logging.info(f"Uploaded {archive_filename} → s3://{S3_BUCKET_NAME}/{archive_s3_path}")
        except Exception as e:
            logging.error(f"Failed to upload archive {archive_filename}: {str(e)}")

    # Delete the files after they have been archived
    for s3_file in s3_files:
        try:
            s3.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_file)
            logging.info(f"Deleted {s3_file} from s3://{S3_BUCKET_NAME}")
        except Exception as e:
            logging.error(f"Failed to delete {s3_file}: {str(e)}")

    logging.info("Archive and delete process complete.")

# === Entry Point ===
if __name__ == '__main__':
    run()
