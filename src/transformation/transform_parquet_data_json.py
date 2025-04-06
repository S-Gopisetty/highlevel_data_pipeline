import pandas as pd
import os
import logging
import time
from datetime import datetime
import pandera as pa
from pandera import Column, DataFrameSchema
import zipfile

today = datetime.today().strftime("%Y%m%dT%H%M%SZ")
# === Logging Setup ===
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "transform_json.log"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# === Schema Definition ===
schema = DataFrameSchema({
    "id": Column(str, nullable=False),
    "name": Column(str, nullable=False),
    "rating": Column(float, nullable=True),
    "rating_count": Column(object, nullable=True),
    "cost": Column(object, nullable=True),
    "address": Column(str, nullable=True),
    "cuisine": Column(object, nullable=True),
    "lic_no": Column(object, nullable=True),
    "link": Column(str, nullable=True),
    "city": Column(str, nullable=True),
    "city_link": Column(str, nullable=True),
    "created_at": Column(str, nullable=True),
})

# === Transformation Function ===
def run():
    start_time = time.time()

    input_dir = 'data/restaurant_parquet_chunks'
    output_dir = 'data/restaurant_parquet_transformed'
    archive_dir = 'data/restaurant_parquet_archived'
    summary_csv = f'logs/transform_summary_{today}.csv'
    archive_name = f"restaurants_archive_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.zip"

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    files = sorted([f for f in os.listdir(input_dir) if f.endswith('.parquet')])
    logging.info(f"Found {len(files)} files for transformation.")

    summary = []
    archive_path = os.path.join(archive_dir, archive_name)
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:

        for i, file in enumerate(files):
            file_path = os.path.join(input_dir, file)
            try:
                df = pd.read_parquet(file_path)

                # Schema validation
                validated_df = schema.validate(df)

                # Transformations
                validated_df.drop_duplicates(subset=["id", "name", "address"], inplace=True)
                validated_df["name"] = validated_df["name"].str.strip().str.title()
                validated_df["city"] = validated_df["city"].str.strip().str.title()
                validated_df["address"] = validated_df["address"].fillna("N/A")

                # Save transformed file
                out_path = os.path.join(output_dir, f"transformed_{i}.parquet")
                validated_df.to_parquet(out_path, index=False)
                logging.info(f"Transformed and saved: {out_path}")

                # Archive original
                zipf.write(file_path, arcname=file)
                os.remove(file_path)
                logging.info(f"Archived and deleted: {file_path}")

                # Summary
                summary.append({
                    "file": file,
                    "rows_in": len(df),
                    "rows_out": len(validated_df),
                    "output_file": out_path,
                    "timestamp": datetime.utcnow().isoformat()
                })

            except Exception as e:
                logging.error(f"Failed to process {file} → {e}")
                continue

    # Save summary report
    pd.DataFrame(summary).to_csv(summary_csv, index=False)
    logging.info(f"Summary report saved to: {summary_csv}")

    duration = round(time.time() - start_time, 2)
    logging.info(f"Done: Transformed {len(files)} files in {duration} seconds")
    logging.info(f"Archived to: {archive_path}")

# === Entrypoint ===
if __name__ == '__main__':
    run()
