import ijson
import pandas as pd
import os
from datetime import datetime
import logging
import time
import zipfile

# === Setup Logging ===
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

today = datetime.today().strftime("%Y%m%dT%H%M%SZ")
log_file = os.path.join(log_dir, f'ingest_json_{today}.log')
invalid_log_file = os.path.join(log_dir, 'invalid_rows.log')

# Main logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.FileHandler(os.path.join(log_dir, 'pipeline.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Invalid logger
invalid_logger = logging.getLogger('invalid_logger')
invalid_handler = logging.FileHandler(invalid_log_file, encoding='utf-8')
invalid_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
invalid_logger.addHandler(invalid_handler)
invalid_logger.setLevel(logging.WARNING)

# === Ingestion Function ===
def run():
    start_time = time.time()

    input_path = 'data/swiggy_restaurants.json'
    output_dir = 'data/restaurant_parquet_chunks/'
    archive_dir = 'data/archives'
    report_path = f'logs/restaurant_ingest_summary_{today}.csv'
    chunk_size = 50000
    buffer = []
    chunk_count = 0
    total_rows = 0
    total_invalid = 0
    chunk_stats = []

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    if not os.path.exists(input_path):
        logging.error(f"File not found: {input_path}")
        return

    logging.info("Starting streamed ingestion of Swiggy JSON...")

    try:
        with open(input_path, 'rb') as f:
            for city, city_data in ijson.kvitems(f, ''):
                city_link = city_data.get('link', '')
                restaurants = city_data.get('restaurants', {})

                for rest_id, rest_data in restaurants.items():
                    try:
                        row = {
                            "id": rest_id,
                            "name": rest_data.get("name"),
                            "rating": float(rest_data["rating"]) if rest_data.get("rating") not in [None, "--", "NA"] else None,
                            "rating_count": rest_data.get("rating_count"),
                            "cost": rest_data.get("cost"),
                            "address": rest_data.get("address"),
                            "cuisine": rest_data.get("cuisine"),
                            "lic_no": rest_data.get("lic_no"),
                            "link": rest_data.get("link"),
                            "city": city,
                            "city_link": city_link,
                            "created_at": datetime.utcnow().isoformat()
                        }

                        if not row["id"] or not row["name"]:
                            continue

                        buffer.append(row)
                        total_rows += 1

                        if len(buffer) >= chunk_size:
                            df = pd.DataFrame(buffer)
                            chunk_path = os.path.join(output_dir, f'restaurants_chunk_{chunk_count}.parquet')
                            df.to_parquet(chunk_path, index=False, engine='pyarrow')
                            logging.info(f"Saved chunk {chunk_count} with {len(buffer)} rows → {chunk_path}")
                            chunk_stats.append({
                                "chunk_number": chunk_count,
                                "rows": len(buffer),
                                "file": chunk_path
                            })
                            buffer.clear()
                            chunk_count += 1

                    except Exception as e:
                        total_invalid += 1
                        error_fields = {k: rest_data.get(k, 'MISSING') for k in ["name", "rating", "rating_count", "cost", "cuisine"]}
                        error_msg = f"[CITY: {city}] [ID: {rest_id}] * Error: {e} | Fields: {error_fields}"
                        invalid_logger.warning(error_msg)
                        logging.warning(error_msg)
                        continue

        # Final buffer
        if buffer:
            df = pd.DataFrame(buffer)
            chunk_path = os.path.join(output_dir, f'restaurants_chunk_{chunk_count}.parquet')
            df.to_parquet(chunk_path, index=False, engine='pyarrow')
            logging.info(f"Final chunk {chunk_count} saved with {len(buffer)} rows → {chunk_path}")
            chunk_stats.append({
                "chunk_number": chunk_count,
                "rows": len(buffer),
                "file": chunk_path
            })

        # === Archive JSON ===
        archive_path = os.path.join(archive_dir, f'swiggy_restaurants_{today}.zip')
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(input_path, arcname=os.path.basename(input_path))
        logging.info(f"Archived original JSON → {archive_path}")

        # === Delete source file ===
        try:
            os.remove(input_path)
            logging.info(f"Deleted original JSON file: {input_path}")
        except Exception as e:
            logging.warning(f"Could not delete original file: {e}")

        # === Generate CSV Summary Report ===
        summary_df = pd.DataFrame(chunk_stats)
        summary_df["timestamp"] = datetime.utcnow().isoformat()
        summary_df["total_valid_rows"] = total_rows
        summary_df["total_invalid_rows"] = total_invalid
        summary_df.to_csv(report_path, index=False)
        logging.info(f"Ingestion summary saved to {report_path}")

        # === Duration Log ===
        duration = round(time.time() - start_time, 2)
        logging.info(f"Time taken: {duration} seconds")
        logging.info(f"Total rows processed: {total_rows}")
        logging.info(f"Invalid rows: {total_invalid}")
        logging.info("Swiggy JSON ingestion completed successfully.")

    except Exception as e:
        logging.exception(f"Failed to parse JSON: {e}")


# === Entry Point ===
if __name__ == '__main__':
    run()
