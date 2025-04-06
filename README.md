# MySQL to Snowflake CDC Data Pipeline

This project implements a robust data engineering pipeline that continuously ingests data from a MySQL database, detects changes (inserts, updates, deletes), transforms the data, uploads it to Amazon S3, and loads it into a Snowflake data warehouse using Apache Airflow.

## Features

- Change Data Capture (CDC) using hash comparison and shadow tables
- Handles inserts, updates, and deletes
- Data transformation with cleaning and normalization
- Chunked export to Parquet format
- Uploads to Amazon S3
- Loads into Snowflake using `COPY INTO` and `MERGE`
- Full logging and archival
- Modular design integrated into Apache Airflow DAGs
- Weekly full delete scan support using a dedicated shadow file

---

# JSON to Snowflake Data Pipeline
This project builds a weekly scheduled pipeline that ingests a large static JSON file (restaurant data), transforms and cleans the data, converts it to Parquet, uploads to S3, and loads it into a Snowflake table. The pipeline is orchestrated using Apache Airflow.

---

## Features

- Streamed ingestion using `ijson`
- Chunked Parquet export for performance
- Pandas + Pandera schema validation
- Error handling and detailed logging (local + Airflow UI)
- Data archiving (JSON & Parquet)
- S3 Upload + Snowflake `COPY INTO` load
- Metrics and summary reporting
- Weekly schedule using Airflow

---

## Project Structure

```
src/
├── ingestion/
│   └── export_mysql_to_parquet.py
│   └── ingest_json_to_parquet.py
├── transformation/
│   └── transform_parquet_data_mysql.py
│   └── transform_parquet_data_json.py
├── s3_upload/
│   └── upload_to_s3_mysql.py
│   └── upload_to_s3_json.py
├── loading/
│   └── load_to_warehouse_mysql.py
│   └── load_to_warehouse_json.py
├── archiving/
│   └── archive_mysql_files_in_s3.py
│   └── archive_json_files_in_s3.py
├── utils/
│   └── db_config.py
│   └── suppress_warnings.py
│   └── logger.py

dags/
└── pipeline_mysql_dag.py
└── weekly_mysql_deletion_dag.py
└── pipeline_json_dag.py

tasks/
├── export_mysql_to_parquet.py
├── transform_parquet_data_mysql.py
├── upload_to_s3_mysql.py
├── load_to_warehouse_mysql.py
├── archive_mysql_files_in_s3.py
└── weekly_delete_cdc.py
├── ingest_json_task.py
├── transform_parquet_task.py
├── upload_s3_task.py
├── load_snowflake_task.py
└── archive_json_task.py
```

---

## Getting Started

### 1. Clone the Repo

```bash
git clone https://github.com/S-Gopisetty/highlevel_data_pipeline.git
cd highlevel_data_pipeline
```

### 2. Set Up Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```env
# AWS
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=your_region
S3_BUCKET_NAME=your_bucket_name
S3_PREFIX="swiggy/parquet/"
S3_REVIEW_PREFIX=swiggy/reviews_parquet/input
S3_ARCHIVE_PREFIX="swiggy/reviews_parquet/archive"
S3_JSON_PREFIX="swiggy/parquet/archive"

# Snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_wh
SNOWFLAKE_DATABASE=your_db
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_STAGE=amazon_stage
SNOWFLAKE_STAGE_SWIGGY=swiggy_stage

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DB=your_db
```

---

## Running the MYSQL Pipeline

### Option 1: Run Manually

```bash
python src/ingestion/export_mysql_to_parquet.py
python src/transformation/transform_parquet_data_mysql.py
python src/s3_upload/upload_to_s3_mysql.py
python src/loading/load_to_warehouse_mysql.py
python src/archiving/archive_mysql_files_in_s3.py
```

### Option 2: Run with Airflow

1. Set `AIRFLOW_HOME` in environment (if needed)
2. Place DAG file inside `dags/`
3. Start scheduler and webserver:

```bash
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@example.com --firstname Admin --lastname User
airflow scheduler
airflow webserver
```

4. Trigger the DAG via Airflow UI (Visit `http://localhost:8080` and trigger the DAG.)

---

## Logs

Logs are written to:

- `logs/` directory in project root
- Airflow task logs (accessible via UI)

---

## Data Flow

```mermaid
flowchart TD
    A[MySQL] --> B[export_mysql_to_parquet.py]
    B --> C[transform_parquet_data_mysql.py]
    C --> D[upload_to_s3_mysql.py]
    D --> E[load_to_warehouse_mysql.py]
    E --> F[archive_mysql_files_in_s3.py]
    F --> G[Snowflake]
```

---

## Weekly Full Delete Detection

- Runs every Sunday via dedicated DAG
- Uses `reviews_shadow_weekly.parquet` for full comparison
- Detects deleted IDs and updates `deleted_reviews.parquet`
- Snowflake script removes rows using this list

---

## Running the JSON Pipeline

### Option 1: Run Manually

```bash
python src/ingestion/ingest_json_to_parquet.py
python src/transformation/transform_parquet_data_json.py
python src/s3_upload/upload_to_s3_json.py
python src/loading/load_to_warehouse_json.py
python src/archiving/archive_json_files_in_s3.py
```

### Option 2: Run with Airflow

1. Initialize Airflow:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create --username admin --password admin --role Admin --firstname Admin --lastname User --email admin@example.com
```

2. Place `pipeline_json_dag.py` into `dags/`

3. Start scheduler and webserver:
```bash
airflow scheduler
airflow webserver
```

4. Visit `http://localhost:8080` and trigger the DAG.

---

## Logs

- Logs are written to:
  - `logs/` folder (local file-based logs)
  - Airflow UI (via task instance logs)

---

## Data Flow

```mermaid
flowchart TD
    A[JSON] --> B[ingest_json_to_parquet.py]
    B --> C[transform_parquet_data_json.py]
    C --> D[upload_to_s3_json.py]
    D --> E[load_to_warehouse_json.py]
    E --> F[archive_json_files_in_s3.py]
    F --> G[Snowflake]
```

---

## Monitoring

- CSV reports with summary stats
- Metrics CSV (`load_metrics_snowflake.csv`) after every Snowflake load
- Invalid records stored in `logs/invalid_rows.log`

---

## Notes

- Designed to run **once per week**
- Assumes full overwrite of target Snowflake table after archival backup
- Schema validation is enforced using Pandera
- Source JSON is archived into a ZIP after ingestion
- Parquet chunks are zipped and archived after Snowflake load

---

## License

MIT

---

Maintained by **Surya**
