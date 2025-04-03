# Data Engineering Pipeline: Swiggy + Amazon Reviews

## Overview
This project builds a robust data pipeline to integrate static restaurant metadata (Swiggy JSON) and dynamic customer reviews (Amazon SQLite DB). The transformed data is loaded into a cloud warehouse (Snowflake / Redshift / BigQuery), with emphasis on integrity, optimization, and automation.

---

## Architecture

```mermaid
graph TD
A[Swiggy JSON] -->|Weekly Ingestion| B[Data Cleaning & Normalization]
C[Amazon SQLite DB] -->|Continuous Sync| D[Review ETL Processor]
B --> E[Transformed Dataset]
D --> E
E --> F[Cloud Warehouse]
F --> G[Analytics / BI Tools]
