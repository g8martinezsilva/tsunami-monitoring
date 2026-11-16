# Workshop 2 — Technical architecture and Bronze/Silver pipeline

Workshop 2 builds the first part of the medallion architecture for the tsunami monitoring system, running locally with Docker Compose and Apache Airflow. The guide's sentiment-analysis items (web scraping, NLP text cleaning) do not apply to this project; the equivalent work here is the cleaning of earthquake, tsunami and bulletin records defined in Workshop 1.

## Layers

| Layer | Folder | Content | File name |
|---|---|---|---|
| Bronze | `datalake_bronze/` | Raw responses, unchanged (JSON, GeoJSON, CSV, Atom/CAP XML) | `source_YYYYMMDD_HHMMSS.ext` |
| Silver | `datalake_silver/` | Cleaned, typed, deduplicated records in Parquet, traceable to their Bronze files | `topic_YYYYMMDD_HHMMSS.parquet` |
| Gold | `datalake_gold/` | Aggregates and indicator inputs (Workshop 3) | — |

## Planned components

- `compose.yaml`: PostgreSQL/PostGIS (already defined) plus the Airflow services (metadata database, webserver, scheduler, triggerer).
- `airflow/dags/bronze_ingestion_dag.py`: TaskFlow DAG that downloads the four sources and saves Bronze files.
- `airflow/dags/silver_processing_dag.py`: DAG started by a `FileSensor` on `datalake_bronze/` that cleans the new files and writes Silver Parquet.
- `src/`: shared source clients and preprocessing functions used by both DAGs.
- `notebooks/02_silver_exploration.ipynb`: reads and explores a Silver Parquet file.
- `workshop_2/report/Team8_Workshop2_Tsunami.tex`: Technical Architecture and Pipeline Report, same template as Workshop 1 (PDF built next to it).

Course scope reminder (Team Project Assignment): Workshop 2 covers the architecture, Docker stack, database schema and historical ingestion with validation and quarantine; incremental ingestion, watermarks and scheduling belong to Workshop 3.
