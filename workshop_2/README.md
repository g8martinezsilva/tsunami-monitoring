# Workshop 2 — Technical architecture and Bronze/Silver pipeline

Workshop 2 builds the first part of the medallion architecture for the tsunami monitoring system, running locally with Docker Compose and Apache Airflow. The Silver layer applies the cleaning of earthquake, tsunami and bulletin records defined in Workshop 1.

## Layers

| Layer | Folder | Content | File name |
|---|---|---|---|
| Bronze | `datalake_bronze/` | Raw responses, unchanged (JSON, GeoJSON, CSV, Atom/CAP XML) | `source_YYYYMMDD_HHMMSS.ext` |
| Silver | `datalake_silver/` | Cleaned, typed, deduplicated records in Parquet, traceable to their Bronze files | `topic_YYYYMMDD_HHMMSS.parquet` |
| Gold | `datalake_gold/` | Aggregates and indicator inputs (Workshop 3) | — |

## Components

- `compose.yaml`: PostgreSQL/PostGIS project database plus the Airflow services (metadata database, init, webserver, scheduler, triggerer).
- `airflow/dags/bronze_ingestion_dag.py`: TaskFlow DAG that downloads the four sources, validates the files and writes a run manifest and an arrival marker.
- `airflow/dags/silver_processing_dag.py`: DAG started by a `FileSensor` on the Bronze arrival markers; it cleans the new run into Silver Parquet and loads it into PostGIS.
- `src/`: shared code (configuration, downloads, Silver preprocessing, database load) used by `ingest.py` and both DAGs.
- `sql/`: database schema and reference data.
- `notebooks/02_silver_exploration.ipynb`: reads and explores the Silver Parquet samples.
- `report/Team8_Workshop2_Tsunami.pdf`: Technical Architecture and Pipeline Report (same template as Workshop 1).

How to run the pipeline: see "Pipeline (Workshop 2)" in the main [README](../README.md).

Course scope reminder (Team Project Assignment): Workshop 2 covers the architecture, Docker stack, database schema and historical ingestion with validation and quarantine; incremental ingestion, watermarks and scheduling belong to Workshop 3.
