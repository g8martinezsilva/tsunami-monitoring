# Workshop 3 — Pipeline improvements, incremental ingestion and Gold layer

Workshop 3 improves the Bronze/Silver pipeline from Workshop 2, adds scheduled incremental ingestion with watermarks and event identification across sources, and builds the Gold layer with PySpark. The Gold files are the only data source for the Plotly Dash dashboard in Workshop 4.

Course scope reminder (Team Project Assignment): Workshop 3 covers incremental ingestion, watermarks, scheduling, change detection, and event identification across sources.

## Layers

| Layer | Folder | Content | File name |
|---|---|---|---|
| Bronze | `datalake_bronze/` | Raw responses, unchanged | `source_YYYYMMDD_HHMMSS.ext` |
| Silver | `datalake_silver/` | Cleaned Parquet, one folder per topic | `topic/topic_YYYYMMDD_HHMMSS.parquet` |
| Gold | `datalake_gold/` | Governance KPIs and storytelling aggregations | `governance_YYYYMMDD_HHMMSS.parquet`, `storytelling_YYYYMMDD_HHMMSS.parquet` |

## Planned components

- [ ] Silver written to one folder per topic (`src/silver.py`)
- [ ] `airflow/dags/incremental_ingestion_dag.py`: scheduled feeds and daily USGS FDSN reconciliation, driven by the watermarks in `ingestion_state`
- [ ] New / updated / unchanged counts per run (`src/load.py`, `sql/03_workshop3.sql`)
- [ ] Event identification across sources (`src/matching.py`, `event_relation`)
- [ ] PySpark in the Airflow image (`airflow/Dockerfile`, `compose.yaml`)
- [ ] `airflow/dags/gold_processing_dag.py` with its transformations in `src/gold.py`
- [ ] `notebooks/03_data_quality.ipynb`: data quality findings (pandas)
- [ ] `notebooks/04_gold_preview.ipynb`: reads both Gold files
- [ ] Gold samples in `datalake_gold/samples/`
- [ ] `report/Team8_Workshop3_Tsunami.pdf`: Pipeline Improvements and Gold Layer Report
