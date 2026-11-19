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

- [x] Silver written to one folder per topic (`src/silver.py`)
- [x] `airflow/dags/incremental_ingestion_dag.py`: scheduled feeds (`incremental_ingestion`, every 10 minutes) compared by source identifier, and daily USGS FDSN reconciliation (`usgs_fdsn_reconciliation`) using the committed watermark
- [x] New / updated / unchanged counts per run (`src/load.py`, `sql/03_workshop3.sql`)
- [x] Event identification across sources (`src/matching.py`, `event_relation`; thresholds in the `event_matching` block of `config/sources.yaml`)
- [x] PySpark in the Airflow image (`airflow/Dockerfile`, `compose.yaml`, session settings in `src/spark_session.py`)
- [ ] `airflow/dags/gold_processing_dag.py` with its transformations in `src/gold.py`
- [ ] `notebooks/03_data_quality.ipynb`: data quality findings (pandas)
- [ ] `notebooks/04_gold_preview.ipynb`: reads both Gold files
- [ ] Gold samples in `datalake_gold/samples/`
- [ ] `report/Team8_Workshop3_Tsunami.pdf`: Pipeline Improvements and Gold Layer Report

## Upgrade an existing Workshop 2 database

Compose mounts `sql/` as initialization scripts for a new PostgreSQL volume. For an existing volume, run the following read-only check against the project container. Confirm the database name in the output. The expected column is `integer` with `NOT NULL` and a default of `0`. Zero rows means the column is absent.

```bash
docker exec -i tsunami-postgres sh -c \
  'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
BEGIN READ ONLY;
SELECT current_database();
SELECT data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = current_schema()
  AND table_name = 'ingestion_run' AND column_name = 'unchanged_count';
ROLLBACK;
SQL
```

When ready to migrate the selected database, apply the SQL below and rerun the read-only check. The migration adds the column in one transaction and backfills successful runs. It can be run again and times out if it cannot acquire the table lock within five seconds.

```bash
docker exec -i tsunami-postgres sh -c \
  'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < sql/03_workshop3.sql
```

## Incremental change detection

The real-time and CAP polls compare every available observation with its committed `(source, external_id)` row. The real-time feed provides a 30-day window, while CAP can only be checked for entries still in Atom. FDSN reconciliation queries from the committed watermark minus the configured overlap, currently 24 hours.

The business hash covers attributes persisted in the canonical model. Revision timestamps, Atom entry identifiers and Bronze/Silver lineage are tracked separately. Only a business change increments `updated_count`. Older USGS revisions are ignored, and a newer revision with the same content advances its metadata. CAP uses Atom update times when both versions have them and falls back to `sent` otherwise. If versions tie and payloads differ, the last successful load takes precedence. Legacy hashes are refreshed when equivalent Silver rows are loaded, without counting them as business updates.

Run the fixture and mock tests from the repository root with `python -B -m unittest discover -s tests -v`. To include tests against PostgreSQL with PostGIS, set `TSUNAMI_TEST_DSN` to a disposable test database, separate from the project database.
