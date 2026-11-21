# Workshop 3 — Pipeline improvements, incremental ingestion and Gold layer

Workshop 3 improves the Bronze/Silver pipeline from Workshop 2, adds scheduled incremental ingestion with watermarks and event identification across sources, and builds the Gold layer with PySpark. The Gold files are the only data source for the Plotly Dash dashboard in Workshop 4.

Course scope reminder (Team Project Assignment): Workshop 3 covers incremental ingestion, watermarks, scheduling, change detection, and event identification across sources.

## Layers

| Layer | Folder | Content | File name |
|---|---|---|---|
| Bronze | `datalake_bronze/` | Raw responses, unchanged | `source_YYYYMMDD_HHMMSS.ext` |
| Silver | `datalake_silver/` | Cleaned Parquet, one folder per topic | `topic/topic_YYYYMMDD_HHMMSS.parquet` |
| Gold | `datalake_gold/` | Governance KPIs and storytelling aggregations | `governance_YYYYMMDD_HHMMSS.parquet`, `storytelling_YYYYMMDD_HHMMSS.parquet` |

## Deliverables

- [x] [Pipeline Improvements and Gold Layer Report](report/Team8_Workshop3_Tsunami.pdf)
- [x] [Exploratory notebook](../notebooks/03_data_quality.ipynb)
- [x] [GitHub repository](https://github.com/g8martinezsilva/tsunami-monitoring)
- [x] [Gold samples](../datalake_gold/samples/)
- [x] [Gold preview notebook](../notebooks/04_gold_preview.ipynb)

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

## Event identification

Version 2 of the matching policy uses the thresholds in [config/sources.yaml](../config/sources.yaml). NOAA clock components are interpreted in UTC under the Workshop 1 assumption. For partial times the search uses the recorded minute, hour or day as an interval with an exclusive upper bound. The original timestamp stays null.

| Observation | Time window | Other candidate criteria | Automatic acceptance |
|---|---|---|---|
| NOAA with seconds | Within 15 minutes of the recorded time | Earthquake-related cause, within 100 km and magnitude difference ≤ 0.5 when both values are available | One distinct USGS identifier within 120 s and 25 km, earthquake-only cause, probable or definite tsunami, and both magnitudes agree |
| NOAA with minute precision | The recorded minute | Same geographic, cause and optional magnitude checks | Pending review |
| NOAA with hour or day precision | The recorded hour or calendar day | Earthquake-related cause, within 100 km, and both magnitudes available with difference ≤ 0.5 | Pending review |
| NTWC or PTWC | Within 120 s of the earthquake origin reported in CAP | Within 100 km and magnitude difference ≤ 0.5 when both values are available | One distinct USGS identifier within 25 km, with agreeing magnitudes of the same reported type |
| USGS real-time and FDSN | No time filter | Exact USGS identifier | Group verified observations when existing memberships and decisions allow it |

Missing or unknown NOAA causes provide insufficient evidence for a causal link. Entries catalogued as errors or seiches are excluded. The magnitude check compares the earthquake magnitude reported by the source with USGS. The 15-minute window, 100 km radius and magnitude tolerance come from the Workshop 1 exploratory comparison. The 120 s and 25 km limits retain a narrower operational acceptance window. The 25 km threshold is a project choice informed by the nearby Workshop 1 examples. All proximity rules remain heuristics. Tsunami and earthquake coordinates and times can differ, magnitude types can differ, and a partial interval can miss an earthquake across its boundary.

When a revision contradicts an automatic link, the matcher retains its relation ID and marks it `rejected` with `state: invalidated` in the evidence. When inputs are missing, the link stays available for review as `pending`. Evidence includes both source references, evaluated criteria and the last eligible assessment when saved during a transition. An automatic link can become eligible again. Record a manual decision with separate evidence that names the reviewer and justification. The matcher respects changed statuses and evidence owned by a reviewer. A confirmation that keeps both the original status and the original automatic evidence cannot be distinguished from the automated decision.

An accepted USGS identity can consolidate two groups. The merge moves all members, retains the previous group evidence and removes the empty group row. A conflicting rejection or group type prevents an automatic merge.

The Silver DAG clears arrival markers after both the load and matching commit. After a matching failure, retry `link_events` and its downstream cleanup in the same Airflow run to reuse the confirmed load. The retained marker also allows recovery from the saved data on a later Silver run.

The candidate queries use indexed time windows and exact source identifiers. Existing links are also reassessed when they fall outside a window. With the current data volume, keep the complete reconciliation. If it slows down as history grows, use `EXPLAIN (ANALYZE, BUFFERS)` to check before adding a geography index or restricting recomputation to affected events and their earlier links.

## Gold indicators

Gold reads the topic directories and the flat historical Parquet files at the root of `SILVER_DIR` with one local Spark session. Each `(source, external_id)` key contributes one current row across both layouts. USGS selection starts with `source_updated_at`. CAP selection uses `atom_updated_at` when present and otherwise `sent_at`. For NOAA and ties in source revisions, the order uses the Bronze run time in the manifest name, falling back to Silver processing time or the Silver filename stamp. Silver processing time, the original path and serialized row values settle remaining ties.

Gold reads compatible files directly and skips empty snapshots. It reads Parquet nanosecond timestamps from temporary microsecond copies to match Spark 3.5 precision. A batch with only untyped null rejection IDs gets a temporary copy with a nullable string ID. Gold uses the original file path for revision selection and removes the copies after the run. Missing timestamps and partial NOAA clock fields retain their source values.

Gold counts observations by source. Physical-event counts would require verified identities across sources. A `caused_by` link connects distinct phenomena. The sample directory remains an independent input.

| Dataset | Measures |
|---|---|
| Governance | Processed rows, unique observations and rows beyond the first for each key, by topic and source. Quality counts and percentages use the current observations. Missing coordinates means either coordinate is absent. Incomplete time means `event_time` or, for CAP, `sent_at` is absent. Each percentage divides by `unique_observations`. Depth and maximum water height outlier counts reuse the selected rows' Silver IQR flags. The `rejected_records` topic reports processed and unique rejections by source. |
| Storytelling | Annual earthquake and historical tsunami observations with a known calendar year. NOAA's `event_date` contributes a year even when its timestamp is null. Finite earthquake magnitudes use 0.5-wide bins grouped by magnitude type. Finite maximum tsunami water heights use 1 m bins. Bin lower bounds are inclusive, and upper bounds are exclusive. Bulletin counts are by issuing center. |
| Governance (user stories US5, US6) | `null_rate`: missing values per field, topic and source. `pipeline_state`: last successful load and watermark age per source (PostgreSQL). `orchestration`: successful Airflow runs against the runs expected from each schedule over `run_window_days`. `matching`: eligible tsunamis and bulletins, accepted and pending links, coverage (`event_relation`). Code: `src/gold_indicators.py`, inputs read by `src/gold_context.py`. |
| Storytelling (US1–US4) | `recent_earthquakes`: USGS real-time earthquakes with M ≥ 4.5 inside the FDSN query area, with region cell, linked bulletin and Tsunami Relevance Level (M ≥ 6). `regional_baseline`: historical M ≥ 6 earthquakes and probable/definite tsunamis per 10° cell. `relevance_level_distribution` and `relevance_validation`: the Workshop 1 indicator over the catalog. Parameters: `tsunami_relevance` and `gold` blocks of `config/sources.yaml`. The level is a monitoring aid, not a tsunami prediction. |

`rejected_records` counts each rejection row from every saved run, including replays. `unique_rejected_records` counts distinct `(source, external_id, bronze_file, locator, reason)` combinations. The same Bronze record rejected again for the same reason counts once even if its external ID is missing. A different Bronze file, location within that file or rejection reason contributes a separate record.

### Run Gold in Airflow

1. In Airflow at `http://localhost:8080`, unpause `gold_processing` and select **Trigger DAG**. The schedule runs daily at 06:00 UTC with catchup disabled. The DAG also accepts manual runs. It reads existing Silver files without starting ingestion, matching or a database load.
2. Open the run and check that `build_gold` succeeded. Its logs show the number of Silver files, per-source input counts, output row counts and output paths. The task retries once after five minutes if a read or write fails.
3. In `datalake_gold/`, find the two directories with the same UTC stamp and a Spark `_SUCCESS` marker in each. Each directory contains Parquet `part-*.parquet` files. A refresh publishes a new pair and retains previous pairs. For Pandas, pass the full directory to `pd.read_parquet(path)`. For Spark, use `spark.read.parquet(str(path))`.

The same manual run can be started from the shell when the stack is running.

```bash
docker exec tsunami-airflow-scheduler airflow dags unpause gold_processing
docker exec tsunami-airflow-scheduler airflow dags trigger gold_processing
docker exec tsunami-airflow-scheduler airflow dags list-runs -d gold_processing
```

Open [the data quality notebook](../notebooks/03_data_quality.ipynb) in VS Code or Jupyter. Select the project's Python kernel with Pandas, PyArrow and Matplotlib, then choose **Run All** from the repository root or `notebooks/`. The notebook uses the latest complete Gold pair. If the default Silver root has no topic files, the notebook uses the saved Silver samples. Set `SILVER_DIR` and `GOLD_DIR` before opening the kernel to inspect another run's input and output roots.

Open [the Gold preview notebook](../notebooks/04_gold_preview.ipynb) with the same kernel and choose **Run All** to inspect the saved Gold samples.

Run the integration tests using the installed Airflow and Spark inside the container. The fixtures and outputs are temporary and the tests run without a project database connection.

```bash
docker exec -i tsunami-airflow-scheduler bash -c '
  set -eu
  test_dir=$(mktemp -d /tmp/tsunami-gold-tests.XXXXXX)
  cat > "$test_dir/test_workshop3_gold.py"
  python -B -m unittest discover -s "$test_dir" -v
' < tests/test_workshop3_gold.py
```
