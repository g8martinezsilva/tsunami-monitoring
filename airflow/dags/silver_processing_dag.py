"""Silver processing DAG: Bronze -> Silver Parquet -> PostgreSQL.

A FileSensor waits for a new Bronze run, signalled by an arrival marker in
datalake_bronze/_new/ that the Bronze DAG writes after a successful, validated run.
For each new Bronze run, the preprocessing of src/silver.py writes
topic_YYYYMMDD_HHMMSS.parquet files (earthquakes, tsunami_events, tsunami_bulletins,
rejected_records) to datalake_silver/, the load of src/load.py upserts them into the
canonical model, and the marker is removed. Markers are removed only after a successful
load, and the database load is an upsert, so processing is idempotent.
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor

from src.downloads import bronze_dir
from src.load import load_silver_run
from src.silver import process_bronze_run

log = logging.getLogger(__name__)

STAMP_FORMAT = "%Y%m%d_%H%M%S"
MARKERS = "_new"


@dag(
    dag_id="silver_processing",
    description="Clean new Bronze runs into Silver Parquet files and load them into PostgreSQL",
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["silver", "workshop2"],
)
def silver_processing():

    wait_for_bronze = FileSensor(
        task_id="wait_for_bronze",
        fs_conn_id="fs_default",
        filepath=str(bronze_dir() / MARKERS / "bronze_manifest_*.json"),
        poke_interval=60,          # check once a minute
        timeout=9 * 60,            # give up before the next 10-minute run starts
        mode="reschedule",         # free the worker slot between checks
        soft_fail=True,            # no new Bronze run: mark as skipped, not failed
    )

    @task
    def find_new_bronze_runs():
        markers = sorted((bronze_dir() / MARKERS).glob("bronze_manifest_*.json"))
        log.info("new Bronze runs: %s", [marker.name for marker in markers])
        return [str(bronze_dir() / marker.name) for marker in markers]

    @task
    def bronze_to_silver(manifests, logical_date=None):
        silver_dir = os.environ.get("SILVER_DIR", "datalake_silver")
        results = []
        for number, manifest in enumerate(manifests):
            # One processing timestamp per Bronze run; later runs of the same batch add seconds
            stamp = (logical_date + timedelta(seconds=number)).strftime(STAMP_FORMAT)
            result = process_bronze_run(manifest, silver_dir, stamp)
            log.info("%s -> %s", Path(manifest).name, {t: o["rows"] for t, o in result["outputs"].items()})
            results.append(result)
        return results

    @task
    def load_to_database(silver_runs):
        connection = PostgresHook(postgres_conn_id="tsunami_db").get_conn()
        try:
            return [load_silver_run(connection, run) for run in silver_runs]
        finally:
            connection.close()

    @task
    def clear_markers(silver_runs, loaded):
        for run in silver_runs:
            (bronze_dir() / MARKERS / run["bronze_manifest"]).unlink(missing_ok=True)
        return [run["bronze_manifest"] for run in silver_runs]

    manifests = find_new_bronze_runs()
    silver_runs = bronze_to_silver(manifests)
    loaded = load_to_database(silver_runs)
    wait_for_bronze >> manifests
    clear_markers(silver_runs, loaded)


silver_processing()
