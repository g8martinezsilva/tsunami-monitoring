"""Incremental Bronze DAGs (Workshop 3).

incremental_ingestion (every 10 minutes): reads the watermark of each source from
ingestion_state, polls the USGS real-time feed, NTWC and PTWC, and saves a response only if it
holds something newer than the watermark. When something was saved, it writes the Bronze manifest
and the arrival marker in datalake_bronze/_new/, so the existing Silver DAG processes it unchanged.
The watermark advances only in load.py, in the same transaction as the data: if Silver or the load
fails, the next poll sees the same items again and nothing is lost.

usgs_fdsn_reconciliation (daily): asks USGS FDSN for events updated since (watermark - overlap),
to catch revisions made after an event left the real-time feed.
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from src.incremental import STAMP_FORMAT, fetch_fdsn_updates, fetch_feed_if_new, publish_run, read_watermarks
from src.sources import FEED_EXTENSIONS

log = logging.getLogger(__name__)


def _watermarks():
    connection = PostgresHook(postgres_conn_id="tsunami_db").get_conn()
    try:
        return read_watermarks(connection)
    finally:
        connection.close()


@dag(
    dag_id="incremental_ingestion",
    description="Poll USGS real-time, NTWC and PTWC; save only what is newer than the watermark",
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 11, 18),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["bronze", "incremental", "workshop3"],
)
def incremental_ingestion():

    @task
    def get_watermarks():
        return _watermarks()

    @task
    def poll_feed(source, watermarks, logical_date=None):
        try:
            return fetch_feed_if_new(source, logical_date.strftime(STAMP_FORMAT), watermarks.get(source))
        except Exception as error:
            log.exception("%s failed", source)
            return {"source": source, "error": str(error)}

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def publish(summaries, logical_date=None, run_id=None):
        return publish_run(list(summaries), logical_date, run_id, tag="feeds")

    watermarks = get_watermarks()
    summaries = poll_feed.partial(watermarks=watermarks).expand(source=list(FEED_EXTENSIONS))
    publish(summaries)


@dag(
    dag_id="usgs_fdsn_reconciliation",
    description="Daily USGS FDSN query of events revised since watermark - overlap",
    schedule="@daily",
    start_date=datetime(2026, 11, 18),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "incremental", "workshop3"],
)
def usgs_fdsn_reconciliation():

    @task
    def get_watermarks():
        return _watermarks()

    @task
    def reconcile(watermarks, logical_date=None):
        return fetch_fdsn_updates(logical_date.strftime(STAMP_FORMAT), watermarks.get("usgs_fdsn"))

    @task(retries=0)
    def publish(summary, logical_date=None, run_id=None):
        return publish_run([summary], logical_date, run_id, tag="fdsn")

    publish(reconcile(get_watermarks()))


incremental_ingestion()
usgs_fdsn_reconciliation()
