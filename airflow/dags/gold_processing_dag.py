"""Refresh the Parquet Gold indicators with one local Spark session.

Besides the Silver Parquet folders, Gold reads two small inputs once per run: the cross-source
links, load runs and watermarks from PostgreSQL (src/gold_context.py), and the recent Airflow DAG
runs for the orchestration KPI. If the database cannot be read, the task fails and retries."""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task


@dag(
    dag_id="gold_processing",
    description="Consolidate Silver Parquet snapshots and publish Gold indicators",
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["gold", "workshop3"],
)
def gold_processing():
    @task
    def build_gold():
        from src.gold import run_gold
        from src.spark_session import get_spark

        from airflow.models import DagRun
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.utils.session import create_session

        from src.config import load_gold_settings
        from src.gold_context import read_database_context

        window_days = load_gold_settings()["run_window_days"]
        connection = PostgresHook(postgres_conn_id="tsunami_db").get_conn()
        try:
            context = read_database_context(connection, window_days)
        finally:
            connection.close()
        since = datetime.now(timezone.utc) - timedelta(days=window_days)
        with create_session() as session:
            runs = session.query(DagRun.dag_id, DagRun.state, DagRun.start_date).filter(
                DagRun.start_date >= since).all()
        context["dag_runs"] = pd.DataFrame([tuple(run) for run in runs], columns=["dag_id", "state", "start_date"])

        spark = get_spark()
        try:
            silver_dir = os.environ.get("SILVER_DIR", "datalake_silver")
            gold_dir = os.environ.get("GOLD_DIR", "datalake_gold")
            paths = run_gold(spark, silver_dir, gold_dir, context=context)
            return {name: str(path) for name, path in paths.items()}
        finally:
            spark.stop()

    build_gold()


gold_processing()
