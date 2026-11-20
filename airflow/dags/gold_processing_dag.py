"""Refresh the Parquet Gold indicators with one local Spark session."""

import os
from datetime import datetime, timedelta, timezone

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

        spark = get_spark()
        try:
            silver_dir = os.environ.get("SILVER_DIR", "datalake_silver")
            gold_dir = os.environ.get("GOLD_DIR", "datalake_gold")
            paths = run_gold(spark, silver_dir, gold_dir)
            return {name: str(path) for name, path in paths.items()}
        finally:
            spark.stop()

    build_gold()


gold_processing()
