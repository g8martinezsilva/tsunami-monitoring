"""PySpark session for the Gold layer (Workshop 3).

Spark runs in local mode inside the Airflow worker container: no cluster, the driver does all the
work with SPARK_MASTER threads. Memory is kept small because the whole stack runs on laptops with
7-8 GB of RAM; every setting can be changed through environment variables (compose.yaml / .env).
"""

import os

from pyspark.sql import SparkSession


def get_spark(app_name="tsunami-gold"):
    """Create (or reuse) the local SparkSession used by the Gold DAG."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        # Local mode: the driver is the only JVM, so its memory is Spark's whole budget
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        # Small data (thousands of rows): few shuffle partitions avoid many tiny tasks and files
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        # Timestamps are read and written in UTC, like the Silver layer
        .config("spark.sql.session.timeZone", "UTC")
        # Silver files of different runs may add columns over time
        .config("spark.sql.parquet.mergeSchema", "true")
        # No Spark web UI inside the container
        .config("spark.ui.enabled", "false")
        # The Airflow user (AIRFLOW_UID) may have no home directory in the image: keep Spark's
        # dependency cache and scratch files in /tmp instead of ~/.ivy2
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.local.dir", "/tmp/spark")
        .getOrCreate()
    )
