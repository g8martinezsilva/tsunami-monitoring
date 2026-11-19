# Airflow image for the tsunami monitoring pipeline.
# The base image already provides pandas, pyarrow, requests, PyYAML and psycopg2;
# airflow/requirements.txt lists what the DAGs need so that new packages are added in one place.
FROM apache/airflow:2.10.4

# Java runtime for PySpark (Gold DAG, Workshop 3). Spark runs in local mode inside the
# Airflow worker, so no Spark cluster or extra container is needed. procps provides `ps`,
# which the Spark launch scripts call.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow

COPY airflow/requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
