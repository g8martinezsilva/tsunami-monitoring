# Airflow image for the tsunami monitoring pipeline.
# The base image already provides pandas, pyarrow, requests, PyYAML and psycopg2;
# airflow/requirements.txt lists what the DAGs need so that new packages are added in one place.
FROM apache/airflow:2.10.4

COPY airflow/requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
