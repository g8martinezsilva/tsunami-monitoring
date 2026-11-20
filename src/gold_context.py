"""Database inputs of the Gold layer (Workshop 3).

Silver holds the observations; the pipeline state (load runs, watermarks) and the cross-source
links (event_relation) live in PostgreSQL. The Gold DAG reads these small tables once with
psycopg2 and hands them to Spark as DataFrames, so the Gold files remain the only data source of
the dashboard and Spark needs no JDBC driver.
"""

import pandas as pd

LINKS = """
SELECT f.source AS from_source, f.external_id AS from_external_id,
       t.source AS to_source, t.external_id AS to_external_id,
       r.relation_type, r.review_status
FROM event_relation r
JOIN event f ON f.event_id = r.from_event_id
JOIN event t ON t.event_id = r.to_event_id
"""
RUNS = """
SELECT source_id AS source, mode, status, started_at, finished_at
FROM ingestion_run
WHERE started_at >= now() - make_interval(days => %s)
   OR run_id IN (SELECT max(run_id) FROM ingestion_run WHERE status = 'success' GROUP BY source_id)
"""
STATE = "SELECT source_id AS source, watermark FROM ingestion_state"

COLUMNS = {
    "links": ["from_source", "from_external_id", "to_source", "to_external_id", "relation_type", "review_status"],
    "runs": ["source", "mode", "status", "started_at", "finished_at"],
    "state": ["source", "watermark"],
    "dag_runs": ["dag_id", "state", "start_date"],
}


def _frame(cursor, query, columns, params=None):
    cursor.execute(query, params)
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def read_database_context(connection, window_days=7):
    """Cross-source links, recent load runs and watermarks as pandas DataFrames."""
    with connection.cursor() as cursor:
        return {
            "links": _frame(cursor, LINKS, COLUMNS["links"]),
            "runs": _frame(cursor, RUNS, COLUMNS["runs"], (window_days,)),
            "state": _frame(cursor, STATE, COLUMNS["state"]),
        }


def empty_context():
    return {name: pd.DataFrame(columns=columns) for name, columns in COLUMNS.items()}
