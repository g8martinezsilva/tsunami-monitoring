# Workshop 4 — Governance and storytelling dashboards

Workshop 4 provides two independent Plotly Dash applications that read only the Gold Parquet files. Governance serves the data team, and Storytelling serves the Pacific monitoring analyst defined in Workshop 1.

Course scope reminder (Team Project Assignment): Workshop 4 covers temporal and geographic analysis, the historical-versus-current comparison, the hazard indicator (Tsunami Relevance Level) and the dashboard. The guide's sentiment components are replaced by tsunami equivalents.

## Applications

| App | Address | Reads |
|---|---|---|
| Governance | http://localhost:8050 | `datalake_gold/governance_*.parquet` |
| Storytelling | http://localhost:8051 | `datalake_gold/storytelling_*.parquet` |

## Deliverables

### Dashboard Implementation Report

The report documents both dashboards, the Gold indicators, the visualization design and the integration evidence. [Workshop 4 report (PDF)](report/Team8_Workshop4_Tsunami.pdf)

### Implementation Code

[Governance app](../dashboard/governance_app.py) · [Storytelling app](../dashboard/storytelling_app.py) · [Gold reader](../dashboard/gold.py) · [Shared theme](../dashboard/theme.py) · [Docker Compose](../compose.yaml)

### Visual Documentation

[Airflow pipeline](evidence/pipeline/) · [Governance](evidence/governance/) · [Storytelling](evidence/storytelling/) · [Refresh Data](evidence/refresh/)

### Integration and Reproducibility

The team verified the Bronze → Silver → Gold pipeline in a clean environment. From a fresh clone, both dashboard services started with Docker Compose using the documented commands and loaded the shared Gold sample. After running incremental ingestion, Silver processing and Gold processing in sequence, **Refresh Data** loaded the newer publication in both running dashboards.

### Gold Data Samples

The report's analytical results use the committed Gold publication `20261128_040949`. [Governance Parquet](../datalake_gold/samples/governance_20261128_040949.parquet/) · [Storytelling Parquet](../datalake_gold/samples/storytelling_20261128_040949.parquet/)

## Run the Dashboards

With Docker: `docker compose up -d governance-dashboard` and `docker compose up -d storytelling-dashboard` (stop both with `docker compose --profile dashboards stop`). Or from the repository root, with the project venv (`pip install -r requirements.txt`):

```bash
python -m dashboard.governance_app      # http://localhost:8050
python -m dashboard.storytelling_app    # http://localhost:8051
```

Both read the newest complete Gold pair among `datalake_gold/` and `datalake_gold/samples/` (set `GOLD_DIR` to use another folder). **Refresh data** reloads a newer Gold run without restarting.

## Shared Gold Sample

To reproduce the reported results with the project virtual environment, set `GOLD_DIR=datalake_gold/samples` when starting either dashboard.

## Future Work

Run the pipeline continuously for longer during the final project to collect more history for volume, freshness and run compliance.
