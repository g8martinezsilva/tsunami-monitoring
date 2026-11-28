# Workshop 4 — Governance and storytelling dashboards

Workshop 4 builds two independent Plotly Dash applications that read only the Gold Parquet files: a governance dashboard for the data team and a storytelling dashboard for the functional user, a Pacific monitoring analyst (Workshop 1). It also verifies the whole Bronze → Silver → Gold pipeline end to end.

Course scope reminder (Team Project Assignment): Workshop 4 covers temporal and geographic analysis, the historical-versus-current comparison, the hazard indicator (Tsunami Relevance Level) and the dashboard. The guide's sentiment components are replaced by tsunami equivalents.

## Applications

| App | File | Address | Reads |
|---|---|---|---|
| Governance | `dashboard/governance_app.py` | http://localhost:8050 | `datalake_gold/governance_*.parquet` |
| Storytelling | `dashboard/storytelling_app.py` | http://localhost:8051 | `datalake_gold/storytelling_*.parquet` |

## Checklist

- [x] Branch `feat/workshop4-dashboards` and dashboard dependencies in `requirements.txt`
- [x] Gold additions for duplicate rate, schema compliance, field outlier rates and box-plot statistics, volume over time, quality thresholds and statuses, and `historical_vs_current`
- [x] End-to-end Bronze → Silver → Gold run in a clean environment with shared Gold samples
- [x] Dashboard layout design and plain-language labels (sketches in `report/figures/`)
- [x] Shared dashboard code: `dashboard/gold.py` (latest complete Gold pair, samples fallback, refresh) and `dashboard/theme.py` (Okabe-Ito palette, header, KPI cards, chart defaults)
- [x] Governance app `dashboard/governance_app.py` (KPI cards, null rates, volume over time, box plots, outlier rates, quality table and matrix, pipeline health)
- [x] Storytelling app `dashboard/storytelling_app.py` (main message, 24 h / 7 days filter, level cards, map, daily activity, bulletins, historical vs current, regional baseline, magnitudes, tsunamis per year)
- [x] Docker Compose integration for `governance-dashboard` and `storytelling-dashboard`
- [x] Fresh-clone verification. Both dashboards started using the documented commands and loaded the shared Gold sample.
- [x] Refresh Data validation. Both running dashboards loaded the new Gold publication after incremental ingestion, Silver processing and Gold processing. [Refresh evidence](evidence/refresh/)
- [x] Final report. [Workshop 4 report (PDF)](report/Team8_Workshop4_Tsunami.pdf)
- [x] Visual evidence. [Airflow pipeline](evidence/pipeline/) · [Governance](evidence/governance/) · [Storytelling](evidence/storytelling/)
- [ ] Tag `v1.0-workshop4`

## Run the dashboards

With Docker: `docker compose up -d governance-dashboard` and `docker compose up -d storytelling-dashboard` (stop both with `docker compose --profile dashboards stop`). Or from the repository root, with the project venv (`pip install -r requirements.txt`):

```bash
python -m dashboard.governance_app      # http://localhost:8050
python -m dashboard.storytelling_app    # http://localhost:8051
```

Both read the newest complete Gold pair among `datalake_gold/` and `datalake_gold/samples/` (set `GOLD_DIR` to use another folder). **Refresh data** reloads a newer Gold run without restarting.

## Shared Gold sample

The report's analytical results use the committed Gold publication `20261128_040949`. For a local run with the project virtual environment, set `GOLD_DIR=datalake_gold/samples` when starting either dashboard to reproduce those results. [Governance Parquet](../datalake_gold/samples/governance_20261128_040949.parquet/) · [Storytelling Parquet](../datalake_gold/samples/storytelling_20261128_040949.parquet/)

## Future work

Run the pipeline continuously for longer during the final project to collect more history for volume, freshness and run compliance.
