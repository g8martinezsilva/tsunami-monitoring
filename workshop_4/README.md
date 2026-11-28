# Workshop 4 — Governance and storytelling dashboards

Workshop 4 builds two independent Plotly Dash applications that read only the Gold Parquet files: a governance dashboard for the data team and a storytelling dashboard for the functional user, a Pacific monitoring analyst (Workshop 1). It also verifies the whole Bronze → Silver → Gold pipeline end to end.

Course scope reminder (Team Project Assignment): Workshop 4 covers temporal and geographic analysis, the historical-versus-current comparison, the hazard indicator (Tsunami Relevance Level) and the dashboard. The guide's sentiment components are replaced by tsunami equivalents.

## Applications

| App | File | Address | Reads |
|---|---|---|---|
| Governance | `dashboard/governance_app.py` | http://localhost:8050 | `datalake_gold/governance_*.parquet` |
| Storytelling | `dashboard/storytelling_app.py` | http://localhost:8051 | `datalake_gold/storytelling_*.parquet` |

## Plan

- [x] Branch `feat/workshop4-dashboards`, dashboard dependencies in `requirements.txt`, this folder and the report skeleton
- [x] Gold additions (`src/gold_dashboard.py`, `gold_dashboard` block of `config/sources.yaml`): duplicate rate, schema compliance rate, outlier rate per numeric field, volume over time, quality thresholds and status, `historical_vs_current`
- [x] Clean end-to-end run on one machine (Lina's laptop) and Gold samples (final run with more history still to do)
- [x] Dashboard layout design and plain-language labels (sketches in `report/figures/`)
- [x] Shared dashboard code: `dashboard/gold.py` (latest complete Gold pair, samples fallback, refresh) and `dashboard/theme.py` (Okabe-Ito palette, header, KPI cards, chart defaults)
- [x] Governance app `dashboard/governance_app.py` (KPI cards, null rates, volume over time, box plots, outlier rates, quality table and matrix, pipeline health)
- [x] Storytelling app `dashboard/storytelling_app.py` (main message, 24 h / 7 days filter, level cards, map, daily activity, bulletins, historical vs current, regional baseline, magnitudes, tsunamis per year)
- [x] Compose services `governance-dashboard` and `storytelling-dashboard` (`dashboard/Dockerfile`) and run instructions
- [ ] Screenshots in `screenshots/` and `report/Team8_Workshop4_Tsunami.pdf`
- [ ] Tag `v1.0-workshop4`

## Run the dashboards

With Docker: `docker compose up -d governance-dashboard` and `docker compose up -d storytelling-dashboard` (stop both with `docker compose --profile dashboards stop`). Or from the repository root, with the project venv (`pip install -r requirements.txt`):

```bash
python -m dashboard.governance_app      # http://localhost:8050
python -m dashboard.storytelling_app    # http://localhost:8051
```

Both read the newest complete Gold pair among `datalake_gold/` and `datalake_gold/samples/` (set `GOLD_DIR` to use another folder). **Refresh data** reloads a newer Gold run without restarting.
