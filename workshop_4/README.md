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
- [ ] Clean end-to-end run on one machine (Lina's laptop) and Gold samples
- [ ] Dashboard layout design and plain-language labels
- [x] Shared dashboard code: `dashboard/gold.py` (latest complete Gold pair, samples fallback, refresh) and `dashboard/theme.py` (Okabe-Ito palette, header, KPI cards, chart defaults)
- [ ] Governance app (KPI cards, null rates, volume over time, outlier rates, quality table, data health)
- [ ] Storytelling app (map, relevance level, bulletins, historical vs current, regional baseline, narrative)
- [ ] Compose service and run instructions
- [ ] Screenshots in `screenshots/` and `report/Team8_Workshop4_Tsunami.pdf`
- [ ] Tag `v1.0-workshop4`
