# Workshop 1 architecture diagram

High-level architecture of the tsunami monitoring system, adapted from the course reference architecture. All components run locally with Docker Compose. The scheduler runs `ingest.py --mode incremental`; the Plotly Dash dashboard reads only from the database. The report figure `workshop_1/report/figures/architecture.png` is exported from this diagram with [mermaid.live](https://mermaid.live) (Config: `{"theme": "default", "look": "classic"}`).

```mermaid
flowchart LR
    subgraph sources["Sources"]
        hazel["NOAA HazEL<br/>JSON, historical"]
        fdsn["USGS FDSN<br/>CSV, historical"]
        feed["USGS real-time feed<br/>GeoJSON, every minute"]
        tg["NTWC / PTWC<br/>Atom + CAP, every 5 min"]
    end

    sched["Scheduler<br/>(Airflow DAG)"]
    ingest["ingest.py<br/>historical / incremental / status"]
    raw["Raw landing zone<br/>data/raw/"]
    proc["Normalize, validate,<br/>deduplicate, match"]
    db[("PostgreSQL<br/>canonical model")]
    quar["Quarantine<br/>rejected_record"]
    ana["Analytics and<br/>TRL indicator"]
    dash["Plotly Dash<br/>localhost:8050"]

    hazel --> ingest
    fdsn --> ingest
    feed --> ingest
    tg --> ingest
    sched --> ingest
    ingest --> raw
    raw --> proc
    proc --> db
    proc --> quar
    db --> ana
    ana --> dash
```

The geographic scope, ingestion contract and indicator shown here are described in the Workshop 1 report (Sections 1.3, 6 and 7.4).
