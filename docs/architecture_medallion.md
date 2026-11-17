# Workshop 2 medallion architecture diagram

Bronze/Silver pipeline as implemented in Workshop 2, with the Gold layer and the dashboard planned for later workshops. Everything runs locally with Docker Compose (`compose.yaml`). The report figure `workshop_2/report/figures/architecture_medallion.png` is exported from this diagram with [mermaid.live](https://mermaid.live) (Config: `{"theme": "default", "look": "classic"}`).

```mermaid
flowchart LR
    subgraph sources["Sources"]
        hazel["NOAA HazEL<br/>JSON, historical"]
        fdsn["USGS FDSN<br/>CSV, historical"]
        feed["USGS real-time feed<br/>GeoJSON"]
        tg["NTWC / PTWC<br/>Atom + CAP"]
    end

    subgraph docker["Docker Compose"]
        subgraph airflow["Apache Airflow 2.10 (scheduler, webserver, triggerer, metadata DB)"]
            bronze_dag["bronze_ingestion DAG<br/>extract, validate, manifest"]
            silver_dag["silver_processing DAG<br/>FileSensor, clean, load"]
        end
        db[("PostGIS<br/>canonical model")]
    end

    subgraph lake["Data lake (host folders)"]
        bronze["datalake_bronze/<br/>raw files + manifest<br/>source_YYYYMMDD_HHMMSS.ext"]
        silver["datalake_silver/<br/>Parquet<br/>topic_YYYYMMDD_HHMMSS.parquet"]
        gold["datalake_gold/<br/>(Workshop 3)"]
    end

    dash["Plotly Dash<br/>(Workshop 4)"]

    hazel --> bronze_dag
    fdsn --> bronze_dag
    feed --> bronze_dag
    tg --> bronze_dag
    bronze_dag --> bronze
    bronze -- "arrival marker _new/" --> silver_dag
    silver_dag --> silver
    silver_dag --> db
    silver -.-> gold
    db -.-> dash
    gold -.-> dash
```
