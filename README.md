# Tsunami Monitoring System

Team 8 project for Data Analysis Programming (2026-II), Universidad Distrital Francisco José de Caldas.

## Hazard

A tsunami is a series of ocean waves that can be caused by a large undersea earthquake. This project monitors recent Pacific earthquakes, places them in the context of historical earthquake and tsunami records, and incorporates official tsunami bulletins when available to assess potential tsunami relevance. Historical context does not imply a direct link between each recent earthquake and a historical tsunami.

This is a monitoring and correlation tool. It does not predict tsunamis or replace official warning systems.

**Scope:** The monitoring area uses three Marine Regions Pacific maritime regions, including waters near Alaska and Hawaii, and a project-defined 100 km influence area. Historical samples cover NOAA HazEL tsunamis from 2010–2025 and USGS FDSN earthquakes from 2010–2011 with magnitude ≥ 6. The source queries used NOAA HazEL regions 80–89 and a USGS rectangle spanning latitude −60..65 and longitude 100..300; these query filters are distinct from the geographic classification in the notebook.

## Team

- Lina Moreno
- Jaider Ávila
- Gabriela Martínez

## Data sources

Workshop 1 profiles saved samples from these four source families. Access, schedules, watermarks and citations for each source are in [config/sources.yaml](config/sources.yaml).

| Type | Source | Format | License / credit |
|---|---|---|---|
| Historical | [NOAA/NCEI Global Historical Tsunami Database (HazEL API)](https://www.ngdc.noaa.gov/hazel/view/swagger) | JSON | Public use; cite doi:10.7289/V5PN93H7 |
| Historical | [USGS earthquake catalog (FDSN)](https://earthquake.usgs.gov/fdsnws/event/1/) | GeoJSON, CSV | U.S. public domain; credit requested |
| Incremental | [USGS real-time earthquake feeds](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php) | GeoJSON | U.S. public domain; credit requested |
| Incremental | tsunami.gov warning-center feeds: [NTWC](https://www.tsunami.gov/events/xml/PAAQAtom.xml) and [PTWC](https://www.tsunami.gov/events/xml/PHEBAtom.xml) | Atom, CAP | U.S. Government work, public domain |
| Geographic reference | [Marine Regions, Global Oceans and Seas v1](https://doi.org/10.14284/542) | GeoJSON (WFS) | CC BY 4.0; not redistributed here |

## Repository structure

```text
src/           Ingestion package (sources, normalization, storage)
ingest.py      Command-line entry point
config/        Source configuration (sources.yaml) and geographic reference
datalake_bronze/ Bronze layer: raw landing zone (git-ignored except samples)
datalake_silver/ Silver layer: cleaned Parquet files
datalake_gold/   Gold layer: governance and storytelling summaries (Workshop 3)
airflow/        Airflow image (Dockerfile, requirements) and DAGs (airflow/dags/)
data/raw/      Workshop 1 raw landing zone (kept; replaced by datalake_bronze/)
sql/           Schema definitions
dashboard/     Plotly Dash application
tests/         Tests and recorded fixtures
notebooks/     Source profiling and analysis
docs/          Project documentation (diagrams, source-status log)
workshop_1/    Workshop 1 deliverables (report, mapping, samples, download scripts)
workshop_2/    Workshop 2 deliverables (report and plan)
workshop_3/    Workshop 3 deliverables (report and plan)
workshop_4/    Workshop 4 deliverables (dashboards report and screenshots)
compose.yaml   Docker Compose stack (PostGIS and Airflow)
```

## Setup

Python 3.12 to 3.14. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` with your own values; it is git-ignored and must never be committed.

## Ingestion

Start the project PostgreSQL service and export your connection settings using the variable names in `.env.example`. The CLI defaults to `localhost:5433`, database `tsunami_monitoring` and user `tsunami`. Set `POSTGRES_PASSWORD` to the database password if libpq needs it. From inside an Airflow container, set `POSTGRES_HOST=db` and `POSTGRES_PORT=5432` for the Compose database.

```bash
python ingest.py --mode historical --start 2010-01-01 --end 2025-12-31
python ingest.py --mode incremental
python ingest.py --mode status
```

- **Historical** downloads NOAA HazEL and USGS FDSN, writes Silver Parquet, loads PostgreSQL and reconciles event links. The date bounds are inclusive UTC dates. HazEL is fetched by year and Silver applies the requested dates. PostgreSQL keeps one observation per source and external ID when a period is reloaded.
- **Incremental** polls the USGS real-time, NTWC and PTWC feeds and reconciles FDSN from its committed revision watermark with the configured overlap. It retains original feed responses for batches that need processing and reuses the loader's checks for new, updated and unchanged observations. A poll with no changes can complete successfully.
- **Status** queries PostgreSQL in a read-only session and lists registered sources, committed watermarks, latest runs and their counts, recorded run statuses and current observation counts.

The write modes call the same Bronze, Silver, loader and matching functions as Airflow and finish after matching. CLI filenames carry a `cli` suffix and the CLI consumes its manifest directly. Airflow's Silver sensor follows arrival markers, which isolates its batches from the CLI. The loader serializes writes per source and commits the watermark with the data. You can keep Airflow active. Scheduling manual ingestion outside its polling times avoids repeated downloads.

The CLI uses `RAW_DIR` and `SILVER_DIR` for its files. Exit code `0` means success, `1` means a run or connection failed, and `2` indicates invalid arguments. An incremental run can load responses from successful sources and still return `1` to report a failed source. For an existing Workshop 2 volume, check the [Workshop 3 migration instructions](workshop_3/README.md#upgrade-an-existing-workshop-2-database) before running a write mode.

## Pipeline (Workshop 2): Docker, Airflow and the medallion layers

The stack runs PostgreSQL/PostGIS (project database, `localhost:5433`) and Apache Airflow 2.10 (UI at http://localhost:8080) with Docker Compose. With `.env` created (see Setup):

```bash
docker compose up -d          # PostGIS, Airflow metadata DB, webserver, scheduler, triggerer
docker compose ps             # all services running
```

Log in to Airflow with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env`, then:

1. Unpause `silver_processing`.
2. Trigger `bronze_ingestion` (optionally with other `start` / `end` dates). It downloads the four sources into `datalake_bronze/`, validates the files and writes a run manifest.
3. Within 10 minutes `silver_processing` detects the new Bronze run with its FileSensor, writes `topic_YYYYMMDD_HHMMSS.parquet` files to `datalake_silver/` and loads them into PostGIS.

The database schema (`sql/`) is created automatically the first time the database volume starts. Check the load with:

```bash
docker exec tsunami-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select * from ingestion_run"'
docker compose down           # stop; data stay in the Docker volumes and the datalake folders
```

Generated datalake files are git-ignored; selected samples are kept in each layer's `samples/` folder.

## Reproduce Workshop 1 profiling

The saved samples are in `workshop_1/samples/`; use those files to reproduce the reported results. To collect new samples for later analysis, run the downloaders separately:

```bash
python3 workshop_1/download_historical_samples.py
python3 workshop_1/download_incremental_samples.py
```

New snapshots can change which files the notebook discovers; `--force` can overwrite saved historical samples. The notebook is [notebooks/01_source_profiling.ipynb](notebooks/01_source_profiling.ipynb). Before running it, download the three selected Marine Regions boundaries from the official VLIZ web feature service (the resulting GeoJSON is about 85 MB; it takes a few minutes):

```bash
python3 workshop_1/download_marine_regions.py
```

The script creates `config/geographic_reference/pacific_maritime_regions.geojson`, which is excluded by `.gitignore`, so the boundaries are not redistributed in this repository. Use `--force` to download them again. The data are *Global Oceans and Seas v1* (Flanders Marine Institute, 2021, CC BY 4.0); see `config/geographic_reference/LICENSE_GOAS_v1.txt`. Run the notebook from the `notebooks/` directory, where its relative paths resolve to the saved samples and geographic file. The geographic section needs several GB of memory, so close other heavy programs before running it.

## Workshop 1 deliverables

- Report: [workshop_1/report/Team8_Workshop1_Tsunami.pdf](workshop_1/report/Team8_Workshop1_Tsunami.pdf) (LaTeX source `Team8_Workshop1_Tsunami.tex`)
- Source profiling notebook: [notebooks/01_source_profiling.ipynb](notebooks/01_source_profiling.ipynb)
- Raw samples: [workshop_1/samples/](workshop_1/samples/)
- Canonical mapping: [workshop_1/mapping.md](workshop_1/mapping.md)
- Entity–relationship diagram: [docs/er_diagram.md](docs/er_diagram.md)
- Architecture diagram: [docs/architecture.md](docs/architecture.md)
- Source-status log: [docs/source_status_log.md](docs/source_status_log.md)
- Recorded test fixtures: [tests/fixtures/](tests/fixtures/)

## Workshop 2 deliverables

- Report: [workshop_2/report/Team8_Workshop2_Tsunami.pdf](workshop_2/report/Team8_Workshop2_Tsunami.pdf) (LaTeX source `Team8_Workshop2_Tsunami.tex`)
- Docker stack: [compose.yaml](compose.yaml), [airflow/Dockerfile](airflow/Dockerfile), [airflow/requirements.txt](airflow/requirements.txt)
- DAGs: [airflow/dags/bronze_ingestion_dag.py](airflow/dags/bronze_ingestion_dag.py), [airflow/dags/silver_processing_dag.py](airflow/dags/silver_processing_dag.py)
- Shared code: [src/](src/) (sources, downloads, Silver preprocessing, database load)
- Database schema: [sql/](sql/)
- Pipeline samples: [datalake_bronze/samples/](datalake_bronze/samples/), [datalake_silver/samples/](datalake_silver/samples/)
- Silver exploration notebook: [notebooks/02_silver_exploration.ipynb](notebooks/02_silver_exploration.ipynb)
- Medallion architecture diagram: [docs/architecture_medallion.md](docs/architecture_medallion.md)

## Workshop 3 deliverables

- Report: [workshop_3/report/Team8_Workshop3_Tsunami.pdf](workshop_3/report/Team8_Workshop3_Tsunami.pdf) (LaTeX source `Team8_Workshop3_Tsunami.tex`)
- Exploratory notebook: [notebooks/03_data_quality.ipynb](notebooks/03_data_quality.ipynb)
- Gold preview notebook: [notebooks/04_gold_preview.ipynb](notebooks/04_gold_preview.ipynb)
- EDA and data curation notebook: [notebooks/05_data_curation.ipynb](notebooks/05_data_curation.ipynb), with its committed [input](datalake_bronze/samples/historical_20261117_001544/) and [curated output](datalake_silver/samples/curated/)
- Gold samples: [datalake_gold/samples/](datalake_gold/samples/)
- Details, components and run steps: [workshop_3/README.md](workshop_3/README.md)

### Gold layer

With the stack running, trigger the `gold_processing` DAG in Airflow at http://localhost:8080, or run:

```bash
docker exec tsunami-airflow-scheduler airflow dags trigger gold_processing
```

The output goes to `datalake_gold/` as a pair of Parquet directories (`governance_*` and `storytelling_*`). Open the exploratory notebook or [Gold preview notebook](notebooks/04_gold_preview.ipynb) in VS Code or Jupyter, select the project Python kernel and run all cells from the repository root or `notebooks/`.
