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
datalake_gold/   Gold layer: aggregates (Workshop 3)
airflow/dags/  Airflow DAGs
sql/           Schema definitions
dashboard/     Plotly Dash application
tests/         Tests and recorded fixtures
notebooks/     Source profiling and analysis
docs/          Project documentation (diagrams, source-status log)
workshop_1/    Workshop 1 deliverables (report, mapping, samples, download scripts)
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

```bash
python3 ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]
python3 ingest.py --mode incremental
python3 ingest.py --mode status
```

In Workshop 1, `ingest.py` is a stub: it downloads the raw responses, with retries, into the Bronze layer `datalake_bronze/` (or the folder in `RAW_DIR`) as `source_YYYYMMDD_HHMMSS.ext`, and `--mode status` lists the saved files. Parsing, validation and the database load are added in Workshops 2 and 3.

## Reproduce Workshop 1 profiling

The saved samples are in `workshop_1/samples/`; use those files to reproduce the reported results. To collect new samples for later analysis, run the downloaders separately:

```bash
python3 workshop_1/download_historical_samples.py
python3 workshop_1/download_incremental_samples.py
```

New snapshots can change which files the notebook discovers; `--force` can overwrite saved historical samples. The notebook is [notebooks/01_source_profiling.ipynb](notebooks/01_source_profiling.ipynb). Before running it, download the three selected Marine Regions boundaries from the official VLIZ web feature service (the resulting GeoJSON is about 145 MB; it takes a few minutes):

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
