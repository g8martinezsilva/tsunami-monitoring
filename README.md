# Tsunami Monitoring System

Team 8 project for Data Analysis Programming (2026-II), Universidad Distrital Francisco José de Caldas.

## Hazard

A tsunami is a series of ocean waves usually caused by a large undersea earthquake. This system integrates historical and near-real-time earthquake and tsunami data to show how relevant a significant earthquake may be, based on its magnitude, depth, location, historical tsunami association, and official alerts.

This is a monitoring and correlation tool. It does not predict tsunamis.

**Scope:** to be defined in Workshop 1.

## Team

- Lina Moreno
- Jaider Ávila
- Gabriela Martínez

## Data sources

These sources are being validated in Workshop 1.

| Type | Source | Format |
|---|---|---|
| Historical | [NOAA/NCEI Global Historical Tsunami Database (HazEL API)](https://www.ngdc.noaa.gov/hazel/view/swagger) | JSON |
| Historical | [USGS earthquake catalog (FDSN)](https://earthquake.usgs.gov/fdsnws/event/1/) | GeoJSON, CSV |
| Incremental | [USGS real-time earthquake feeds](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php) | GeoJSON |
| Incremental | [tsunami.gov warning-center feeds](https://www.tsunami.gov/events/xml/PAAQAtom.xml) | Atom, CAP |

## Repository structure

```text
src/           Ingestion package (sources, normalization, storage)
ingest.py      Command-line entry point
config/        Source configuration (sources.yaml)
data/raw/      Raw landing zone (git-ignored)
sql/           Schema definitions
dashboard/     Plotly Dash application
tests/         Tests and recorded fixtures
notebooks/     Source profiling and analysis
docs/          Project documentation
workshop_1/    Workshop 1 deliverables
```

## Status

Workshop 1 in progress: source validation and data contract. Setup and run instructions will be added as the system is built.