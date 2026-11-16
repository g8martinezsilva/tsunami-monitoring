# Test fixtures

Recorded source responses, copied unchanged from `workshop_1/samples/`, so that tests never call the live services. Each file covers one case that the pipeline must handle.

| File | Source | Case to test |
|---|---|---|
| `noaa_tsunamis_pacific_page2.json` | NOAA HazEL | Last page of a paginated response (21 events); missing coordinates and time of day |
| `usgs_earthquakes_2010_2011_m6_pacific.csv` | USGS FDSN | Historical CSV parsing; `magError` always empty; `official` network IDs |
| `usgs_realtime_20261107_050434.geojson` | USGS real-time | Earlier snapshot for change detection |
| `usgs_realtime_20261107_140301.geojson` | USGS real-time | Later snapshot: 22 new, 25 modified (21 relocated) and 10 absent events |
| `usgs_fdsn_absent_events_20261116.json` | USGS FDSN | Catalog lookup of absent events: magnitudes revised from 4.5–4.6 to 4.4, none deleted |
| `ptwc_20261107_140301.xml`, `ptwc_cap1_20261107_140301.xml` | PTWC | CAP `EventOriginTime` one day earlier than the matching USGS earthquake (`us6000u4r9`) |
| `ntwc_20261116_013756.xml`, `ntwc_cap1_20261116_013756.xml` | NTWC | CAP `expires` 11 hours before `sent` (invalid validity interval) |

The expected results are documented in the Workshop 1 report (Results chapter) and in `notebooks/01_source_profiling.ipynb`.
