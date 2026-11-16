# Source status log

Changes and anomalies observed in the project's data sources, as required by the team assignment ("each team must keep a short source-status log"). Report any change to the professor as soon as it is detected. Dates are in UTC.

| Date checked | Source | Observation | Action taken |
|---|---|---|---|
| 2026-11-07 | USGS real-time feed | In 9 hours, 25 events kept their ID but were revised (21 relocated, depth changes up to 49 km, 6 magnitude changes), although all were already `reviewed`. | Watermark on `updated`, content hash, and comparison of `geometry` as well as `properties`. |
| 2026-11-07 | PTWC (tsunami.gov) | CAP message `PHEB-1-26297050`, sent 2026-10-25 02:27, gives `EventOriginTime` 2026-10-24 02:09; the matching USGS earthquake `us6000u4r9` occurred on 2026-10-25 02:09:24. | Flagged as data-quality problem 6; source values preserved; link kept as a candidate for review. |
| 2026-11-09 | tsunami.gov | Feeds keep only their latest messages and send no `ETag`; PTWC published a new message on 2026-11-08 06:35 between our snapshots. | Poll every 5 minutes; limitation documented. |
| 2026-11-16 | NOAA HazEL | The API description (Swagger) does not state the units of `eqDepth` and `maxWaterHeight`; the HazEL search forms state "Focal Depth (km)" and "Maximum Water Height (m)". | Units documented in the report and `workshop_1/mapping.md`. |
| 2026-11-16 | NTWC (tsunami.gov) | CAP message `PAAQ-1-togrrv` has `expires` 2026-11-13 12:30:02, 11 hours before its `sent` time (23:30:02). | Flagged as data-quality problem 8; validation rule `expires >= sent`. |
| 2026-11-16 | USGS real-time feed | Six events left the M4.5+ feed while younger than 30 days; the FDSN catalog shows their magnitudes revised from 4.5–4.6 to 4.4. None of the 288 absences was a deletion. | FDSN lookup by event ID for early absences; responses saved in `workshop_1/samples/usgs_fdsn_absent_events_20261116.json`. |
| 2026-11-16 | Marine Regions (GOAS v1) | The dataset stores the region name as "South China and Easter Archipelagic Seas" (published as "Eastern"). The license asks users not to redistribute the files. | Literal name used in the filter; boundaries downloaded from the VLIZ WFS by `workshop_1/download_marine_regions.py` instead of being committed. |
