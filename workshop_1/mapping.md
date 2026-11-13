# Draft data model and canonical mapping

**Status:** Workshop 1 discussion draft. This document defines entities and mapping rules; it does not define a physical database or an ingestion pipeline.

## 1. Purpose and evidence

The model covers historical Pacific tsunami events from NOAA HazEL (2010–2025), a Pacific USGS FDSN earthquake sample (2010–2011, M ≥ 6), saved USGS real-time snapshots (M ≥ 4.5), and saved NTWC/PTWC Atom and CAP messages. The project polygon is `config/pacific_polygon.geojson`. The live feeds have a different time window from the historical samples; the model does not imply that all four sources cover 2010–2025.

The current download scripts, the saved samples, `notebooks/01_source_profiling.ipynb`, and `workshop_1/report/main.tex` were reviewed locally. The saved NOAA event files contain 221 records over two pages (200 + 21); the USGS CSV contains 343 rows. The reference USGS real-time snapshot from 7 November contains 726 features; the later saved snapshot from 12 November contains 649. Each saved NTWC/PTWC Atom file contains one entry and has a corresponding CAP file. No separate Workshop 1 guide was found in the repository.

## 2. Proposed entities and identity

| Table | Meaning | Identity |
|---|---|---|
| `event` | One internally identified earthquake, historical tsunami, or bulletin. Holds the eight canonical fields and their interpretation. | Generated `event_id`; never a source ID. |
| `source_record` | A traceable observation of an event in a saved file. Allows multiple snapshots and multiple sources to support one `event`. | Generated `source_record_id`; unique `(source, original_id, raw_reference)`. |
| `earthquake` | Earthquake measurements from USGS. | `event_id` is both PK and FK to `event`. |
| `tsunami_event` | Historical tsunami facts from NOAA HazEL. | `event_id` is both PK and FK to `event`. |
| `tsunami_bulletin` | One official CAP message and its associated Atom entry. | `event_id` is both PK and FK to `event`. |
| `noaa_cause`, `noaa_event_validity`, `noaa_region`, `noaa_warning_status` | NOAA descriptor labels, kept separate from CAP classifications. | The integer NOAA descriptor code is each table's PK. |

Each `event` has exactly one of the three domain rows. Every `source_record` belongs to exactly one `event`; an event has one or more source records. Source observations remain distinct from the event they describe. For example, FDSN and real-time records for the same verified earthquake may point to one earthquake event, while a NOAA tsunami, its causative earthquake, and a related bulletin remain **three different events** connected through explicit relationships.

`event.source` names the source of the selected canonical representation, such as `usgs_fdsn` for an earthquake first represented by its historical catalog row. It is not a list of all contributing sources; `source_record.source` carries each observation's actual source. The selected `event.raw_reference` points to the source row used for its current canonical values. The source records retain other snapshots and their references. These two canonical fields are derived provenance metadata, not fields emitted by the APIs.

## 3. Canonical field mapping

`—` means there is no equivalent source field. `timestamp` is the time of the canonical **event type**: earthquake origin, tsunami occurrence, or bulletin issue. All nullable fields remain null when the source lacks them.

| Canonical field | NOAA HazEL | USGS FDSN CSV | USGS real-time GeoJSON | NTWC/PTWC Atom + CAP | Transformation | Limitations |
|---|---|---|---|---|---|---|
| `event_id` | `id` is an external tsunami ID | `id` is an external earthquake ID | `feature.id` is an external earthquake ID | CAP `identifier` is an external message ID; Atom `entry.id` is separate | Generate an internal `event_id`; store every external ID in `source_record.original_id` and the Atom ID in `tsunami_bulletin.atom_entry_id`. | Never join unrelated sources merely because their ID strings match. NOAA `earthquakeEventId` is a NOAA ID, not a USGS ID. |
| `source` | Derived `noaa_hazel` | Derived `usgs_fdsn` | Derived `usgs_realtime` | Derived `ntwc_cap` or `ptwc_cap` from the feed used | Record the selected source on `event`; record every observation's source on `source_record`. | CAP `sender` identifies the publisher but is not a stable substitute for the feed source; one sampled PTWC CAP has `sender=ntwc@noaa.gov`. |
| `event_type` | Derived `tsunami` from the events endpoint | CSV `type` (sample: `earthquake`) | `properties.type` (sample: `earthquake`) | CAP `info.event` describes the message topic; derived type `bulletin` | Use the controlled types `earthquake`, `tsunami`, `bulletin`. Preserve source wording in the domain row or raw source. | CAP `msgType=Alert` does not prove an active tsunami warning or an observed tsunami. |
| `timestamp` | `year`, `month`, `day`, `hour`, `minute`, `second` | `time` | `properties.time` in Unix milliseconds | CAP `sent` for the bulletin; Atom `entry.updated` is revision time | Parse to UTC. Assemble NOAA components only to the available precision. Keep `event_date` and `time_precision`; keep `timestamp` null for a date-only NOAA event. | CAP `EventOriginTime` is a *separate*, preliminary earthquake-origin field; USGS `updated`, real-time `properties.updated`, and Atom `entry.updated` are update times. |
| `latitude` | `latitude` | `latitude` | `geometry.coordinates[1]` | CAP `EventLatLon` first number; Atom `geo:lat` if CAP lacks it | Parse to WGS84 decimal degrees; null when absent. | NOAA has 9 events with missing coordinates; a bulletin's coordinates describe the reported earthquake location. |
| `longitude` | `longitude` | `longitude` | `geometry.coordinates[0]` | CAP `EventLatLon` second number; Atom `geo:long` if CAP lacks it | Parse to WGS84 decimal degrees; normalize to −180…180. | GeoJSON order is longitude, latitude, depth. The Pacific polygon uses a separate shifted longitude only for its point-in-polygon test. |
| `severity` | `maxWaterHeight` for a tsunami, if present; `eqMagnitude` is *causative earthquake* information | `mag` with `magType` | `properties.mag` with `properties.magType` | CAP `info.severity` (categorical); preliminary magnitude is separate | Store the canonical value as text with `severity_scale` and optional `severity_unit`; keep numeric measurements in domain columns. Examples: `tsunami_max_water_height`/`m`, `earthquake_magnitude`/magnitude type, `cap_severity`/CAP category. | The three scales are not directly comparable. Verify the NOAA water-height unit against its metadata before relying on `m`; NOAA has no magnitude type for `eqMagnitude`. |
| `raw_reference` | Saved JSON path plus `items` index or NOAA ID | Saved CSV path plus row or ID | Saved GeoJSON snapshot path plus feature ID | Saved CAP path plus CAP identifier; retain Atom path/entry ID | Derive a local path and record locator, e.g. `workshop_1/samples/…#id=…`; retain it on `event` and `source_record`. | A file name alone does not locate one row. Later revisions in other snapshots receive separate source-record references. |

`severity` is a display/mapping field rather than a common numeric measure. Its typed values must not be aggregated across event types. For analyses comparing earthquakes, use `earthquake.magnitude` and its `magnitude_type`. Keep tsunami height and CAP severity in their own columns.

## 4. Data dictionary and keys

`PK` = primary key; `FK` = foreign key. “Nullable” is a proposed rule, not a claim that every current sample has a null there. Derived fields are identified below.

### `event`

| Field | Proposed type / nullability | Origin and purpose |
|---|---|---|
| `event_id` | Generated identifier, PK | Internal identity independent of source IDs. |
| `source` | Text, required | Derived selected source (`noaa_hazel`, `usgs_fdsn`, `usgs_realtime`, `ntwc_cap`, `ptwc_cap`). |
| `event_type` | Text, required | Derived controlled type: `earthquake`, `tsunami`, or `bulletin`. |
| `timestamp` | UTC timestamp, nullable | Type-specific time; null if no usable time of day. |
| `event_date` | Date, nullable | Preserves a NOAA date when precise `timestamp` is unavailable. |
| `time_precision` | Text, required | Derived `date`, `minute`, `second`, or `subsecond`, as supported by the source. |
| `latitude`, `longitude` | Decimal degrees, nullable | WGS84; longitude normalized to −180…180. |
| `severity` | Text, nullable | Typed display value; its numeric counterpart remains in the domain table. |
| `severity_scale` | Text, nullable | `earthquake_magnitude`, `tsunami_max_water_height`, or `cap_severity`. |
| `severity_unit` | Text, nullable | Magnitude type (e.g. `mww`), `m` if verified for water height, or null for CAP category. |
| `raw_reference` | Text, required | Derived locator of the selected original row/message in a saved file. |

The `event_type` controls which domain table is present. A possible database implementation would enforce exactly one matching domain row, but this draft does not prescribe a migration or trigger.

### `source_record`

| Field | Proposed type / nullability | Origin and purpose |
|---|---|---|
| `source_record_id` | Generated identifier, PK | Identity of this saved observation. |
| `event_id` | FK → `event.event_id`, required | The event supported by the observation. |
| `source` | Text, required | Actual source family/feed, independent of `event.source`. |
| `original_id` | Text, required | NOAA `id`, USGS `id`/`feature.id`, or CAP `identifier`, converted to text without changing its content. |
| `raw_reference` | Text, required | Saved file path plus record locator. |

Proposed uniqueness: `(source, original_id, raw_reference)` to avoid ingesting the same row twice while permitting another saved snapshot of the same source ID. Comparison of values across snapshots is a later ingestion decision; this table only defines traceability. For an Atom/CAP message, the CAP identifier is `original_id`; the Atom entry ID and Atom file are retained in `tsunami_bulletin` and the raw files.

### `earthquake`

| Field | Proposed type / nullability | Origin and purpose |
|---|---|---|
| `event_id` | PK, FK → `event.event_id` | Exactly one earthquake detail row for this event. |
| `magnitude` | Decimal, nullable | FDSN `mag` or real-time `properties.mag`. |
| `magnitude_type` | Text, nullable | FDSN `magType` or real-time `properties.magType`. |
| `depth_km` | Decimal, nullable | FDSN `depth` or real-time `geometry.coordinates[2]`. |
| `place` | Text, nullable | FDSN `place` or real-time `properties.place`. |
| `status` | Text, nullable | FDSN `status` or real-time `properties.status`. |
| `network` | Text, nullable | FDSN `net` or real-time `properties.net`. |
| `source_updated_at` | UTC timestamp, nullable | FDSN `updated` or real-time `properties.updated`; not the origin time. |
| `tsunami_bulletin_flag` | Boolean, nullable | Real-time `properties.tsunami` (0/1); bulletin indicator only, not proof of a tsunami. |

### `tsunami_event`

| Field | Proposed type / nullability | Origin and purpose |
|---|---|---|
| `event_id` | PK, FK → `event.event_id` | Exactly one historical tsunami detail row for this event. |
| `cause_code` | Integer, nullable FK → `noaa_cause.code` | NOAA `causeCode`. |
| `event_validity` | Integer, nullable FK → `noaa_event_validity.code` | NOAA `eventValidity`; includes negative code −1. |
| `region_code` | Integer, nullable FK → `noaa_region.code` | NOAA `regionCode`. |
| `warning_status_id` | Integer, nullable FK → `noaa_warning_status.code` | NOAA `warningStatusId`, absent in 169 of 221 saved events. |
| `noaa_earthquake_event_id` | Integer, nullable; **not** a USGS FK | NOAA `earthquakeEventId`; a source-native link whose referenced NOAA earthquake record is not in these tsunami files. |
| `causative_earthquake_event_id` | Nullable FK → `earthquake.event_id` | Only set after a reviewed cross-source association. A non-earthquake tsunami leaves it null. |
| `reported_eq_magnitude` | Decimal, nullable | NOAA `eqMagnitude`; causative earthquake measurement, not the tsunami's severity. |
| `reported_eq_depth` | Decimal, nullable | NOAA `eqDepth`; likely km, unit still needs external confirmation. |
| `max_water_height` | Decimal, nullable | NOAA `maxWaterHeight`; expected meters, verify source unit. |
| `num_runups` | Integer, nullable | NOAA `numRunups`; not the downloaded runup observation table. |
| `oceanic_tsunami` | Boolean, nullable | NOAA `oceanicTsunami`. |
| `country`, `location_name` | Text, nullable | NOAA `country`, `locationName`. |

The sample also contains one page of runup observations, but this draft does not add a runup table: the requested event-level model only needs `num_runups` and `max_water_height`. A future observation-level requirement can justify that table.

### `tsunami_bulletin`

| Field | Proposed type / nullability | Origin and purpose |
|---|---|---|
| `event_id` | PK, FK → `event.event_id` | Exactly one bulletin detail row for this event. |
| `center` | Text, required | Derived from the saved NTWC or PTWC feed. |
| `atom_entry_id` | Text, nullable | Atom `entry.id`; distinct from CAP `identifier`. |
| `atom_reference` | Text, nullable | Saved Atom file path and entry locator. |
| `cap_identifier` | Text, required | CAP `identifier`; also the `source_record.original_id`. |
| `sender` | Text, nullable | CAP `sender`; keep source value even if it differs from the feed center. |
| `sent_at` | UTC timestamp, nullable | CAP `sent`; canonical bulletin issue time when available. |
| `atom_updated_at` | UTC timestamp, nullable | Atom `entry.updated`; not earthquake origin. |
| `msg_type` | Text, nullable | CAP `msgType` (e.g. `Alert`); not a tsunami outcome. |
| `cap_event`, `cap_severity`, `urgency`, `certainty` | Text, nullable | CAP `info.event`, `info.severity`, `info.urgency`, `info.certainty`. |
| `title` | Text, nullable | Atom `entry.title`. |
| `cap_link` | Text, nullable | Atom `entry.link` URL to the corresponding CAP message. |
| `reported_origin_at` | UTC timestamp, nullable | CAP `EventOriginTime`; preliminary and potentially erroneous. |
| `preliminary_magnitude`, `preliminary_magnitude_type` | Decimal / text, nullable | CAP `EventPreliminaryMagnitude`, `EventPreliminaryMagnitudeType`. |
| `reported_depth_km` | Decimal, nullable | Parse CAP `EventDepth` only when the unit is explicitly kilometers. |
| `earthquake_event_id` | Nullable FK → `earthquake.event_id` | Set only for a reviewed bulletin–earthquake association. |

The CAP `EventLatLon` supplies canonical bulletin coordinates when present; Atom `geo:lat`/`geo:long` is the fallback. Retain both original forms in their saved files. There is no direct bulletin-to-tsunami FK yet: a bulletin may be an information statement about an earthquake without an observed tsunami.

### NOAA reference tables

Each table has **`code` integer PK** and **`description` text required**. Descriptor JSON `items[].id` values are strings (including `"-1"`), whereas the event codes are integers; parse descriptor IDs to integers before joining. Each FK from `tsunami_event` is nullable. Missing codes are null, not synthetic “unknown” codes: NOAA's explicit `0` or `-1` categories keep their actual meanings.

| Table and source file | Code → description in the saved descriptor |
|---|---|
| `noaa_cause` — `noaa_descriptor_causes.json` | 0 Unknown; 1 Earthquake; 2 Questionable Earthquake; 3 Earthquake and Landslide; 4 Volcano and Earthquake; 5 Volcano, Earthquake and Landslide; 6 Volcano; 7 Volcano and Landslide; 8 Landslide; 9 Meteorological; 10 Explosion; 11 Astronomical Tide. |
| `noaa_event_validity` — `noaa_descriptor_validity.json` | −1 Erroneous entry; 0 Event that only caused a seiche; 1 Very Doubtful Tsunami; 2 Questionable Tsunami; 3 Probable Tsunami; 4 Definite Tsunami. |
| `noaa_region` — `noaa_descriptor_regions.json` | 30 Red Sea and Persian Gulf; 40 Black Sea and Caspian Sea; 50 Mediterranean Sea; 60 Indian Ocean (including W. Australia and W. Indonesia); 70 Southeast Atlantic Ocean; 71 Southwest Atlantic Ocean; 72 Northwest Atlantic Ocean; 73 Northeast Atlantic Ocean; 74 Caribbean Sea; 75 East Coast of United States and Canada; 76 Gulf of America/Mexico; 77 West Coast of Africa; 78 Central Africa; 80 Hawaii, Johnston Atoll, Midway I; 81 E. Australia, New Zealand, South Pacific Is.; 82 New Caledonia, New Guinea, Solomon Is., Vanuatu; 83 E. Indonesia and Malaysia; 84 China, North and South Korea, Philippines, Taiwan; 85 Japan; 86 Kamchatka and Kuril Islands; 87 Alaska (including Aleutian Islands); 88 West Coast of North and Central America; 89 West Coast of South America. |
| `noaa_warning_status` — `noaa_descriptor_warning_statuses.json` | −1 status unknown; 0 no warning issued; 1 PTW - Pacific-wide Tsunami Warning issued by PTWC; 2 RTW - Regional Tsunami Warning issued by PTWC for areas having no TWS; 3 LTW - Local Tsunami Warning issued by regional or national TWC; 4 TIB - Tsunami Information or Attention Bulletin issued by any agency. |

The saved 221 tsunami events use only a subset of each descriptor table: all their non-null `causeCode`, `eventValidity`, `regionCode`, and `warningStatusId` values occur in the matching descriptors. The four tables preserve NOAA's definitions without conflating historical `warningStatusId` with CAP `info.severity` or `msgType`.

## 5. Relationships and cardinality

| Relationship | Cardinality and optionality | Rule |
|---|---|---|
| `event` → `source_record` | One event has **1..many** source records; each source record has **exactly 1** event. | Different snapshots can retain separate original references. |
| `event` → each domain table | An event has **exactly 1** row in its matching domain table and **0** in the other two; each domain row has **exactly 1** event. | `event_type` determines the matching table. |
| `earthquake` → `tsunami_event` | One earthquake can cause **0..many** historical tsunami events; a tsunami has **0..1** linked earthquake. | Non-earthquake causes and unverified matches remain unlinked. |
| `earthquake` → `tsunami_bulletin` | One earthquake can have **0..many** bulletins; a bulletin has **0..1** linked earthquake. | A bulletin is a separate message, regardless of its CAP `msgType`. |
| Each NOAA descriptor → `tsunami_event` | One code can label **0..many** tsunami events; each tsunami has **0..1** code from each descriptor. | Four independent nullable FKs. |

## 6. Preliminary normalization rules

1. **UTC and precision:** Parse FDSN `time`/`updated`, Atom `updated`, CAP `sent`/`EventOriginTime`, and USGS real-time Unix milliseconds into UTC. Assemble NOAA year/month/day into `event_date`. If NOAA hour and minute exist, form a UTC timestamp and record `minute` precision when seconds are absent; if hour or minute is absent, leave `timestamp` null and keep `event_date` with `date` precision. Nine of the 221 NOAA events have hour and minute but no second, and 23 have no hour or minute. Do not substitute midnight for an unknown time. Preserve CAP origin separately from CAP issue time; the saved PTWC message `PHEB-1-26297050` gives an origin one day earlier than the likely matching USGS event `us6000u4r9`.
2. **Coordinates and geographic scope:** Store WGS84 latitude/longitude in decimal degrees and normalize longitude with `((lon + 180) % 360) - 180`. Keep missing coordinates null. The polygon uses transformed positive longitudes (`lon - 360`) only during the point-in-polygon test; do not store those polygon-axis values as canonical longitude. Apply the polygon to classify geographic scope, keeping raw records available for audit. NOAA region codes and the FDSN query rectangle are prefilters, not substitutes for the polygon. Nine NOAA sample events lack coordinates.
3. **Magnitude, depth, and height:** Keep magnitude as a number and its source magnitude type as text. Store USGS depth in km. Parse CAP `EventDepth` as km only when the text explicitly says `kilometers`; preserve the original text in the raw file. Treat NOAA `eqDepth` as a reported value with unit pending confirmation. Keep NOAA `maxWaterHeight` distinct from earthquake depth and magnitude; its meter unit also needs source-metadata confirmation.
4. **Severity and official messages:** Type every `event.severity` using `severity_scale` and `severity_unit`. Never aggregate earthquake magnitudes, tsunami heights, and CAP severity categories as one numeric series. CAP `severity`, `urgency`, `certainty`, and NOAA historical warning codes are distinct concepts. An internal analytical indicator, if later designed, must be stored separately from an official bulletin and must not be presented as an official warning.
5. **IDs and duplicate control:** Preserve source IDs exactly as strings in `source_record`. Repeated `(source, original_id, raw_reference)` is one observation, while the same ID in a later snapshot is another observation of a potentially updated event. Reconcile FDSN and real-time rows as one earthquake only after confirming source identity and event details. A NOAA tsunami and a USGS earthquake retain distinct `event_id` values; a time-and-distance match is only a candidate until reviewed. Preserve NOAA `earthquakeEventId` without treating it as a USGS FK.

## 7. Decisions and open checks

**Supported by the saved data:** the eight canonical fields can be populated or explicitly left null; the four NOAA descriptor IDs can be converted from string to integer; FDSN and real-time GeoJSON expose different representations of USGS earthquakes; Atom and CAP expose distinct message IDs and times; all four NOAA code relationships admit missing values. The model keeps the bulletin independent of tsunami occurrence.

**Requires validation before implementation:**

- Confirm NOAA `eqDepth` and `maxWaterHeight` units in source metadata. Until then, keep the reported values and avoid irreversible unit assumptions.
- Decide the review rule for linking FDSN, real-time, NOAA tsunami, and CAP candidates. The notebook's 15-minute/100-km rule is exploratory; the PTWC CAP date error shows why automatic linking is unsafe.
- Check behavior when a source ID is corrected, a CAP message is revised, or more than one earthquake may have contributed to a tsunami. A many-to-many association table is justified only if real cases require it.
- Confirm whether the selected canonical values should follow FDSN, the latest real-time revision, or a documented precedence rule when both describe one earthquake.
- Reconcile the historical script's current `itemsPerPage=500` parameter with cached NOAA files containing 200 + 21 events over two pages. The draft uses the saved evidence; a forced future download may have a different pagination layout.
- Verify the geographical polygon's boundary policy and handling of missing coordinates. In the saved evidence, it excludes 7 geolocated NOAA events and 18 FDSN events; 9 NOAA events cannot be located.

The accompanying diagram is in `docs/er_diagram.md`.
