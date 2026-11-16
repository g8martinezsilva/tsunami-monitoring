# Workshop 1 canonical mapping

This Workshop 1 document specifies the canonical model; it does not implement the database or ingestion pipelines. Source evidence is in `workshop_1/samples/` and `notebooks/01_source_profiling.ipynb`.

## 1. Purpose and Sources

The model supports monitoring recent Pacific earthquakes using historical earthquake and tsunami records for context and official tsunami bulletins when available.

The analysis includes 221 historical tsunami records from NOAA HazEL (2010–2025), of which 205 fall within the project's defined Pacific monitoring scope, 7 fall outside it, and 9 lack valid coordinates. The historical USGS FDSN sample contains 343 earthquake records (2010–2011, M ≥ 6).

Incremental data comprises four USGS real-time snapshots and four Atom/CAP snapshots from each of the NTWC and PTWC warning centers. Historical and incremental sources cover different periods.

| `source.source_id` | Observation | External identifier | Evidence format |
|---|---|---|---|
| `noaa_hazel` | Historical tsunami record | HazEL `id` | Paginated JSON |
| `usgs_fdsn` | Catalog earthquake record | CSV `id` | CSV |
| `usgs_realtime` | Feed earthquake feature | GeoJSON feature `id` | GeoJSON |
| `ntwc` | NTWC bulletin | CAP `identifier` | Atom entry and linked CAP XML |
| `ptwc` | PTWC bulletin | CAP `identifier` | Atom entry and linked CAP XML |

Each record is associated with the source from which it was retrieved. For NTWC and PTWC, the source is identified by the Atom feed rather than the CAP sender field. Source IDs identify data providers, while external IDs identify individual records.

## 2. Canonical Model and Event Identity

The course defines a common conceptual data model consisting of `source`, `ingestion_run`, `ingestion_state`, `event`, `event_group`, and `rejected_record`. Our project extends this model with `earthquake`, `tsunami_event`, `tsunami_bulletin`, four NOAA descriptor tables, and `event_relation` to represent relationships between events.

**`event`** stores an observation obtained from a data source, such as an earthquake from USGS, a historical tsunami from NOAA, or an official bulletin from NTWC or PTWC. Each observation receives an internal `event_id`, while `external_id` preserves its original identifier. The combination `UNIQUE(source, external_id)` prevents duplicate observations from the same source. If a record is updated, its existing `event_id` is preserved. The `raw_reference` field identifies the original file and record, while content hashes can be used to detect changes between downloads.

**`event_group`** groups observations that have been verified to represent the same real-world event. For example, an earthquake reported by USGS FDSN and USGS real-time can belong to the same group. Both observations remain separate records, preserving their original information. Each group contains at least two verified observations of the same event type.

**`event_relation`** connects related events that represent different phenomena. For example, a historical tsunami may be linked to the earthquake that caused it (`caused_by`), while an official bulletin may be linked to the earthquake or tsunami it describes (`reports_on`). The model also supports possible matches between observations (`identity_candidate`), which are reviewed before being confirmed.

Relationships are classified as `pending`, `accepted`, or `rejected` according to the available evidence. Observations confirmed to represent the same event are grouped through `event_group`. Temporal and geographic proximity can help identify possible relationships, but they are not sufficient on their own to confirm them.

## 3. Canonical Field Mapping

`—` means no corresponding source field. These eight fields are required by the course; “nullable” describes the proposed canonical field, not whether every sample has a null value. `event.source` references `source.source_id`.

| Canonical field | Type / nullability | NOAA HazEL | USGS FDSN / real-time | NTWC / PTWC Atom + CAP | Transformation and meaning |
|---|---|---|---|---|---|
| `event_id` | Generated opaque ID, PK; required | `id` is external | CSV `id` / feature `id` are external | CAP `identifier` is external; Atom `entry.id` is separate | Generate independently of source IDs. Preserve the original ID as `external_id` and in `raw_reference`. |
| `source` | Controlled text, FK; required | `noaa_hazel` | `usgs_fdsn` / `usgs_realtime` | `ntwc` / `ptwc`, derived from the feed | Identify the observation's actual source, not a preferred source for a merged event. |
| `event_type` | `earthquake` / `tsunami` / `bulletin`; required | `tsunami` for HazEL tsunami records | `earthquake` for sampled earthquake records | `bulletin` | Preserve source `type` or CAP `event` separately. A bulletin is not evidence that a tsunami occurred. |
| `timestamp` | UTC timestamp; nullable | Date and clock components | FDSN `time` / `properties.time` (epoch ms) | CAP `sent` | USGS: earthquake origin. NOAA: event time only when seconds are present; otherwise null and preserve known date/hour/minute with `time_precision`. CAP: bulletin issue time. Never substitute midnight for a date-only record. |
| `latitude` | WGS84 decimal degrees; nullable | `latitude` | CSV `latitude` / `geometry.coordinates[1]` | CAP `EventLatLon` first value; Atom `geo:lat` fallback | Preserve nulls. CAP coordinates describe the reported earthquake point when supplied, not necessarily the impacted area. |
| `longitude` | WGS84 decimal degrees; nullable | `longitude` | CSV `longitude` / `geometry.coordinates[0]` | CAP `EventLatLon` second value; Atom `geo:long` fallback | Normalize the canonical value to −180…180; retain the raw value. GeoJSON order is longitude, latitude, depth. |
| `severity` | Text value; nullable | `maxWaterHeight` (meters); not `eqMagnitude` | `mag` / `properties.mag` | CAP `info.severity` category | Use `severity_scale` and `severity_unit` alongside the value. Earthquake magnitude, tsunami height, and CAP severity are different measurements and cannot be compared directly. |
| `raw_reference` | Structured JSON-like object; required for accepted records | JSON file + `items` ID/index | CSV file + row/ID; GeoJSON snapshot + feature ID | CAP file + identifier, with Atom file/entry reference | Include external ID, immutable saved artifact path, record locator, and original URL when available; add a content hash when computed. A filename alone is insufficient. |

Additional `event` fields support the mapping: `external_id` (text, required, unique with `source`), `event_date` (date, nullable), `known_hour` and `known_minute` (integers, nullable), `time_precision` (`day`, `hour`, `minute`, `second`, or `subsecond`; required when a time is known), `severity_scale` and `severity_unit` (text, nullable), `event_group_id` (nullable FK), and derived `in_pacific_ocean` / `in_pacific_scope` booleans. NOAA's `date_utc` and exact `time_utc` in the notebook illustrate why partial clocks need separate fields. `raw_reference` points to the latest accepted source representation, while saved artifacts retain earlier values.

For `severity`, use `severity_scale` to distinguish earthquake magnitude, tsunami water height, and CAP severity. Earthquake magnitude has no physical unit, NOAA `maxWaterHeight` is expressed in meters, and CAP severity is categorical. These values cannot be compared directly. NOAA `eqMagnitude` refers to the associated earthquake, not the tsunami itself.

## 4. Domain Tables

Each accepted `event` has exactly one matching domain detail row, determined by `event_type`; each domain row has `event_id` as both PK and FK. This exact-one-across-three requirement is a business rule.

| Table | Main fields and source mapping | Important distinction |
|---|---|---|
| `earthquake` | `magnitude` ← USGS `mag`; `magnitude_type` ← `magType`; `depth_km` ← FDSN `depth` or GeoJSON coordinates `[2]`; `place`, `status`, `net`; `source_updated_at` ← `updated`; nullable `tsunami_bulletin_flag` ← real-time `tsunami` (0/1). | The flag indicates bulletin-related information; it does not confirm a tsunami. `updated` is not origin time. |
| `tsunami_event` | NOAA `causeCode`, `eventValidity`, `regionCode`, `warningStatusId` as nullable descriptor FKs; `earthquakeEventId` as a NOAA-native ID; `eqMagnitude`, `eqDepth`, `maxWaterHeight`, `numRunups`, `oceanicTsunami`, `country`, `locationName`. | Causative-earthquake magnitude/depth are not tsunami severity. `numRunups` counts records, not height. NOAA-native earthquake ID is not a USGS FK. Unknown or non-seismic causes do not imply an earthquake relation. `warningStatusId` is missing in 169 of 221 saved records. |
| `tsunami_bulletin` | Center; CAP `identifier`, `sender`, `sent`, `msgType`, `info.event`, `severity`, `urgency`, `certainty`, `expires`, `EventPreliminaryMagnitude`, `EventPreliminaryMagnitudeType`, `EventDepth`, `EventOriginTime`, `EventLocationName`; Atom entry `id`, `updated`, title and CAP link. | CAP `sent` is issue time; Atom `updated` is a source update; `EventOriginTime` is a reported earthquake origin. Preserve the original text depth and parse `depth_km` only with a known unit. A new CAP ID need not revise the same event. |

### NOAA Descriptor Catalogs

NOAA provides descriptor catalogs to interpret coded fields in its historical tsunami records. Four reference tables are included in the model:

| Table | NOAA field | Purpose |
|---|---|---|
| `noaa_cause` | `causeCode` | Identifies the reported cause of the tsunami. |
| `noaa_event_validity` | `eventValidity` | Describes how confidently the event is classified as a tsunami. |
| `noaa_region` | `regionCode` | Identifies NOAA's geographic region. |
| `noaa_warning_status` | `warningStatusId` | Describes the historical warning classification. |

Each table contains an integer `code` as its primary key and a required text `description`. Codes from the descriptor files are converted to integers to match the corresponding fields in `tsunami_event`. Missing values remain `NULL`, while codes such as `0` and `-1` retain their original NOAA meanings.

All non-null descriptor codes found in the 221 saved NOAA records have corresponding definitions in the downloaded catalogs. The original descriptor JSON files preserve the complete code lists. Historical warning classifications are kept separate from the severity and message types used in NTWC/PTWC CAP bulletins.

## 5. Source and Ingestion Metadata

The course requires source and ingestion metadata alongside event observations.

| Table | Key fields | Cardinality and use |
|---|---|---|
| `source` | `source_id` PK; name, URL, format, license/attribution | One source has 0..many `event` observations and 0..many `ingestion_run` records. The five controlled source IDs are listed in Section 1. |
| `ingestion_run` | Generated `run_id` PK; `source_id` FK; mode (`historical`/`incremental`), start/end UTC, status, counts (read/accepted/updated/rejected), input artifact references | One run belongs to one source and can have 0..many rejected records. It records processing outcomes and input artifacts. |
| `ingestion_state` | `source_id` PK/FK; watermark value and meaning (nullable), `last_successful_run_id` nullable FK | At most one state per source. NOAA may have no incremental watermark. A watermark advances only after a successful run. |
| `rejected_record` | Generated `rejection_id` PK; `source_id` FK; `run_id` FK; external ID if available, reason, `raw_reference` | A rejected source row remains traceable without becoming an `event`. Source and run must agree. |

Accepted observations are stored in `event`, while `ingestion_run` keeps a record of each ingestion process and its original files. When a source record is updated, the existing observation is updated instead of duplicated. Previous downloads remain available in the saved raw files.

## 6. Relationships and Cardinalities

| Link | Cardinality | Rule |
|---|---|---|
| `source` → `event` | One source to 0..many events; each event has exactly one source. | `(source, external_id)` identifies a current observation. |
| `event_group` → `event` | One group to 2..many reviewed observations (business rule); an event belongs to 0..1 group. | Only same-phenomenon, same-type observations join a group. |
| `event` → domain detail | One event to exactly one row across the three domain tables. | The chosen table matches `event_type`. |
| `event` → `event_relation` | An event can be either endpoint in 0..many links; each link has exactly two endpoint events. | `identity_candidate` retains review evidence; confirmed same-phenomenon identity is expressed by `event_group` membership. `caused_by` and `reports_on` express reviewed cross-type links. |
| NOAA descriptor → `tsunami_event` | One code to 0..many tsunami rows; each tsunami has 0..1 code per descriptor. | Four independent nullable FKs; missing is null, not a made-up code. |
| `source` → `ingestion_run` / `ingestion_state` | 0..many runs and 0..1 state per source. | Each state may point to 0..1 last successful run for that same source. |
| `ingestion_run` → `rejected_record` | One run to 0..many rejected rows; each rejected row has exactly one run and source. | Retain the original locator and rejection reason. |

Verified observations of the same earthquake can be grouped through `event_group`. Relationships between earthquakes, tsunamis, and official bulletins are represented through `event_relation`.

The notebook identified 28 possible NOAA–USGS associations, including two ambiguous cases, and three exploratory CAP–USGS candidates, one with an approximately one-day time difference. These results are preliminary matches that require verification before being stored as confirmed relationships.

## 7. Normalization Rules

The following rules define how data from different sources will be standardized. Some transformations were already tested in the profiling notebook, while database validation and ingestion will be implemented in later workshops.

1. **Time:** Dates from USGS and CAP are converted to UTC, keeping earthquake origin, update, and bulletin publication times separate. NOAA records preserve their available date and time precision without inventing missing values. The ingestion process will also normalize CAP expiration and Atom update times. The notebook identified timestamp inconsistencies in NTWC and PTWC messages that require review.

2. **Coordinates and scope:** Coordinates use WGS84, with longitude standardized to −180…180. The notebook uses three maritime regions from Marine Regions and an additional 100 km monitoring margin. `in_pacific_ocean` identifies points inside the selected regions, while `in_pacific_scope` includes the additional margin. Missing or invalid coordinates are counted separately because their geographic scope cannot be determined. Spatial calculations account for the antimeridian.

3. **Measures:** Earthquake magnitude is stored with its magnitude type. USGS and NOAA earthquake depths are expressed in kilometers, while NOAA `maxWaterHeight` is expressed in meters. CAP depth values will be converted only when their units are clear. Earthquake magnitude, tsunami measurements, and bulletin severity represent different information and must not be compared directly. A warning or tsunami-related flag does not confirm that a tsunami occurred.

4. **IDs and revisions:** Original source identifiers are preserved as text. The notebook compares saved snapshots to identify new and modified records. During ingestion, `UNIQUE(source, external_id)` and content comparisons will prevent duplicates and detect updates. Original files will be preserved for traceability. A record missing from a later USGS snapshot is not necessarily deleted, and a new CAP identifier does not automatically indicate a revision of an earlier bulletin.

5. **Cross-source review:** The notebook identifies possible relationships using time, location, and magnitude. Historical NOAA–USGS comparisons use a ±15-minute window for exact timestamps, a maximum distance of 100 km, and a magnitude difference of up to 0.5 when applicable. Incomplete NOAA timestamps are treated according to their available precision. The wider PTWC comparison was only used to investigate a time discrepancy. Possible matches require verification before being accepted.

6. **Missing values and validation:** Missing information is preserved as `NULL` rather than replaced with invented values. The notebook identifies missing coordinates, incomplete dates, and inconsistent records. During ingestion, required identifiers, data types, and coordinate ranges will be validated. Records that cannot be processed will be stored in `rejected_record` with their rejection reason, while valid but incomplete records may still be accepted.

Source-specific watermarks, overlap windows, failure recovery, and idempotent ingestion will be detailed in the report's Ingestion Contract. The 100 km monitoring margin is a project-defined approximation, not an official maritime boundary.

The corresponding entity–relationship diagram is available in [docs/er_diagram.md](../docs/er_diagram.md).