# Workshop 1 canonical mapping

This Workshop 1 document specifies the canonical model; it does not implement the database or ingestion pipelines. Source evidence is in `workshop_1/samples/` and `notebooks/01_source_profiling.ipynb`.

## 1. Purpose and Sources

The model supports monitoring recent Pacific earthquakes using historical earthquake and tsunami records for context and official tsunami bulletins when available. Its evidence comprises NOAA HazEL historical tsunamis (2010–2025), a USGS FDSN earthquake sample (2010–2011, M ≥ 6), four saved USGS real-time snapshots, and four NTWC and four PTWC Atom/CAP snapshots. The coverage periods differ. The NOAA sample has 221 records (a preliminary 205 in the project geographic scope; nine without coordinates), and the USGS historical sample has 343. These are profiling results, not database row counts.

| `source.source_id` | Observation | External identifier | Evidence format |
|---|---|---|---|
| `noaa_hazel` | Historical tsunami record | HazEL `id` | Paginated JSON |
| `usgs_fdsn` | Catalog earthquake record | CSV `id` | CSV |
| `usgs_realtime` | Feed earthquake feature | GeoJSON feature `id` | GeoJSON |
| `ntwc` | NTWC bulletin | CAP `identifier` | Atom entry and linked CAP XML |
| `ptwc` | PTWC bulletin | CAP `identifier` | Atom entry and linked CAP XML |

The feed or file identifies the source. CAP `sender` is preserved as source data but does not replace the feed identity: the saved PTWC CAP messages report `sender=ntwc@noaa.gov`. Source IDs above are controlled values; they are not the external record IDs.

## 2. Canonical Model and Event Identity

The course's common model has `source`, `ingestion_run`, `ingestion_state`, `event`, `event_group`, and `rejected_record`. This project adds `earthquake`, `tsunami_event`, `tsunami_bulletin`, four NOAA descriptor catalogs, and one `event_relation` table for reviewed or pending links.

**`event` is one normalized observation from one source**, identified internally by generated `event_id`. `UNIQUE(source, external_id)` identifies current observations, and `external_id` preserves the original source identifier as text. A revised record retains its canonical `event_id`. Its `raw_reference` identifies the saved file and record locator for the current values; a content hash, when available, supports change detection. Earlier snapshots remain in the raw/audit trail, with their artifact references associated with `ingestion_run`. The model does not include a separate `source_record` or queryable per-version table.

**`event_group` is a reviewed same-phenomenon grouping**, not a merged observation row. An event has zero or one group; a group is created for at least two observations verified to describe the same real-world phenomenon and has a single `event_type`. Mermaid can show one or more group members, but its cardinality does not fully express this two-member minimum; that minimum is a business rule. For example, an FDSN row and a real-time row may remain two `event` rows while sharing one earthquake group after review. Unverified matches remain separate.

Earthquake, tsunami, and bulletin are three different `event_type` values. A tsunami caused by an earthquake and a bulletin about that earthquake are separate observations and must not be placed in the earthquake's group. `event_relation` stores a typed link between two events: `identity_candidate` (a possible same-phenomenon match awaiting review), `caused_by` (tsunami → earthquake), or `reports_on` (bulletin → earthquake or tsunami). Each link has `pending`, `accepted`, or `rejected` review status and an evidence reference. When an identity candidate is accepted, the observations join the appropriate `event_group`; that membership is the sole source of truth for confirmed identity. The original relation may remain as review evidence, not a second independent identity claim. Accepted causal and informational links remain in `event_relation`. Time and distance alone never confirm identity. A non-seismic tsunami needs no earthquake relation.

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
| `severity` | Text value; nullable | `maxWaterHeight` when its unit is verified; **not** `eqMagnitude` | `mag` / `properties.mag` | CAP `info.severity` category | Use `severity_scale` and `severity_unit` alongside the value. Earthquake magnitude, tsunami height, and CAP category are not one numeric scale; numeric measurements stay in domain tables. |
| `raw_reference` | Structured JSON-like object; required for accepted records | JSON file + `items` ID/index | CSV file + row/ID; GeoJSON snapshot + feature ID | CAP file + identifier, with Atom file/entry reference | Include external ID, immutable saved artifact path, record locator, and original URL when available; add a content hash when computed. A filename alone is insufficient. |

Additional `event` fields support the mapping: `external_id` (text, required, unique with `source`), `event_date` (date, nullable), `known_hour` and `known_minute` (integers, nullable), `time_precision` (`day`, `hour`, `minute`, `second`, or `subsecond`; required when a time is known), `severity_scale` and `severity_unit` (text, nullable), `event_group_id` (nullable FK), and derived `in_pacific_ocean` / `in_pacific_scope` booleans. NOAA's `date_utc` and exact `time_utc` in the notebook illustrate why partial clocks need separate fields. `raw_reference` points to the latest accepted source representation, while saved artifacts retain earlier values.

For `severity`, use controlled `severity_scale` values such as `earthquake_magnitude`, `tsunami_max_water_height`, and `cap_severity`. Magnitude has no physical unit; its type (for example `mww`) remains in `earthquake.magnitude_type`. Tsunami height needs a verified length unit; CAP severity is categorical and has no unit. Do not compare or aggregate values across these scales. NOAA `eqMagnitude` belongs to the reported causative earthquake, not the tsunami height. Any derived analytical indicator is distinct from official CAP severity.

## 4. Domain Tables

Each accepted `event` has exactly one matching domain detail row, determined by `event_type`; each domain row has `event_id` as both PK and FK. This exact-one-across-three requirement is a business rule.

| Table | Main fields and source mapping | Important distinction |
|---|---|---|
| `earthquake` | `magnitude` ← USGS `mag`; `magnitude_type` ← `magType`; `depth_km` ← FDSN `depth` or GeoJSON coordinates `[2]`; `place`, `status`, `net`; `source_updated_at` ← `updated`; nullable `tsunami_bulletin_flag` ← real-time `tsunami` (0/1). | The flag indicates bulletin-related information; it does not confirm a tsunami. `updated` is not origin time. |
| `tsunami_event` | NOAA `causeCode`, `eventValidity`, `regionCode`, `warningStatusId` as nullable descriptor FKs; `earthquakeEventId` as a NOAA-native ID; `eqMagnitude`, `eqDepth`, `maxWaterHeight`, `numRunups`, `oceanicTsunami`, `country`, `locationName`. | Causative-earthquake magnitude/depth are not tsunami severity. `numRunups` counts records, not height. NOAA-native earthquake ID is not a USGS FK. Unknown or non-seismic causes do not imply an earthquake relation. `warningStatusId` is missing in 169 of 221 saved records. |
| `tsunami_bulletin` | Center; CAP `identifier`, `sender`, `sent`, `msgType`, `info.event`, `severity`, `urgency`, `certainty`, `expires`, `EventPreliminaryMagnitude`, `EventPreliminaryMagnitudeType`, `EventDepth`, `EventOriginTime`, `EventLocationName`; Atom entry `id`, `updated`, title and CAP link. | CAP `sent` is issue time; Atom `updated` is a source update; `EventOriginTime` is a reported earthquake origin. Preserve the original text depth and parse `depth_km` only with a known unit. A new CAP ID need not revise the same event. |

`tsunami_bulletin.cap_identifier` must equal its `event.external_id`; the Atom ID is separate. NOAA's reported `eqDepth` and `maxWaterHeight` retain their source values because their units have not been confirmed against source metadata. `tsunami_event` has no direct earthquake FK, and `tsunami_bulletin` has no direct earthquake/tsunami FK: reviewed causal and informational links use `event_relation`. The event-level model does not include a run-up observation table.

### NOAA descriptor catalogs

Each table has **`code` integer PK** and **`description` text required**. Descriptor JSON `items[].id` values are strings (including `"-1"`), whereas the event codes are integers; parse descriptor IDs to integers before joining. Each FK from `tsunami_event` is nullable. Missing codes are null, not synthetic “unknown” codes: NOAA's explicit `0` or `-1` categories keep their actual meanings.

| Table and source file | Code → description in the saved descriptor |
|---|---|
| `noaa_cause` — `noaa_descriptor_causes.json` | 0 Unknown; 1 Earthquake; 2 Questionable Earthquake; 3 Earthquake and Landslide; 4 Volcano and Earthquake; 5 Volcano, Earthquake and Landslide; 6 Volcano; 7 Volcano and Landslide; 8 Landslide; 9 Meteorological; 10 Explosion; 11 Astronomical Tide. |
| `noaa_event_validity` — `noaa_descriptor_validity.json` | −1 Erroneous entry; 0 Event that only caused a seiche; 1 Very Doubtful Tsunami; 2 Questionable Tsunami; 3 Probable Tsunami; 4 Definite Tsunami. |
| `noaa_region` — `noaa_descriptor_regions.json` | 30 Red Sea and Persian Gulf; 40 Black Sea and Caspian Sea; 50 Mediterranean Sea; 60 Indian Ocean (including W. Australia and W. Indonesia); 70 Southeast Atlantic Ocean; 71 Southwest Atlantic Ocean; 72 Northwest Atlantic Ocean; 73 Northeast Atlantic Ocean; 74 Caribbean Sea; 75 East Coast of United States and Canada; 76 Gulf of America/Mexico; 77 West Coast of Africa; 78 Central Africa; 80 Hawaii, Johnston Atoll, Midway I; 81 E. Australia, New Zealand, South Pacific Is.; 82 New Caledonia, New Guinea, Solomon Is., Vanuatu; 83 E. Indonesia and Malaysia; 84 China, North and South Korea, Philippines, Taiwan; 85 Japan; 86 Kamchatka and Kuril Islands; 87 Alaska (including Aleutian Islands); 88 West Coast of North and Central America; 89 West Coast of South America. |
| `noaa_warning_status` — `noaa_descriptor_warning_statuses.json` | −1 status unknown; 0 no warning issued; 1 PTW - Pacific-wide Tsunami Warning issued by PTWC; 2 RTW - Regional Tsunami Warning issued by PTWC for areas having no TWS; 3 LTW - Local Tsunami Warning issued by regional or national TWC; 4 TIB - Tsunami Information or Attention Bulletin issued by any agency. |

The saved 221 tsunami events use only a subset of each descriptor table: all their non-null `causeCode`, `eventValidity`, `regionCode`, and `warningStatusId` values occur in the matching descriptors. The four tables preserve NOAA's definitions without conflating historical `warningStatusId` with CAP `info.severity` or `msgType`.

## 5. Source and Ingestion Metadata

The course requires source and ingestion metadata alongside event observations.

| Table | Key fields | Cardinality and use |
|---|---|---|
| `source` | `source_id` PK; name, URL, format, license/attribution | One source has 0..many `event` observations and 0..many `ingestion_run` records. The five controlled source IDs are listed in Section 1. |
| `ingestion_run` | Generated `run_id` PK; `source_id` FK; mode (`historical`/`incremental`), start/end UTC, status, counts (read/accepted/updated/rejected), input artifact references | One run belongs to one source and can have 0..many rejected records. It records processing outcomes and input artifacts. |
| `ingestion_state` | `source_id` PK/FK; watermark value and meaning (nullable), `last_successful_run_id` nullable FK | At most one state per source. NOAA may have no incremental watermark. A watermark advances only after a successful run. |
| `rejected_record` | Generated `rejection_id` PK; `source_id` FK; `run_id` FK; external ID if available, reason, `raw_reference` | A rejected source row remains traceable without becoming an `event`. Source and run must agree. |

An accepted `event` stores the current normalized source observation. Raw artifact references in `ingestion_run` and `event.raw_reference` make earlier snapshots traceable without duplicating canonical rows. This design preserves raw versions but does not provide a queryable per-version history.

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

Two verified USGS observations can share an earthquake group. A historical NOAA tsunami can be linked to a reviewed earthquake by `caused_by`; several bulletins can point to one earthquake with `reports_on`. A landslide tsunami needs no earthquake link. The notebook found 28 NOAA–USGS candidate links (two ambiguous), two CAP–USGS candidates inside the normal exploratory window, and one PTWC diagnostic candidate with an approximately one-day time discrepancy. These are candidate counts, not confirmed relations or automatic `event_group` membership.

## 7. Normalization Rules

1. **Time:** Parse USGS origin and update times, CAP `sent`/`expires`/reported origin, and Atom `updated` into UTC as distinct fields. USGS real-time timestamps are Unix milliseconds. NOAA has day, hour, minute, and second precision; preserve `event_date`, known clock components, and `time_precision`. A date-only NOAA event has null `timestamp`. The PTWC origin discrepancy requires review, not silent correction. One saved NTWC CAP bulletin has `expires` 11 hours before `sent`; preserve and flag both values.
2. **Coordinates and scope:** Store WGS84/EPSG:4326 source coordinates and normalize canonical longitude to −180…180 without changing the raw value. Section 4 of the notebook loads `config/geographic_reference/pacific_maritime_regions.geojson`, containing three selected regions from [Marine Regions](https://www.marineregions.org/sources.php): `North Pacific Ocean`, `South Pacific Ocean`, and the source's literal `South China and Easter Archipelagic Seas` (published name: **South China and Eastern Archipelagic Seas**). `in_pacific_ocean` marks points inside these official maritime polygons; `in_pacific_scope` also includes points within the project's approximate 100 km geodesic influence area. This margin is not an official boundary or solely a coastline buffer. Spatial tests handle the antimeridian separately. Invalid/missing coordinates are outside scope and counted separately; the notebook's `in_pacific` is an alias, not a third rule.
3. **Measures:** Keep magnitude with its type, USGS depth in km, and NOAA `eqDepth`/`maxWaterHeight` in their source values because their units remain unverified. Preserve CAP depth text and parse kilometers only when the unit is explicit. Keep `numRunups` as a count. Never treat USGS `tsunami=1`, NOAA `warningStatusId`, or CAP severity as interchangeable or as proof of tsunami occurrence.
4. **IDs and revisions:** Preserve source IDs as strings and use `UNIQUE(source, external_id)` for current observations. Compare content hashes for the same source ID to detect changes without treating a later snapshot as a new natural event; retain previous hashes and snapshots in the raw/audit trail. A missing USGS feed feature is not proof of deletion. Keep the original file and record locator for each accepted or rejected source row. A changed CAP identifier is a new bulletin observation unless a same-incident review establishes a link.
5. **Cross-source review:** NOAA partial times are intervals, not invented exact timestamps. The notebook's exploratory match uses ±15 minutes for exact NOAA times, at most 100 km between candidate epicenters, and a magnitude difference of at most 0.5 where required. These 100 km are an association tolerance independent of the monitoring margin. A diagnostic two-day search for the PTWC discrepancy is not a routine matching rule. Keep ambiguous links as candidates until reviewed.

Source-specific watermarks, overlap windows, failure recovery, and idempotent writes belong to the report's Ingestion Contract. The geographic counts remain preliminary because the project's 100 km boundary approximation has not been independently validated.

The corresponding entity–relationship diagram is available in [docs/er_diagram.md](../docs/er_diagram.md).
