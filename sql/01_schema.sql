-- Canonical model of the tsunami monitoring system (Workshop 1 ER diagram: docs/er_diagram.md,
-- field rules: workshop_1/mapping.md). Safe to run more than once.

CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------------
-- Sources and ingestion metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS source (
    source_id   TEXT PRIMARY KEY,              -- noaa_hazel, usgs_fdsn, usgs_realtime, ntwc, ptwc
    name        TEXT NOT NULL,
    url         TEXT NOT NULL,
    format      TEXT NOT NULL,
    license     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_run (
    run_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id         TEXT NOT NULL REFERENCES source (source_id),
    mode              TEXT NOT NULL CHECK (mode IN ('historical', 'incremental')),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    status            TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    read_count        INTEGER NOT NULL DEFAULT 0,
    accepted_count    INTEGER NOT NULL DEFAULT 0,
    updated_count     INTEGER NOT NULL DEFAULT 0,
    rejected_count    INTEGER NOT NULL DEFAULT 0,
    input_references  JSONB NOT NULL DEFAULT '[]'   -- Bronze files read by the run
);

CREATE TABLE IF NOT EXISTS ingestion_state (
    source_id               TEXT PRIMARY KEY REFERENCES source (source_id),
    watermark               TEXT,                  -- meaning depends on the source (config/sources.yaml)
    last_successful_run_id  BIGINT REFERENCES ingestion_run (run_id)
);

CREATE TABLE IF NOT EXISTS rejected_record (
    rejection_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id      TEXT NOT NULL REFERENCES source (source_id),
    run_id         BIGINT NOT NULL REFERENCES ingestion_run (run_id),
    external_id    TEXT,
    reason         TEXT NOT NULL,
    raw_reference  JSONB NOT NULL,                  -- Bronze file and record locator
    rejected_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Events: one row per source observation
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS event_group (
    event_group_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type          TEXT NOT NULL CHECK (event_type IN ('earthquake', 'tsunami', 'bulletin')),
    evidence_reference  TEXT
);

CREATE TABLE IF NOT EXISTS event (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source            TEXT NOT NULL REFERENCES source (source_id),
    external_id       TEXT NOT NULL,                -- original source identifier
    event_group_id    BIGINT REFERENCES event_group (event_group_id),
    event_type        TEXT NOT NULL CHECK (event_type IN ('earthquake', 'tsunami', 'bulletin')),
    "timestamp"       TIMESTAMPTZ,                  -- UTC; NULL when the source time is incomplete
    event_date        DATE,
    known_hour        SMALLINT CHECK (known_hour BETWEEN 0 AND 23),
    known_minute      SMALLINT CHECK (known_minute BETWEEN 0 AND 59),
    time_precision    TEXT CHECK (time_precision IN ('day', 'hour', 'minute', 'second', 'subsecond')),
    latitude          DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
    longitude         DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    severity          TEXT,
    severity_scale    TEXT CHECK (severity_scale IN ('earthquake_magnitude', 'tsunami_max_water_height', 'cap_severity')),
    severity_unit     TEXT,
    raw_reference     JSONB NOT NULL,               -- saved file, record locator, URL
    content_hash      TEXT,                         -- detects real changes between versions
    in_pacific_ocean  BOOLEAN,
    in_pacific_scope  BOOLEAN,
    geom              GEOMETRY(Point, 4326) GENERATED ALWAYS AS (
                          CASE WHEN longitude IS NOT NULL AND latitude IS NOT NULL
                               THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) END
                      ) STORED,
    inserted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, external_id),
    UNIQUE (event_id, event_type)                   -- lets each domain table enforce its event type
);

CREATE INDEX IF NOT EXISTS event_timestamp_idx ON event ("timestamp");
CREATE INDEX IF NOT EXISTS event_type_idx ON event (event_type);
CREATE INDEX IF NOT EXISTS event_geom_idx ON event USING GIST (geom);

CREATE TABLE IF NOT EXISTS event_relation (
    relation_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    from_event_id       BIGINT NOT NULL REFERENCES event (event_id),
    to_event_id         BIGINT NOT NULL REFERENCES event (event_id),
    relation_type       TEXT NOT NULL CHECK (relation_type IN ('identity_candidate', 'caused_by', 'reports_on')),
    review_status       TEXT NOT NULL DEFAULT 'pending' CHECK (review_status IN ('pending', 'accepted', 'rejected')),
    evidence_reference  TEXT,
    CHECK (from_event_id <> to_event_id),
    UNIQUE (from_event_id, to_event_id, relation_type)
);

-- ---------------------------------------------------------------------------
-- NOAA descriptor catalogs (codes from the HazEL descriptor files)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS noaa_cause          (code INTEGER PRIMARY KEY, description TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS noaa_event_validity (code INTEGER PRIMARY KEY, description TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS noaa_region         (code INTEGER PRIMARY KEY, description TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS noaa_warning_status (code INTEGER PRIMARY KEY, description TEXT NOT NULL);

-- ---------------------------------------------------------------------------
-- Domain tables: exactly one detail row per event, matching its event_type
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS earthquake (
    event_id               BIGINT PRIMARY KEY,
    event_type             TEXT NOT NULL DEFAULT 'earthquake' CHECK (event_type = 'earthquake'),
    magnitude              NUMERIC(4, 2),
    magnitude_type         TEXT,
    depth_km               DOUBLE PRECISION,
    place                  TEXT,
    status                 TEXT,
    source_updated_at      TIMESTAMPTZ,           -- USGS "updated", not the origin time
    tsunami_bulletin_flag  BOOLEAN,               -- USGS "tsunami": a bulletin may exist, not a confirmed tsunami
    FOREIGN KEY (event_id, event_type) REFERENCES event (event_id, event_type) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tsunami_event (
    event_id                  BIGINT PRIMARY KEY,
    event_type                TEXT NOT NULL DEFAULT 'tsunami' CHECK (event_type = 'tsunami'),
    cause_code                INTEGER REFERENCES noaa_cause (code),
    event_validity            INTEGER REFERENCES noaa_event_validity (code),
    region_code               INTEGER REFERENCES noaa_region (code),
    warning_status_id         INTEGER REFERENCES noaa_warning_status (code),
    noaa_earthquake_event_id  INTEGER,            -- NOAA earthquake ID, not a USGS key
    reported_eq_magnitude     NUMERIC(4, 2),
    reported_eq_depth         DOUBLE PRECISION,   -- km (HazEL "Focal Depth (km)")
    max_water_height          DOUBLE PRECISION,   -- m (HazEL "Maximum Water Height (m)")
    num_runups                INTEGER,            -- a count, not a height
    oceanic_tsunami           BOOLEAN,
    country                   TEXT,
    location_name             TEXT,
    FOREIGN KEY (event_id, event_type) REFERENCES event (event_id, event_type) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tsunami_bulletin (
    event_id                    BIGINT PRIMARY KEY,
    event_type                  TEXT NOT NULL DEFAULT 'bulletin' CHECK (event_type = 'bulletin'),
    center                      TEXT NOT NULL CHECK (center IN ('ntwc', 'ptwc')),
    cap_identifier              TEXT NOT NULL UNIQUE,
    atom_entry_id               TEXT,
    sender                      TEXT,
    sent_at                     TIMESTAMPTZ,       -- issue time
    atom_updated_at             TIMESTAMPTZ,
    expires_at                  TIMESTAMPTZ,       -- kept as published, even if before sent_at
    msg_type                    TEXT,
    cap_event                   TEXT,
    cap_severity                TEXT,
    urgency                     TEXT,
    certainty                   TEXT,
    reported_origin_at          TIMESTAMPTZ,       -- reported earthquake origin (may be inconsistent)
    preliminary_magnitude       NUMERIC(4, 2),
    preliminary_magnitude_type  TEXT,
    reported_depth_text         TEXT,              -- original text, e.g. "13 kilometers"
    reported_depth_km           DOUBLE PRECISION,  -- parsed only when the unit is explicit
    FOREIGN KEY (event_id, event_type) REFERENCES event (event_id, event_type) ON DELETE CASCADE
);
