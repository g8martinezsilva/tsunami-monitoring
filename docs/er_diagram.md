# Workshop 1 entity–relationship diagram

`event` is one normalized observation from one source. `UNIQUE(source, external_id)` preserves source identity; a revision updates that observation rather than adding another `event`. `raw_reference` identifies the saved file and record, with a content hash when available for change detection; earlier snapshots remain in the raw/audit trail. `event_group` contains at least two verified observations of the **same phenomenon and event type**. An event can belong to zero or one group. Earthquakes, tsunamis, and bulletins remain distinct event types; candidate matches and cross-type links use `event_relation`.

```mermaid
erDiagram
    source {
        string source_id PK
        string name
        string url
        string format
        string license
    }

    ingestion_run {
        string run_id PK
        string source_id FK
        string mode
        datetime started_at
        datetime finished_at
        string status
        int read_count
        int accepted_count
        int updated_count
        int rejected_count
        json input_references
    }

    ingestion_state {
        string source_id PK, FK
        string watermark
        string last_successful_run_id FK
    }

    event_group {
        string event_group_id PK
        string event_type
        string evidence_reference
    }

    event {
        string event_id PK
        string source FK
        string external_id
        string event_group_id FK
        string event_type
        datetime timestamp
        date event_date
        int known_hour
        int known_minute
        string time_precision
        decimal latitude
        decimal longitude
        string severity
        string severity_scale
        string severity_unit
        json raw_reference
        boolean in_pacific_ocean
        boolean in_pacific_scope
    }

    rejected_record {
        string rejection_id PK
        string source_id FK
        string run_id FK
        string external_id
        string reason
        json raw_reference
    }

    event_relation {
        string relation_id PK
        string from_event_id FK
        string to_event_id FK
        string relation_type
        string review_status
        string evidence_reference
    }

    earthquake {
        string event_id PK, FK
        decimal magnitude
        string magnitude_type
        decimal depth_km
        string place
        string status
        datetime source_updated_at
        boolean tsunami_bulletin_flag
    }

    tsunami_event {
        string event_id PK, FK
        int cause_code FK
        int event_validity FK
        int region_code FK
        int warning_status_id FK
        int noaa_earthquake_event_id
        decimal reported_eq_magnitude
        decimal reported_eq_depth
        decimal max_water_height
        int num_runups
    }

    tsunami_bulletin {
        string event_id PK, FK
        string center
        string cap_identifier
        string atom_entry_id
        string sender
        datetime sent_at
        datetime atom_updated_at
        datetime expires_at
        string msg_type
        string cap_event
        string cap_severity
        string urgency
        string certainty
        datetime reported_origin_at
        decimal preliminary_magnitude
        string preliminary_magnitude_type
        string reported_depth_text
        decimal reported_depth_km
    }

    noaa_cause {
        int code PK
        string description
    }
    noaa_event_validity {
        int code PK
        string description
    }
    noaa_region {
        int code PK
        string description
    }
    noaa_warning_status {
        int code PK
        string description
    }

    source ||--o{ event : supplies
    source ||--o{ ingestion_run : executes
    source ||--o| ingestion_state : tracks
    source ||--o{ rejected_record : identifies
    ingestion_run ||--o{ rejected_record : quarantines
    ingestion_run o|--o| ingestion_state : last_successful
    event_group o|--|{ event : groups
    event ||--o| earthquake : has_details
    event ||--o| tsunami_event : has_details
    event ||--o| tsunami_bulletin : has_details
    event ||--o{ event_relation : from_event
    event ||--o{ event_relation : to_event
    noaa_cause o|--o{ tsunami_event : cause
    noaa_event_validity o|--o{ tsunami_event : validity
    noaa_region o|--o{ tsunami_event : region
    noaa_warning_status o|--o{ tsunami_event : warning_status
```

The three optional domain lines encode a subtype choice: **exactly one** detail row must match `event.event_type`. Mermaid's `|{` shows one or more group members but cannot express the **two-member minimum**, which is a business rule. `event_relation` records `identity_candidate`, `caused_by`, or `reports_on` links with `pending`, `accepted`, or `rejected` review status. A confirmed identity is represented by `event_group` membership; the original candidate relation may remain as review evidence without becoming a second source of truth. NOAA descriptor FKs, group membership, source watermark, and last successful run are optional where the source data or process do not provide them. Field definitions and validation rules are in [workshop_1/mapping.md](../workshop_1/mapping.md).
