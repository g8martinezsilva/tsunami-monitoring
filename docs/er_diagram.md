# Preliminary entity–relationship diagram

This Workshop 1 diagram shows proposed identities and optional links. Each `event` has one matching domain row (`earthquake`, `tsunami_event`, or `tsunami_bulletin`) according to `event_type`. The three optional lines from `event` express that subtype choice; they do not mean that a single event should have all three rows. A source record identifies a saved observation, while `event_id` identifies the internally represented event.

```mermaid
erDiagram
    event {
        string event_id PK
        string source
        string event_type
        datetime timestamp
        date event_date
        string time_precision
        decimal latitude
        decimal longitude
        string severity
        string severity_scale
        string severity_unit
        string raw_reference
    }

    source_record {
        string source_record_id PK
        string event_id FK
        string source
        string original_id
        string raw_reference
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
        string causative_earthquake_event_id FK
        decimal reported_eq_magnitude
        decimal reported_eq_depth
        decimal max_water_height
        int num_runups
    }

    tsunami_bulletin {
        string event_id PK, FK
        string center
        string atom_entry_id
        string cap_identifier
        datetime sent_at
        datetime reported_origin_at
        string msg_type
        string cap_severity
        string urgency
        string certainty
        decimal preliminary_magnitude
        string earthquake_event_id FK
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

    event ||--|{ source_record : "documented by"
    event ||--o| earthquake : "earthquake details"
    event ||--o| tsunami_event : "tsunami details"
    event ||--o| tsunami_bulletin : "bulletin details"
    earthquake o|--o{ tsunami_event : "confirmed cause"
    earthquake o|--o{ tsunami_bulletin : "confirmed subject"
    noaa_cause o|--o{ tsunami_event : "cause code"
    noaa_event_validity o|--o{ tsunami_event : "validity code"
    noaa_region o|--o{ tsunami_event : "region code"
    noaa_warning_status o|--o{ tsunami_event : "historical status code"
```

`o|` on the earthquake side means a tsunami or bulletin can have no linked earthquake. A link is set only after review, not from matching source IDs alone. The four descriptor relationships are nullable; a missing NOAA code stays null. CAP `cap_severity` belongs to the bulletin and is unrelated to `noaa_warning_status`. Full column definitions and normalization rules are in `workshop_1/mapping.md`.
