"""Silver -> PostgreSQL: idempotent load of one Silver run into the canonical model (sql/01_schema.sql).

Events are upserted on (source, external_id) after comparing the source revision and business
content. Each source gets an ingestion_run with its counts and commits its watermark with the data."""

import json
import logging
from datetime import datetime, timezone

import pandas as pd

from src.silver import HASH_FIELDS, content_hash

log = logging.getLogger(__name__)

DOMAIN_TABLES = {"earthquake": "earthquake", "tsunami": "tsunami_event", "bulletin": "tsunami_bulletin"}
EVENT_FIELDS = {
    "event_time": 'e."timestamp"', "event_date": "e.event_date", "known_hour": "e.known_hour",
    "known_minute": "e.known_minute", "time_precision": "e.time_precision",
    "latitude": "e.latitude", "longitude": "e.longitude",
}
REVISION_FIELDS = {
    "earthquake": ("source_updated_at",),
    "tsunami": (),
    "bulletin": ("atom_updated_at", "atom_entry_id"),
}

EVENT_UPSERT = """
INSERT INTO event (source, external_id, event_type, "timestamp", event_date, known_hour, known_minute,
                   time_precision, latitude, longitude, severity, severity_scale, severity_unit,
                   raw_reference, content_hash)
VALUES (%(source)s, %(external_id)s, %(event_type)s, %(timestamp)s, %(event_date)s, %(known_hour)s,
        %(known_minute)s, %(time_precision)s, %(latitude)s, %(longitude)s, %(severity)s,
        %(severity_scale)s, %(severity_unit)s, %(raw_reference)s, %(content_hash)s)
ON CONFLICT (source, external_id) DO UPDATE SET
    "timestamp" = EXCLUDED."timestamp", event_date = EXCLUDED.event_date, known_hour = EXCLUDED.known_hour,
    known_minute = EXCLUDED.known_minute, time_precision = EXCLUDED.time_precision,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, severity = EXCLUDED.severity,
    severity_scale = EXCLUDED.severity_scale, severity_unit = EXCLUDED.severity_unit,
    raw_reference = EXCLUDED.raw_reference, content_hash = EXCLUDED.content_hash, updated_at = now()
RETURNING event_id
"""

DOMAIN_UPSERTS = {
    "earthquake": """
        INSERT INTO earthquake (event_id, magnitude, magnitude_type, depth_km, place, status,
                                source_updated_at, tsunami_bulletin_flag)
        VALUES (%(event_id)s, %(magnitude)s, %(magnitude_type)s, %(depth_km)s, %(place)s, %(status)s,
                %(source_updated_at)s, %(tsunami_bulletin_flag)s)
        ON CONFLICT (event_id) DO UPDATE SET magnitude = EXCLUDED.magnitude,
            magnitude_type = EXCLUDED.magnitude_type, depth_km = EXCLUDED.depth_km, place = EXCLUDED.place,
            status = EXCLUDED.status, source_updated_at = EXCLUDED.source_updated_at,
            tsunami_bulletin_flag = EXCLUDED.tsunami_bulletin_flag""",
    "tsunami": """
        INSERT INTO tsunami_event (event_id, cause_code, event_validity, region_code, warning_status_id,
                                   noaa_earthquake_event_id, reported_eq_magnitude, reported_eq_depth,
                                   max_water_height, num_runups, oceanic_tsunami, country, location_name)
        VALUES (%(event_id)s, %(cause_code)s, %(event_validity)s, %(region_code)s, %(warning_status_id)s,
                %(noaa_earthquake_event_id)s, %(reported_eq_magnitude)s, %(reported_eq_depth)s,
                %(max_water_height)s, %(num_runups)s, %(oceanic_tsunami)s, %(country)s, %(location_name)s)
        ON CONFLICT (event_id) DO UPDATE SET cause_code = EXCLUDED.cause_code,
            event_validity = EXCLUDED.event_validity, region_code = EXCLUDED.region_code,
            warning_status_id = EXCLUDED.warning_status_id,
            noaa_earthquake_event_id = EXCLUDED.noaa_earthquake_event_id,
            reported_eq_magnitude = EXCLUDED.reported_eq_magnitude,
            reported_eq_depth = EXCLUDED.reported_eq_depth, max_water_height = EXCLUDED.max_water_height,
            num_runups = EXCLUDED.num_runups, oceanic_tsunami = EXCLUDED.oceanic_tsunami,
            country = EXCLUDED.country, location_name = EXCLUDED.location_name""",
    "bulletin": """
        INSERT INTO tsunami_bulletin (event_id, center, cap_identifier, atom_entry_id, sender, sent_at,
                                      atom_updated_at, expires_at, msg_type, cap_event, cap_severity, urgency,
                                      certainty, reported_origin_at, preliminary_magnitude,
                                      preliminary_magnitude_type, reported_depth_text, reported_depth_km)
        VALUES (%(event_id)s, %(center)s, %(cap_identifier)s, %(atom_entry_id)s, %(sender)s, %(sent_at)s,
                %(atom_updated_at)s, %(expires_at)s, %(msg_type)s, %(cap_event)s, %(cap_severity)s, %(urgency)s,
                %(certainty)s, %(reported_origin_at)s, %(preliminary_magnitude)s,
                %(preliminary_magnitude_type)s, %(reported_depth_text)s, %(reported_depth_km)s)
        ON CONFLICT (event_id) DO UPDATE SET
            atom_entry_id = COALESCE(EXCLUDED.atom_entry_id, tsunami_bulletin.atom_entry_id),
            atom_updated_at = COALESCE(EXCLUDED.atom_updated_at, tsunami_bulletin.atom_updated_at),
            sender = EXCLUDED.sender, sent_at = EXCLUDED.sent_at, expires_at = EXCLUDED.expires_at,
            msg_type = EXCLUDED.msg_type, cap_event = EXCLUDED.cap_event, cap_severity = EXCLUDED.cap_severity,
            urgency = EXCLUDED.urgency, certainty = EXCLUDED.certainty, reported_origin_at = EXCLUDED.reported_origin_at,
            preliminary_magnitude = EXCLUDED.preliminary_magnitude,
            preliminary_magnitude_type = EXCLUDED.preliminary_magnitude_type,
            reported_depth_text = EXCLUDED.reported_depth_text,
            reported_depth_km = EXCLUDED.reported_depth_km""",
}

REVISION_UPDATES = {
    "earthquake": """
        UPDATE earthquake SET source_updated_at = %(source_updated_at)s WHERE event_id = %(event_id)s""",
    "bulletin": """
        UPDATE tsunami_bulletin SET atom_entry_id = COALESCE(%(atom_entry_id)s, atom_entry_id),
            atom_updated_at = COALESCE(%(atom_updated_at)s, atom_updated_at)
        WHERE event_id = %(event_id)s""",
}

STATE_UPSERT = """
INSERT INTO ingestion_state (source_id, watermark, last_successful_run_id) VALUES (%s, %s, %s)
ON CONFLICT (source_id) DO UPDATE SET last_successful_run_id = EXCLUDED.last_successful_run_id,
    watermark = CASE
        WHEN EXCLUDED.watermark IS NULL THEN ingestion_state.watermark
        WHEN NULLIF(ingestion_state.watermark, '')::timestamptz >= EXCLUDED.watermark::timestamptz
            THEN ingestion_state.watermark
        ELSE EXCLUDED.watermark
    END
"""


def _clean(value):
    """pandas missing values and timestamps -> psycopg2-friendly Python values."""
    if value is None or (not isinstance(value, (list, dict, str)) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item"):  # numpy scalars
        return value.item()
    return value


def _event_row(record, event_type):
    row = {key: _clean(value) for key, value in record.items()}
    # A pending Silver file may still carry the hash from an older Workshop.
    row["content_hash"] = content_hash(row, HASH_FIELDS[event_type])
    common = {
        "source": row["source"], "external_id": row["external_id"], "event_type": event_type,
        "timestamp": row.get("event_time") if event_type != "bulletin" else row.get("sent_at"),
        "event_date": None, "known_hour": None, "known_minute": None,
        "time_precision": "subsecond" if event_type == "earthquake" else "second",
        "latitude": row.get("latitude"), "longitude": row.get("longitude"),
        "raw_reference": json.dumps({"bronze_file": row["bronze_file"], "locator": row["locator"],
                                     "external_id": row["external_id"]}),
        "content_hash": row["content_hash"],
    }
    if event_type == "earthquake":
        common.update(severity=None if row["magnitude"] is None else str(row["magnitude"]),
                      severity_scale="earthquake_magnitude", severity_unit=None)
        common["event_date"] = common["timestamp"].date() if common["timestamp"] else None
    elif event_type == "tsunami":
        common.update(event_date=row["event_date"], known_hour=row["known_hour"], known_minute=row["known_minute"],
                      time_precision=row["time_precision"],
                      severity=None if row["max_water_height"] is None else str(row["max_water_height"]),
                      severity_scale="tsunami_max_water_height", severity_unit="m")
    else:
        common.update(severity=row["cap_severity"], severity_scale="cap_severity", severity_unit=None,
                      event_date=common["timestamp"].date() if common["timestamp"] else None)
    return common, row


def read_observations(cursor, source, event_type, external_ids):
    """Read the committed revisions and comparable content for the supplied source keys."""
    if not external_ids:
        return {}
    fields = HASH_FIELDS[event_type] + list(REVISION_FIELDS[event_type])
    projection = ", ".join(EVENT_FIELDS.get(field, f"d.{field}") for field in fields)
    cursor.execute(
        f"""SELECT e.external_id, e.event_id, e.content_hash, d.event_id, {projection}
            FROM event e LEFT JOIN {DOMAIN_TABLES[event_type]} d ON e.event_id = d.event_id
            WHERE e.source = %s AND e.external_id = ANY(%s)""",
        (source, external_ids),
    )
    observations = {}
    for external_id, event_id, stored_hash, detail_id, *values in cursor.fetchall():
        row = dict(zip(fields, values))
        # Re-hash the persisted fields so previously loaded hashes compare under the same rules.
        observations[external_id] = {
            **row, "event_id": event_id, "stored_hash": stored_hash,
            "content_hash": content_hash(row, HASH_FIELDS[event_type]) if detail_id is not None else None,
        }
    return observations


def observation_change(event_type, row, previous):
    """Classify a normalized row against the current observation with the same source ID."""
    if previous is None:
        return "new"

    if event_type == "earthquake":
        before, incoming = previous["source_updated_at"], _clean(row["source_updated_at"])
        if before is not None and (incoming is None or incoming < before):
            return "stale"
    elif event_type == "bulletin":
        before, incoming = previous["atom_updated_at"], _clean(row["atom_updated_at"])
        if before is None or incoming is None:
            before, incoming = previous["sent_at"], _clean(row["sent_at"])
        if before is not None and (incoming is None or incoming < before):
            return "stale"

    if row["content_hash"] != previous["content_hash"]:
        return "updated"
    for field in REVISION_FIELDS[event_type]:
        value = _clean(row[field])
        if value is not None and value != previous[field]:
            return "metadata"
    return "unchanged"


def require_workshop3_schema(cursor):
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_attribute
            WHERE attrelid = to_regclass('ingestion_run') AND attname = 'unchanged_count'
              AND atttypid = 'integer'::regtype AND attnotnull AND atthasdef AND NOT attisdropped
        )
    """)
    if not cursor.fetchone()[0]:
        raise RuntimeError("Missing or incompatible ingestion_run.unchanged_count. "
                           "Follow the migration procedure in workshop_3/README.md.")


def _load_observations(cursor, frame, source, event_type, counts):
    records = frame[frame["source"] == source].to_dict("records")
    previous = read_observations(cursor, source, event_type, [r["external_id"] for r in records])
    revision_field = {"earthquake": "source_updated_at", "bulletin": "sent_at"}.get(event_type)
    watermark = None
    for record in records:
        counts["read"] += 1
        event, row = _event_row(record, event_type)
        existing = previous.get(row["external_id"])
        change = observation_change(event_type, row, existing)

        if change in {"new", "updated"}:
            cursor.execute(EVENT_UPSERT, event)
            cursor.execute(DOMAIN_UPSERTS[event_type], {**row, "event_id": cursor.fetchone()[0]})
            counts["accepted" if change == "new" else "updated"] += 1
        else:
            counts["unchanged"] += 1
            if change == "stale":
                continue
            params = {**row, "event_id": existing["event_id"], "raw_reference": event["raw_reference"]}
            if change == "metadata":
                cursor.execute(REVISION_UPDATES[event_type], params)
                cursor.execute("""
                    UPDATE event SET raw_reference = %(raw_reference)s, content_hash = %(content_hash)s
                    WHERE event_id = %(event_id)s
                """, params)
            elif row["content_hash"] != existing["stored_hash"]:
                cursor.execute("UPDATE event SET content_hash = %s WHERE event_id = %s",
                               (row["content_hash"], existing["event_id"]))

        version = row.get(revision_field)
        if version is not None and (watermark is None or version > watermark):
            watermark = version
    return watermark.isoformat() if watermark is not None else None


def load_silver_run(connection, silver_manifest, mode="historical"):
    """Load the Silver files of one run; returns counts per source. One transaction per source."""
    mode = silver_manifest.get("mode", mode)
    empty = {"rejected_records": pd.DataFrame(
        columns=["source", "external_id", "reason", "bronze_file", "locator"])}
    frames = {topic: pd.read_parquet(info["file"]) if info["file"]
              else empty.get(topic, pd.DataFrame(columns=["source"]))
              for topic, info in silver_manifest["outputs"].items()}
    topics = [("earthquakes", "earthquake"), ("tsunami_events", "tsunami"), ("tsunami_bulletins", "bulletin")]
    rejected = frames["rejected_records"]
    for topic, _ in topics:
        if not frames[topic].empty and frames[topic].duplicated(["source", "external_id"]).any():
            raise ValueError(f"Duplicate observation keys in Silver output {topic}")
    sources = sorted(set().union(*[set(frames[t]["source"]) for t, _ in topics if not frames[t].empty],
                                 set(rejected["source"])))
    results = {}
    for source in sources:
        with connection:  # commit on success, roll back on error
            with connection.cursor() as cursor:
                require_workshop3_schema(cursor)
                # Serialize loads of this source so version checks and writes see the same state.
                cursor.execute("SELECT source_id FROM source WHERE source_id = %s FOR NO KEY UPDATE", (source,))
                cursor.execute(
                    "INSERT INTO ingestion_run (source_id, mode, input_references) VALUES (%s, %s, %s) RETURNING run_id",
                    (source, mode, json.dumps([silver_manifest["bronze_manifest"]])))
                run_id = cursor.fetchone()[0]
                counts = {"read": 0, "accepted": 0, "updated": 0, "unchanged": 0, "rejected": 0}
                watermark = None
                for topic, event_type in topics:
                    frame = frames[topic]
                    if frame.empty:
                        continue
                    watermark = _load_observations(cursor, frame, source, event_type, counts) or watermark
                for record in rejected[rejected["source"] == source].to_dict("records"):
                    counts["read"] += 1
                    counts["rejected"] += 1
                    reference = json.dumps({"bronze_file": record["bronze_file"], "locator": record["locator"]})
                    # Quarantine each rejected source record once, even if the same Bronze file is reloaded
                    cursor.execute(
                        "INSERT INTO rejected_record (source_id, run_id, external_id, reason, raw_reference) "
                        "SELECT %s, %s, %s, %s, %s::jsonb WHERE NOT EXISTS (SELECT 1 FROM rejected_record "
                        "WHERE source_id = %s AND raw_reference = %s::jsonb AND reason = %s)",
                        (source, run_id, _clean(record["external_id"]), record["reason"], reference,
                         source, reference, record["reason"]))
                cursor.execute(
                    "UPDATE ingestion_run SET finished_at = %s, status = 'success', read_count = %s, "
                    "accepted_count = %s, updated_count = %s, unchanged_count = %s, rejected_count = %s "
                    "WHERE run_id = %s",
                    (datetime.now(timezone.utc), counts["read"], counts["accepted"], counts["updated"],
                     counts["unchanged"], counts["rejected"], run_id))
                cursor.execute(STATE_UPSERT, (source, watermark, run_id))
        log.info("%s: run %s %s", source, run_id, counts)
        results[source] = {"run_id": run_id, **counts}
    return results
