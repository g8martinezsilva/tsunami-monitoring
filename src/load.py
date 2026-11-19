"""Silver -> PostgreSQL: idempotent load of one Silver run into the canonical model (sql/01_schema.sql).

Events are upserted on (source, external_id) and only rewritten when their content hash changes;
each source gets an ingestion_run row with its counts, rejected records go to rejected_record,
and the source watermark advances in the same transaction as the data (Workshop 1 contract)."""

import json
import logging
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger(__name__)

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
WHERE event.content_hash IS DISTINCT FROM EXCLUDED.content_hash
RETURNING event_id, (xmax = 0) AS inserted
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
        ON CONFLICT (event_id) DO UPDATE SET atom_entry_id = EXCLUDED.atom_entry_id,
            atom_updated_at = EXCLUDED.atom_updated_at, expires_at = EXCLUDED.expires_at,
            cap_severity = EXCLUDED.cap_severity, reported_origin_at = EXCLUDED.reported_origin_at,
            preliminary_magnitude = EXCLUDED.preliminary_magnitude,
            reported_depth_km = EXCLUDED.reported_depth_km""",
}


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
    sources = sorted(set().union(*[set(frames[t]["source"]) for t, _ in topics if not frames[t].empty],
                                 set(rejected["source"])))
    results = {}
    for source in sources:
        with connection:  # commit on success, roll back on error
            with connection.cursor() as cursor:
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
                    for record in frame[frame["source"] == source].to_dict("records"):
                        counts["read"] += 1
                        event, row = _event_row(record, event_type)
                        cursor.execute(EVENT_UPSERT, event)
                        result = cursor.fetchone()
                        if result is None:  # unchanged content: nothing to rewrite
                            counts["unchanged"] += 1
                            continue
                        counts["accepted" if result[1] else "updated"] += 1
                        cursor.execute(DOMAIN_UPSERTS[event_type], {**row, "event_id": result[0]})
                    if event_type == "earthquake" and "source_updated_at" in frame:
                        updated = frame.loc[frame["source"] == source, "source_updated_at"]
                        if not updated.dropna().empty:
                            watermark = updated.max().isoformat()
                    if event_type == "bulletin" and "sent_at" in frame:
                        sent = frame.loc[frame["source"] == source, "sent_at"]
                        if not sent.dropna().empty:
                            watermark = sent.max().isoformat()
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
                cursor.execute(
                    "INSERT INTO ingestion_state (source_id, watermark, last_successful_run_id) VALUES (%s, %s, %s) "
                    "ON CONFLICT (source_id) DO UPDATE SET last_successful_run_id = EXCLUDED.last_successful_run_id, "
                    "watermark = COALESCE(GREATEST(ingestion_state.watermark, EXCLUDED.watermark), "
                    "ingestion_state.watermark, EXCLUDED.watermark)",
                    (source, watermark, run_id))
        log.info("%s: run %s %s", source, run_id, counts)
        results[source] = {"run_id": run_id, **counts}
    return results