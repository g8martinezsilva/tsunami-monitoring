"""Bronze -> Silver preprocessing. Each function reads the raw files of one Bronze run and
returns (clean DataFrame, rejected records). The rules follow the Workshop 1 mapping
(workshop_1/mapping.md) and profiling: UTC times, WGS84 coordinates in -180..180, units
kept explicit, partial NOAA times never completed with invented values, inconsistent
source values flagged and preserved, and duplicates removed by source identifier."""

import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

CAP = "{urn:oasis:names:tc:emergency:cap:1.2}"
ATOM = "{http://www.w3.org/2005/Atom}"

# Mutable observation attributes persisted in PostgreSQL. Identifiers, revision times,
# lineage and derived quality flags do not change the business content.
HASH_FIELDS = {
    "earthquake": ["event_time", "latitude", "longitude", "depth_km", "magnitude", "magnitude_type",
                   "place", "status", "tsunami_bulletin_flag"],
    "tsunami": ["event_time", "event_date", "known_hour", "known_minute", "time_precision", "latitude", "longitude",
                "cause_code", "event_validity", "region_code", "warning_status_id", "noaa_earthquake_event_id",
                "reported_eq_magnitude", "reported_eq_depth", "max_water_height", "num_runups",
                "oceanic_tsunami", "country", "location_name"],
    "bulletin": ["sender", "sent_at", "expires_at", "msg_type", "cap_event", "cap_severity", "urgency", "certainty",
                 "reported_origin_at", "preliminary_magnitude", "preliminary_magnitude_type",
                 "reported_depth_text", "reported_depth_km", "latitude", "longitude"],
}
MAGNITUDE_FIELDS = {"magnitude", "reported_eq_magnitude", "preliminary_magnitude"}


def _hash_value(field, value):
    if pd.isna(value):
        return None
    if field in MAGNITUDE_FIELDS:  # NUMERIC(4, 2) in the domain tables
        rounded = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return "0.00" if rounded == 0 else str(rounded)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    return str(value)


def content_hash(row, fields):
    values = {field: _hash_value(field, row[field]) for field in fields}
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def normalize_longitude(lon):
    """Bring longitudes to -180..180 (USGS Pacific queries can return values above 180)."""
    return ((lon + 180) % 360) - 180


def iqr_outlier(series):
    """Flag values outside [Q1 - 1.5 IQR, Q3 + 1.5 IQR]; flagged values are kept, not dropped."""
    values = series.dropna()
    if values.empty:
        return pd.Series(False, index=series.index)
    q1, q3 = values.quantile(0.25), values.quantile(0.75)
    low, high = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    return series.notna() & ((series < low) | (series > high))


def _reject(rejected, source, external_id, reason, bronze_file, locator):
    rejected.append({"source": source, "external_id": None if external_id is None else str(external_id),
                     "reason": reason, "bronze_file": Path(bronze_file).name, "locator": locator})


def _coordinates_ok(lat, lon):
    return (pd.isna(lat) or -90 <= lat <= 90) and (pd.isna(lon) or -180 <= lon <= 180)


# ---------------------------------------------------------------------------
# Earthquakes: USGS FDSN (CSV) and USGS real-time feed (GeoJSON)
# ---------------------------------------------------------------------------

def earthquakes_from_fdsn(paths):
    rows, rejected = [], []
    for path in paths:
        frame = pd.read_csv(path)
        for index, record in frame.iterrows():
            locator = f"row {index + 2}"
            if pd.isna(record["id"]):
                _reject(rejected, "usgs_fdsn", None, "missing id", path, locator)
                continue
            if record["type"] != "earthquake":
                _reject(rejected, "usgs_fdsn", record["id"], f"event type {record['type']}", path, locator)
                continue
            rows.append({
                "source": "usgs_fdsn", "external_id": record["id"],
                "event_time": pd.to_datetime(record["time"], utc=True, errors="coerce"),
                "latitude": record["latitude"], "longitude": normalize_longitude(record["longitude"]),
                "depth_km": record["depth"], "magnitude": record["mag"], "magnitude_type": record["magType"],
                "place": record["place"], "status": record["status"], "net": record["net"],
                "source_updated_at": pd.to_datetime(record["updated"], utc=True, errors="coerce"),
                "tsunami_bulletin_flag": pd.NA,
                "bronze_file": Path(path).name, "locator": locator,
            })
    return rows, rejected


def _earthquakes_from_features(features, bronze_file):
    rows, rejected = [], []
    for feature in features:
        props, coords = feature["properties"], feature["geometry"]["coordinates"]
        if not feature.get("id"):
            _reject(rejected, "usgs_realtime", None, "missing id", bronze_file, "feature")
            continue
        tsunami_flag = props.get("tsunami")
        rows.append({
            "source": "usgs_realtime", "external_id": feature["id"],
            "event_time": pd.to_datetime(props["time"], unit="ms", utc=True),
            "latitude": coords[1], "longitude": normalize_longitude(coords[0]), "depth_km": coords[2],
            "magnitude": props["mag"], "magnitude_type": props["magType"], "place": props["place"],
            "status": props["status"], "net": props["net"],
            "source_updated_at": pd.to_datetime(props.get("updated"), unit="ms", utc=True, errors="coerce"),
            "tsunami_bulletin_flag": None if tsunami_flag is None else bool(tsunami_flag),
            "bronze_file": Path(bronze_file).name, "locator": f"feature {feature['id']}",
        })
    return rows, rejected


def clean_earthquakes(fdsn_paths, feed_paths):
    rows, rejected = earthquakes_from_fdsn(fdsn_paths)
    for path in feed_paths:
        features = json.loads(Path(path).read_text(encoding="utf-8"))["features"]
        feed_rows, feed_rejected = _earthquakes_from_features(features, path)
        rows += feed_rows
        rejected += feed_rejected
    return _clean_earthquake_rows(rows, rejected)


def clean_realtime_features(features, bronze_file):
    """Normalize a downloaded feed in memory using the same rules as Silver."""
    return _clean_earthquake_rows(*_earthquakes_from_features(features, bronze_file))


def _clean_earthquake_rows(rows, rejected):
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame, rejected

    # Validation: a usable origin time, a magnitude, and coordinates in range
    valid = frame["event_time"].notna() & frame["magnitude"].notna() & frame.apply(
        lambda r: _coordinates_ok(r["latitude"], r["longitude"]), axis=1)
    for _, record in frame[~valid].iterrows():
        _reject(rejected, record["source"], record["external_id"],
                "missing time or magnitude, or coordinates out of range", record["bronze_file"], record["locator"])
    frame = frame[valid].copy()

    # Deduplication: one row per (source, id), keeping the latest revision
    frame = (frame.sort_values("source_updated_at", kind="stable", na_position="first")
                  .drop_duplicates(["source", "external_id"], keep="last")
                  .reset_index(drop=True))
    frame["magnitude"] = frame["magnitude"].astype(float)
    frame["depth_km"] = frame["depth_km"].astype(float)
    frame["magnitude_type"] = frame["magnitude_type"].astype("string")
    frame["tsunami_bulletin_flag"] = frame["tsunami_bulletin_flag"].astype("boolean")
    frame["depth_km_iqr_outlier"] = iqr_outlier(frame["depth_km"])
    frame["content_hash"] = frame.apply(lambda r: content_hash(r, HASH_FIELDS["earthquake"]), axis=1)
    return frame, rejected


# ---------------------------------------------------------------------------
# Historical tsunamis: NOAA HazEL (JSON pages)
# ---------------------------------------------------------------------------

def _time_fields(item):
    """Keep NOAA time precision: a full timestamp only when the seconds are known."""
    year, month, day = item.get("year"), item.get("month"), item.get("day")
    hour, minute, second = item.get("hour"), item.get("minute"), item.get("second")
    event_date = pd.Timestamp(year=year, month=month, day=day).date() if None not in (year, month, day) else None
    if event_date and None not in (hour, minute, second):
        precision = "second" if float(second).is_integer() else "subsecond"
        timestamp = pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute, tz="UTC") \
            + pd.to_timedelta(second, unit="s")
    else:
        timestamp = pd.NaT
        precision = "minute" if minute is not None else "hour" if hour is not None else "day" if event_date else None
    return event_date, timestamp, precision


def clean_tsunamis(paths):
    rows, rejected = [], []
    for path in paths:
        for position, item in enumerate(json.loads(Path(path).read_text(encoding="utf-8"))["items"]):
            locator = f"items[{position}]"
            event_date, timestamp, precision = _time_fields(item)
            lat, lon = item.get("latitude"), item.get("longitude")
            if item.get("id") is None or event_date is None:
                _reject(rejected, "noaa_hazel", item.get("id"), "missing id or date", path, locator)
                continue
            if not _coordinates_ok(lat, lon):
                _reject(rejected, "noaa_hazel", item["id"], "coordinates out of range", path, locator)
                continue
            rows.append({
                "source": "noaa_hazel", "external_id": str(item["id"]),
                "event_time": timestamp, "event_date": event_date,
                "known_hour": item.get("hour"), "known_minute": item.get("minute"), "seconds": item.get("second"),
                "time_precision": precision,
                "latitude": lat, "longitude": None if lon is None else normalize_longitude(lon),
                "cause_code": item.get("causeCode"), "event_validity": item.get("eventValidity"),
                "region_code": item.get("regionCode"), "warning_status_id": item.get("warningStatusId"),
                "noaa_earthquake_event_id": item.get("earthquakeEventId"),
                "reported_eq_magnitude": item.get("eqMagnitude"),
                "reported_eq_depth": item.get("eqDepth"),          # km
                "max_water_height": item.get("maxWaterHeight"),    # m
                "num_runups": item.get("numRunups"), "oceanic_tsunami": item.get("oceanicTsunami"),
                "country": item.get("country"), "location_name": item.get("locationName"),
                "bronze_file": Path(path).name, "locator": locator,
            })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame, rejected
    frame = frame.drop_duplicates(["source", "external_id"], keep="last").reset_index(drop=True)
    for column in ["known_hour", "known_minute", "cause_code", "event_validity", "region_code",
                   "warning_status_id", "noaa_earthquake_event_id", "num_runups"]:
        frame[column] = frame[column].astype("Int64")       # nullable integers: missing stays missing
    for column in ["seconds", "latitude", "longitude", "reported_eq_magnitude", "reported_eq_depth", "max_water_height"]:
        frame[column] = frame[column].astype(float)
    frame["oceanic_tsunami"] = frame["oceanic_tsunami"].astype("boolean")
    frame["max_water_height_iqr_outlier"] = iqr_outlier(frame["max_water_height"])
    frame["content_hash"] = frame.apply(lambda r: content_hash(r, HASH_FIELDS["tsunami"]), axis=1)
    return frame, rejected


# ---------------------------------------------------------------------------
# Bulletins: NTWC/PTWC Atom feeds and their CAP messages
# ---------------------------------------------------------------------------

def _utc(text):
    return pd.to_datetime(text, utc=True, errors="coerce") if text else pd.NaT


def _number(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def clean_bulletins(feed_paths, cap_paths):
    """feed_paths: {center: atom file}; cap_paths: {center: [cap files in entry order]}."""
    messages = []
    for center, cap_files in cap_paths.items():
        entries = ET.parse(feed_paths[center]).getroot().findall(f"{ATOM}entry") if center in feed_paths else []
        for number, path in enumerate(cap_files):
            entry = entries[number] if number < len(entries) else None
            messages.append((center, ET.parse(path).getroot(), entry, path))
    return clean_bulletin_messages(messages)


def clean_bulletin_messages(messages):
    """Normalize parsed CAP alerts paired with their Atom entries and Bronze filenames."""
    rows, rejected = [], []
    for center, root, entry, path in messages:
        if root.tag != f"{CAP}alert":
            raise ValueError(f"{path}: expected a CAP alert")
        info = root.find(f"{CAP}info")
        params = {p.findtext(f"{CAP}valueName"): p.findtext(f"{CAP}value") for p in root.iter(f"{CAP}parameter")}
        identifier = root.findtext(f"{CAP}identifier")
        if not identifier or info is None:
            _reject(rejected, center, identifier, "CAP message without identifier or info", path, "alert")
            continue
        lat = lon = None
        if params.get("EventLatLon"):
            lat, lon = (_number(value) for value in params["EventLatLon"].split()[0].split(","))
        depth_text = params.get("EventDepth")
        rows.append({
            "source": center, "external_id": identifier, "center": center, "cap_identifier": identifier,
            "atom_entry_id": entry.findtext(f"{ATOM}id") if entry is not None else None,
            "atom_updated_at": _utc(entry.findtext(f"{ATOM}updated")) if entry is not None else pd.NaT,
            "sender": root.findtext(f"{CAP}sender"), "sent_at": _utc(root.findtext(f"{CAP}sent")),
            "expires_at": _utc(info.findtext(f"{CAP}expires")), "msg_type": root.findtext(f"{CAP}msgType"),
            "cap_event": info.findtext(f"{CAP}event"), "cap_severity": info.findtext(f"{CAP}severity"),
            "urgency": info.findtext(f"{CAP}urgency"), "certainty": info.findtext(f"{CAP}certainty"),
            "reported_origin_at": _utc(params.get("EventOriginTime")),
            "preliminary_magnitude": _number(params.get("EventPreliminaryMagnitude")),
            "preliminary_magnitude_type": params.get("EventPreliminaryMagnitudeType"),
            "reported_depth_text": depth_text,
            # Converted only when the unit is explicit
            "reported_depth_km": _number(depth_text.split()[0])
                if depth_text and depth_text.strip().endswith("kilometers") else None,
            "location_name": params.get("EventLocationName"),
            "latitude": lat, "longitude": None if lon is None else normalize_longitude(lon),
            "bronze_file": Path(path).name, "locator": "alert",
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame, rejected
    frame = (frame.sort_values("atom_updated_at", kind="stable", na_position="first")
                  .drop_duplicates(["source", "external_id"], keep="last").reset_index(drop=True))
    # Flag, never correct: problem 8 of Workshop 1 (a message that expires before it is sent)
    frame["expires_before_sent"] = (frame["expires_at"] < frame["sent_at"]).astype("boolean")
    frame["content_hash"] = frame.apply(lambda r: content_hash(r, HASH_FIELDS["bulletin"]), axis=1)
    return frame, rejected


# ---------------------------------------------------------------------------
# One Bronze run -> Silver Parquet files (Updated for Workshop 3)
# ---------------------------------------------------------------------------

def process_bronze_run(manifest_path, silver_dir, stamp):
    """Clean every file listed in a Bronze manifest and write topic subfolders
    under datalake_silver/<topic>/<topic>_YYYYMMDD_HHMMSS.parquet.
    Each row keeps its Bronze file (lineage); manifests are stored in datalake_silver/_manifests/."""
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    files = {summary["source"]: [Path(f).name for f in summary["files"]] for summary in manifest["sources"]}
    for summary in manifest["sources"]:
        if summary["source"] == "usgs_fdsn":
            files["usgs_fdsn"] = [Path(f).name for s in manifest["sources"] if s["source"] == "usgs_fdsn"
                                  for f in s["files"]]
    bronze = Path(manifest_path).parent

    earthquakes, rejected_eq = clean_earthquakes(
        [bronze / f for f in files.get("usgs_fdsn", [])], [bronze / f for f in files.get("usgs_realtime", [])])
    tsunamis, rejected_ts = clean_tsunamis([bronze / f for f in files.get("noaa_hazel", [])])
    feeds = {c: bronze / f for c in ("ntwc", "ptwc") for f in files.get(c, []) if "_cap" not in f}
    caps = {c: [bronze / f for f in files.get(c, []) if "_cap" in f] for c in ("ntwc", "ptwc")}
    bulletins, rejected_bu = clean_bulletins(feeds, caps)
    rejected = pd.DataFrame(rejected_eq + rejected_ts + rejected_bu,
                            columns=["source", "external_id", "reason", "bronze_file", "locator"])

    silver_dir = Path(silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir = silver_dir / "_manifests"
    manifests_dir.mkdir(exist_ok=True)
    outputs = {}

    for topic, frame in [("earthquakes", earthquakes), ("tsunami_events", tsunamis),
                         ("tsunami_bulletins", bulletins), ("rejected_records", rejected)]:
        if frame.empty:  # no empty parquet: it would have no columns and break folder reads
            outputs[topic] = {"file": None, "rows": 0}
            continue
        frame = frame.copy()
        frame["bronze_manifest"] = Path(manifest_path).name
        frame["silver_processed_at"] = datetime.now(timezone.utc)
        topic_dir = silver_dir / topic
        topic_dir.mkdir(exist_ok=True)
        path = topic_dir / f"{topic}_{stamp}.parquet"
        # Microsecond timestamps: pandas writes nanoseconds by default, which Spark 3.5 cannot read
        frame.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        outputs[topic] = {"file": str(path), "rows": len(frame)}

    silver_manifest = {
        "bronze_manifest": Path(manifest_path).name,
        "stamp": stamp,
        "mode": manifest.get("mode", "historical"),
        "outputs": outputs,
    }
    (manifests_dir / f"silver_manifest_{stamp}.json").write_text(json.dumps(silver_manifest, indent=2), encoding="utf-8")
    return silver_manifest
