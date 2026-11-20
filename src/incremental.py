"""Poll complete feeds and compare their observations against the committed database state.

The feed windows provide overlap. FDSN reconciliation uses a query window based on its
watermark. Published Bronze files retain the original responses for Silver processing.
"""

import json
import logging
import xml.etree.ElementTree as ET
from datetime import timedelta

import pandas as pd

from src.bronze_checks import check_file
from src.config import load_sources
from src.downloads import bronze_dir, get_with_retries, save_raw
from src.load import observation_change, read_observations
from src.silver import ATOM, clean_bulletin_messages, clean_realtime_features
from src.sources import FEED_EXTENSIONS

log = logging.getLogger(__name__)

STAMP_FORMAT = "%Y%m%d_%H%M%S"
MARKERS = "_new"


def read_watermarks(connection):
    """{source_id: watermark text or None} from ingestion_state."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT source_id, watermark FROM ingestion_state")
        return {source: watermark for source, watermark in cursor.fetchall()}


def _limit(watermark):
    """Parsed watermark, or None when there is none yet (first run) or it cannot be read."""
    if not watermark:
        return None
    limit = pd.to_datetime(watermark, utc=True, errors="coerce")
    return None if pd.isna(limit) else limit


def nothing_new(source):
    return {"source": source, "files": [], "records": 0, "candidate_records": 0}


def _cap_messages(source, stamp, content):
    feed = ET.fromstring(content)
    if feed.tag != f"{ATOM}feed":
        raise ValueError(f"{source}: expected an Atom feed")
    captures, messages = [], []
    for number, entry in enumerate(feed.findall(f"{ATOM}entry"), start=1):
        links = [link.get("href") for link in entry.findall(f"{ATOM}link")
                 if link.get("href", "").endswith("CAP.xml")]
        if len(links) != 1:
            raise ValueError(f"{source}: expected one CAP link for Atom entry {number}")
        cap = get_with_retries(links[0]).content
        captures.append(cap)
        messages.append((source, ET.fromstring(cap), entry, f"{source}_cap{number}_{stamp}.xml"))
    return captures, messages


def fetch_feed_if_new(source, stamp, connection):
    """Save a complete feed when it contains an unapplied observation or a rejection."""
    config = load_sources()[source]
    extension = FEED_EXTENSIONS[source]
    response = get_with_retries(config["url"])

    caps = []
    if extension == "geojson":
        features = response.json()["features"]
        if not isinstance(features, list):
            raise ValueError(f"{source}: expected a GeoJSON features list")
        event_type = "earthquake"
        frame, rejected = clean_realtime_features(features, f"{source}_{stamp}.geojson")
        records = len(features)
    else:
        event_type = "bulletin"
        caps, messages = _cap_messages(source, stamp, response.content)
        frame, rejected = clean_bulletin_messages(messages)
        records = len(caps)

    candidates = 0
    if not frame.empty:
        with connection.cursor() as cursor:
            previous = read_observations(cursor, source, event_type, frame["external_id"].tolist())
        for row in frame.to_dict("records"):
            change = observation_change(event_type, row, previous.get(row["external_id"]))
            candidates += change in {"new", "updated", "metadata"}

    if not candidates and not rejected:
        log.info("%s: no unapplied observations in the feed", source)
        return nothing_new(source)

    files = [save_raw(source, response.content, extension, stamp)]
    files += [save_raw(source, cap, "xml", stamp, suffix=f"_cap{number}")
              for number, cap in enumerate(caps, start=1)]
    log.info("%s: %d candidate(s) and %d rejected of %d", source, candidates, len(rejected), records)
    return {"source": source, "files": [str(path) for path in files], "records": records,
            "candidate_records": candidates}


def fetch_fdsn_updates(stamp, watermark):
    """Daily reconciliation: events revised since (watermark - overlap), including old ones
    that already left the real-time feed. The query needs an explicit starttime because USGS
    defaults to the last 30 days of origin time."""
    config = load_sources()["usgs_fdsn"]
    limit = _limit(watermark)
    if limit is None:
        log.warning("usgs_fdsn has no watermark yet: run the historical load first")
        return nothing_new("usgs_fdsn")
    since = (limit - timedelta(hours=config.get("overlap_hours", 24))).strftime("%Y-%m-%dT%H:%M:%S")
    query = dict(config["query"], starttime=config.get("reconcile_starttime", "2010-01-01"), updatedafter=since)
    expected = int(get_with_retries(config["count_url"], **query).text.strip() or 0)
    if expected == 0:
        log.info("usgs_fdsn: no event updated after %s", since)
        return nothing_new("usgs_fdsn")
    response = get_with_retries(config["url"], format="csv", **query)
    rows = max(len(response.text.strip().splitlines()) - 1, 0)
    if rows != expected:
        raise RuntimeError(f"usgs_fdsn reconcile: got {rows} rows, count method says {expected}")
    path = save_raw("usgs_fdsn", response.content, "csv", stamp, suffix="_reconcile")
    log.info("usgs_fdsn: %d event(s) updated after %s", rows, since)
    return {"source": "usgs_fdsn", "files": [str(path)], "records": rows, "candidate_records": rows}


def publish_run(summaries, logical_date, run_id, tag, *, mode="incremental", period=None, notify_silver=True):
    """Validate the saved files and write the Bronze manifest, with a marker for Airflow by default.
    The marker is written only if every file is valid; source errors are raised at the end,
    after the sources that worked have been published."""
    stamp = logical_date.strftime(STAMP_FORMAT)
    summaries = [s for s in summaries if s]
    new = [s for s in summaries if s.get("files")]
    errors = [f"{s['source']}: {s['error']}" for s in summaries if s.get("error")]

    problems = [f"{path}: {problem}" for s in new for path in s["files"] if (problem := check_file(path))]
    manifest_path = None
    if new:
        manifest = {"run_id": run_id, "logical_date": logical_date.isoformat(), "mode": mode,
                    "sources": new, "problems": problems}
        if period is not None:
            manifest["period"] = period
        # The tag keeps names distinct when two DAGs share the same logical date
        manifest_path = bronze_dir() / f"bronze_manifest_{stamp}_{tag}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        if not problems and notify_silver:
            marker = bronze_dir() / MARKERS / manifest_path.name
            marker.parent.mkdir(exist_ok=True)
            marker.write_text(str(manifest_path), encoding="utf-8")
        log.info("manifest saved: %s", manifest_path)
    else:
        log.info("nothing new in this run")

    if errors or problems:
        raise RuntimeError(f"source errors: {errors}; file problems: {problems}")
    return str(manifest_path) if manifest_path else None
