"""Incremental ingestion (Workshop 3): read the watermarks, poll the sources and keep a
response only when it holds something newer than the stored watermark. Responses are saved
unchanged (Bronze); the per-record change detection is done later by the content hash in load.py."""

import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import timedelta

import pandas as pd

from src.bronze_checks import check_file
from src.config import load_sources
from src.downloads import bronze_dir, get_with_retries, save_raw
from src.sources import FEED_EXTENSIONS

log = logging.getLogger(__name__)

CAP = "{urn:oasis:names:tc:emergency:cap:1.2}"
STAMP_FORMAT = "%Y%m%d_%H%M%S"
MARKERS = "_new"


def read_watermarks(connection):
    """{source_id: watermark text or None} from ingestion_state."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT source_id, watermark FROM ingestion_state")
        return {source: watermark for source, watermark in cursor.fetchall()}


def to_utc(value):
    return pd.to_datetime(value, utc=True, errors="coerce")


def _limit(watermark):
    """Parsed watermark, or None when there is none yet (first run) or it cannot be read."""
    if not watermark:
        return None
    limit = to_utc(watermark)
    return None if pd.isna(limit) else limit


def count_new_features(features, watermark):
    """USGS feed: features whose 'updated' (epoch ms) is greater than the watermark."""
    limit = _limit(watermark)
    if limit is None:
        return len(features)
    limit_ms = limit.value // 1_000_000
    return sum(1 for feature in features if (feature["properties"].get("updated") or 0) > limit_ms)


def is_newer(sent, watermark):
    """CAP bulletins: 'sent' later than the watermark (unreadable times count as new)."""
    limit = _limit(watermark)
    return limit is None or pd.isna(sent) or sent > limit


def cap_sent(content):
    return to_utc(ET.fromstring(content).findtext(f"{CAP}sent"))


def nothing_new(source):
    return {"source": source, "files": [], "records": 0, "new_records": 0}


def fetch_feed_if_new(source, stamp, watermark):
    """Poll a feed; save it (and the CAP messages of NTWC/PTWC) only if something is newer."""
    config = load_sources()[source]
    extension = FEED_EXTENSIONS[source]
    response = get_with_retries(config["url"])

    if extension == "geojson":
        features = response.json()["features"]
        new = count_new_features(features, watermark)
        if new == 0:
            log.info("%s: nothing newer than the watermark %s", source, watermark)
            return nothing_new(source)
        files = [save_raw(source, response.content, extension, stamp)]
        records = len(features)
    else:
        cap_urls = re.findall(r'href="([^"]+CAP\.xml)"', response.text)
        caps = [get_with_retries(url) for url in cap_urls]
        new = sum(1 for cap in caps if is_newer(cap_sent(cap.content), watermark))
        if new == 0:
            log.info("%s: no bulletin sent after the watermark %s", source, watermark)
            return nothing_new(source)
        # Save the Atom feed and ALL its CAP messages in entry order (Silver pairs them by position)
        files = [save_raw(source, response.content, extension, stamp)]
        files += [save_raw(source, cap.content, "xml", stamp, suffix=f"_cap{number}")
                  for number, cap in enumerate(caps, start=1)]
        records = len(caps)

    log.info("%s: %d new record(s) of %d", source, new, records)
    return {"source": source, "files": [str(path) for path in files], "records": records, "new_records": new}


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
    return {"source": "usgs_fdsn", "files": [str(path)], "records": rows, "new_records": rows}


def publish_run(summaries, logical_date, run_id, tag):
    """Validate the saved files, write the Bronze manifest and the arrival marker for Silver.
    The marker is written only if every file is valid; source errors are raised at the end,
    after the sources that worked have been published."""
    stamp = logical_date.strftime(STAMP_FORMAT)
    summaries = [s for s in summaries if s]
    new = [s for s in summaries if s.get("files")]
    errors = [f"{s['source']}: {s['error']}" for s in summaries if s.get("error")]

    problems = [f"{path}: {problem}" for s in new for path in s["files"] if (problem := check_file(path))]
    manifest_path = None
    if new:
        manifest = {"run_id": run_id, "logical_date": logical_date.isoformat(), "mode": "incremental",
                    "sources": new, "problems": problems}
        # The tag keeps names distinct when two DAGs share the same logical date
        manifest_path = bronze_dir() / f"bronze_manifest_{stamp}_{tag}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        if not problems:
            marker = bronze_dir() / MARKERS / manifest_path.name
            marker.parent.mkdir(exist_ok=True)
            marker.write_text(str(manifest_path), encoding="utf-8")
        log.info("manifest saved: %s", manifest_path)
    else:
        log.info("nothing new in this run")

    if errors or problems:
        raise RuntimeError(f"source errors: {errors}; file problems: {problems}")
    return str(manifest_path) if manifest_path else None