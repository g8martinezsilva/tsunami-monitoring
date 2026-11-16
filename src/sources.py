"""Download functions for the four sources. Each one saves the raw responses in the Bronze
layer and returns a summary: {"source", "files", "records"}, used for validation and logging.
URLs and query parameters come from config/sources.yaml."""

import logging
import re
from datetime import date

from src.config import load_sources
from src.downloads import get_with_retries, save_raw

log = logging.getLogger(__name__)

FEED_EXTENSIONS = {"usgs_realtime": "geojson", "ntwc": "xml", "ptwc": "xml"}
SOURCE_IDS = ["noaa_hazel", "usgs_fdsn", *FEED_EXTENSIONS]


def fetch_noaa_hazel(start, end, stamp):
    """Download the HazEL tsunami events of [start.year, end.year], page by page."""
    config = load_sources()["noaa_hazel"]
    params = dict(config["query"], minYear=start.year, maxYear=end.year)
    page, total_pages, events, files = 1, 1, 0, []
    while page <= total_pages:
        response = get_with_retries(config["url"], page=page, **params)
        data = response.json()
        files.append(save_raw("noaa_hazel", response.content, "json", stamp, suffix=f"_page{page}"))
        total_pages = data["totalPages"]
        events += len(data["items"])
        page += 1
    if events != data["totalItems"]:
        raise RuntimeError(f"noaa_hazel: got {events} events, expected {data['totalItems']}")
    log.info("noaa_hazel: %d events in %d page(s)", events, total_pages)
    return {"source": "noaa_hazel", "files": [str(path) for path in files], "records": events}


def fetch_usgs_fdsn_year(year, start, end, stamp):
    """Download one calendar year (clipped to [start, end]) and check it against the count method."""
    config = load_sources()["usgs_fdsn"]
    query = dict(config["query"],
                 starttime=max(start, date(year, 1, 1)).isoformat(),
                 endtime=f"{min(end, date(year, 12, 31)).isoformat()}T23:59:59")
    expected = int(get_with_retries(config["count_url"], **query).text)
    response = get_with_retries(config["url"], format="csv", **query)
    path = save_raw("usgs_fdsn", response.content, "csv", stamp, suffix=f"_{year}")
    rows = max(len(response.text.strip().splitlines()) - 1, 0)
    if rows != expected:
        raise RuntimeError(f"usgs_fdsn {year}: got {rows} rows, count method says {expected}")
    log.info("usgs_fdsn %d: %d events", year, rows)
    return {"source": "usgs_fdsn", "files": [str(path)], "records": rows}


def fetch_usgs_fdsn(start, end, stamp):
    """Download [start, end] one calendar year per request."""
    years = [fetch_usgs_fdsn_year(year, start, end, stamp) for year in range(start.year, end.year + 1)]
    return {"source": "usgs_fdsn",
            "files": [path for year in years for path in year["files"]],
            "records": sum(year["records"] for year in years)}


def fetch_feed(source, stamp):
    """Download an incremental feed; for NTWC/PTWC also the CAP message linked by each Atom entry."""
    config = load_sources()[source]
    extension = FEED_EXTENSIONS[source]
    response = get_with_retries(config["url"])
    files = [save_raw(source, response.content, extension, stamp)]
    log.info("%s: Last-Modified %s", source, response.headers.get("Last-Modified"))
    if extension == "geojson":
        records = len(response.json()["features"])
    else:
        cap_urls = re.findall(r'href="([^"]+CAP\.xml)"', response.text)
        for number, cap_url in enumerate(cap_urls, start=1):
            cap = get_with_retries(cap_url)
            files.append(save_raw(source, cap.content, "xml", stamp, suffix=f"_cap{number}"))
        records = len(cap_urls)
    return {"source": source, "files": [str(path) for path in files], "records": records}
