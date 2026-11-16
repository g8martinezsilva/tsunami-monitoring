"""Command-line entry point of the Team 8 tsunami monitoring system.

    python ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]
    python ingest.py --mode incremental
    python ingest.py --mode status

Workshop 1 stub: every mode downloads or inspects the raw landing zone
(the Bronze layer, datalake_bronze/ by default; set RAW_DIR to change it),
following the ingestion contract of the Workshop 1 report.
Parsing, validation and the database load are added in Workshops 2 and 3.
Run it from the repository root.
"""

import argparse
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

HEADERS = {"User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"}
RAW = Path(os.environ.get("RAW_DIR", "datalake_bronze"))  # Bronze layer; one place to change the folder
ATTEMPTS = 3  # Team policy: three attempts with increasing waits

HAZEL_EVENTS = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events"
USGS_FDSN = "https://earthquake.usgs.gov/fdsnws/event/1"
PACIFIC_REGIONS = "80,81,82,83,84,85,86,87,88,89"
USGS_RECTANGLE = dict(minlatitude=-60, maxlatitude=65, minlongitude=100, maxlongitude=300)

# Incremental sources: (URL, file extension)
FEEDS = {
    "usgs_realtime": (
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_month.geojson",
        "geojson",
    ),
    "ntwc": ("https://www.tsunami.gov/events/xml/PAAQAtom.xml", "xml"),
    "ptwc": ("https://www.tsunami.gov/events/xml/PHEBAtom.xml", "xml"),
}

SOURCES = ["noaa_hazel", "usgs_fdsn", *FEEDS]


def get_with_retries(url, **params):
    """GET a URL, retrying with exponential backoff (2 s, 4 s) before failing."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            if attempt == ATTEMPTS:
                raise
            wait = 2 ** attempt
            print(f"  attempt {attempt} failed ({error}); retrying in {wait} s")
            time.sleep(wait)


def save_raw(source, content, extension, stamp, suffix=""):
    """Store a response unchanged in the landing zone as source_YYYYMMDD_HHMMSS.ext."""
    RAW.mkdir(parents=True, exist_ok=True)
    path = RAW / f"{source}{suffix}_{stamp}.{extension}"
    path.write_bytes(content)
    print(f"  saved {path} ({len(content):,} bytes)")
    return path


def run_historical(start, end, stamp):
    # NOAA HazEL: paginated JSON, 200 events per page
    print(f"noaa_hazel: tsunami events {start.year}-{end.year}, regions 80-89")
    params = dict(minYear=start.year, maxYear=end.year, itemsPerPage=200, regionCode=PACIFIC_REGIONS)
    page, total_pages, events = 1, 1, 0
    while page <= total_pages:
        response = get_with_retries(HAZEL_EVENTS, page=page, **params)
        data = response.json()
        save_raw("noaa_hazel", response.content, "json", stamp, suffix=f"_page{page}")
        total_pages = data["totalPages"]
        events += len(data["items"])
        page += 1
    if events != data["totalItems"]:
        raise RuntimeError(f"noaa_hazel: got {events} events, expected {data['totalItems']}")
    print(f"  {events} events in {total_pages} page(s)")

    # USGS FDSN: one calendar year per request, checked against the count method
    print(f"usgs_fdsn: earthquakes M >= 6, {start} to {end}, one request per year")
    for year in range(start.year, end.year + 1):
        year_start = max(start, date(year, 1, 1))
        year_end = min(end, date(year, 12, 31))
        query = dict(starttime=year_start.isoformat(), endtime=f"{year_end.isoformat()}T23:59:59",
                     minmagnitude=6, **USGS_RECTANGLE)
        expected = int(get_with_retries(f"{USGS_FDSN}/count", **query).text)
        response = get_with_retries(f"{USGS_FDSN}/query", format="csv", **query)
        save_raw("usgs_fdsn", response.content, "csv", stamp, suffix=f"_{year}")
        rows = max(len(response.text.strip().splitlines()) - 1, 0)
        if rows != expected:
            raise RuntimeError(f"usgs_fdsn {year}: got {rows} rows, count method says {expected}")
        print(f"  {year}: {rows} events")


def run_incremental(stamp):
    failed = []
    for source, (url, extension) in FEEDS.items():
        print(f"{source}: {url}")
        try:
            response = get_with_retries(url)
            save_raw(source, response.content, extension, stamp)
            print(f"  Last-Modified: {response.headers.get('Last-Modified')}")
            # Each Atom entry links to its CAP message
            if extension == "xml":
                for number, cap_url in enumerate(re.findall(r'href="([^"]+CAP\.xml)"', response.text), start=1):
                    cap = get_with_retries(cap_url)
                    save_raw(source, cap.content, "xml", stamp, suffix=f"_cap{number}")
        except requests.RequestException as error:
            # One failed source does not stop the others
            print(f"  FAILED: {error}")
            failed.append(source)
    return failed


def run_status():
    print(f"Raw landing zone: {RAW.resolve()}")
    for source in SOURCES:
        files = sorted(RAW.glob(f"{source}_*"))
        latest = files[-1].name if files else "none"
        print(f"  {source:<14} files: {len(files):>4} | latest: {latest}")
    print("Watermarks, run history and row counts need the database (Workshop 2).")


def parse_args():
    parser = argparse.ArgumentParser(description="Tsunami monitoring ingestion (Team 8).")
    parser.add_argument("--mode", required=True, choices=["historical", "incremental", "status"])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2010, 1, 1),
                        help="historical mode only, YYYY-MM-DD (default 2010-01-01)")
    parser.add_argument("--end", type=date.fromisoformat, default=date(2025, 12, 31),
                        help="historical mode only, YYYY-MM-DD (default 2025-12-31)")
    args = parser.parse_args()
    if args.start > args.end:
        parser.error("--start must not be after --end")
    return args


def main():
    args = parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    if args.mode == "status":
        run_status()
        return 0

    print(f"Run {stamp} UTC, mode {args.mode}")
    try:
        if args.mode == "historical":
            run_historical(args.start, args.end, stamp)
            failed = []
        else:
            failed = run_incremental(stamp)
    except (requests.RequestException, RuntimeError) as error:
        print(f"Run failed: {error}")
        return 1

    print("Raw files saved. Parsing, validation and the database load come in Workshops 2 and 3.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
