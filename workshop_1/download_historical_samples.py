import json
import sys
from pathlib import Path

import requests

BASE = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1"
USGS = "https://earthquake.usgs.gov/fdsnws/event/1"
HEADERS = {"User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"}

SAMPLES = Path("workshop_1/samples")  # Run from the repository root
SAMPLES.mkdir(parents=True, exist_ok=True)
FORCE = "--force" in sys.argv

def get_raw(filename, url, **params):
    """Download the original response, or read the saved file if it exists."""
    path = SAMPLES / filename

    if FORCE or not path.exists():
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        path.write_bytes(response.content)

    return path.read_bytes()

def get_json(filename, url, **params):
    return json.loads(get_raw(filename, url, **params))

# Historical tsunami events from NOAA
EVENTS_URL = f"{BASE}/tsunamis/events"
PACIFIC = "80,81,82,83,84,85,86,87,88,89"
params = dict(minYear=2010, maxYear=2025, itemsPerPage=200, regionCode=PACIFIC)

page1 = get_json("noaa_tsunamis_pacific_2010_2025.json", EVENTS_URL, page=1, **params)
events = list(page1["items"])

noaa_bytes = (SAMPLES / "noaa_tsunamis_pacific_2010_2025.json").stat().st_size

for page in range(2, page1["totalPages"] + 1):
    filename = f"noaa_tsunamis_pacific_page{page}.json"
    data = get_json(filename, EVENTS_URL, page=page, **params)

    events.extend(data["items"])
    noaa_bytes += (SAMPLES / filename).stat().st_size

    print(page1["totalPages"])

assert len(events) == page1["totalItems"], "Missing or extra tsunami events"
assert len({event["id"] for event in events}) == len(events), "Duplicate tsunami event IDs"

print(f"NOAA 2010-2025: {len(events)} events, {noaa_bytes / 1024:.1f} KB")

# Sample of runup observations from the 2010 Chile tsunami
EVENT_ID = 4682

runups = get_json(
    f"noaa_runups_event_{EVENT_ID}_page1.json",
    f"{BASE}/tsunamis/runups",
    tsunamiEventId=EVENT_ID,
    itemsPerPage=200,
    page=1
)

print(f"NOAA runups for event {EVENT_ID}: {len(runups['items'])} of {runups['totalItems']} saved")

# NOAA code descriptions
for name in ["causes", "validity", "warning_statuses", "regions"]:
    url = f"{BASE}/descriptors/tsunami/{name.replace('_', '-')}"
    get_json(f"noaa_descriptor_{name}.json", url)

# Historical earthquake sample from USGS (2010-2011)
usgs_filters = dict(
    minmagnitude=6,
    minlatitude=-60,
    maxlatitude=65,
    minlongitude=100,
    maxlongitude=300
)

raw = get_raw(
    "usgs_earthquakes_2010_2011_m6_pacific.csv",
    f"{USGS}/query",
    format="csv",
    starttime="2010-01-01",
    endtime="2012-01-01",
    **usgs_filters
)

print(f"USGS historical sample: {len(raw.splitlines()) - 1} earthquakes")

# Estimate the complete USGS historical volume using yearly counts
usgs_total = 0

for year in range(2010, 2026):
    count = int(get_raw(
        f"usgs_count_{year}_m6_pacific.txt",
        f"{USGS}/count",
        starttime=f"{year}-01-01",
        endtime=f"{year}-12-31T23:59:59.999",
        **usgs_filters
    ))

    usgs_total += count
    print(f"USGS {year}: {count} earthquakes")

sample_rows = len(raw.splitlines()) - 1
estimated_kb = (len(raw) / sample_rows) * usgs_total / 1024

print(f"USGS 2010-2025 estimate: {usgs_total} earthquakes, {estimated_kb:.1f} KB of CSV")