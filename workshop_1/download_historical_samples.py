import json
import sys
from collections import Counter
from pathlib import Path

import requests

BASE = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1"       # NOAA API base URL
HEADERS = {"User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"}  # Identify the team in requests
SAMPLES = Path("workshop_1/samples")
SAMPLES.mkdir(parents=True, exist_ok=True)
FORCE = "--force" in sys.argv


def get_raw(filename, url, **params):
    """Request data and save the original response bytes in samples/.
    If the file already exists, read it from disk instead of downloading it again."""
    path = SAMPLES / filename
    if FORCE or not path.exists():
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()         # Stop if the response status is not successful (4xx/5xx)
        path.write_bytes(r.content)  # Save the original response without changing it
    return path.read_bytes()


def get_json(filename, url, **params):
    return json.loads(get_raw(filename, url, **params))


EVENTS_URL = f"{BASE}/tsunamis/events"
PACIFIC = "80,81,82,83,84,85,86,87,88,89"  # NOAA regionCode values for the Pacific
params = dict(minYear=2010, maxYear=2025, itemsPerPage=200, regionCode=PACIFIC)

page1 = get_json("noaa_tsunamis_pacific_2010_2025.json", EVENTS_URL, page=1, **params)
events = list(page1["items"])                  # Start with the events from the first page
for p in range(2, page1["totalPages"] + 1):    # Download any remaining pages
    events += get_json(f"noaa_tsunamis_pacific_page{p}.json", EVENTS_URL, page=p, **params)["items"]

# Validations:
assert len(events) == page1["totalItems"], "faltan o sobran eventos"       # Check that all events were downloaded
assert len({e["id"] for e in events}) == len(events), "hay ids repetidos"  # Check for repeated event IDs
print(len(events), "eventos")

# Extra 2011 sample for the report
get_json("noaa_tsunamis_pacific_2011.json", EVENTS_URL,
         minYear=2011, maxYear=2011, itemsPerPage=200, regionCode=PACIFIC)

RUNUPS_URL = f"{BASE}/tsunamis/runups"
EVENT_ID = 4682
by_id = {e["id"]: e for e in events}  # Look up an event by its ID

runups = get_json(f"noaa_runups_event_{EVENT_ID}_page1.json", RUNUPS_URL,
                  tsunamiEventId=EVENT_ID, itemsPerPage=200, page=1)

# Compare the event summary with the runups endpoint
print("según el evento:", by_id[EVENT_ID]["numRunups"],
      "| según el endpoint runups:", runups["totalItems"],
      "| guardados en esta página:", len(runups["items"]))

DESCRIPTORS = {  # Descriptor name: corresponding event field
    "causes": "causeCode",
    "validity": "eventValidity",
    "warning_statuses": "warningStatusId",
    "regions": "regionCode",
}
for name, field in DESCRIPTORS.items():
    url = f"{BASE}/descriptors/tsunami/{name.replace('_', '-')}"  # Use hyphens in the endpoint URL
    items = get_json(f"noaa_descriptor_{name}.json", url)["items"]
    labels = {int(i["id"]): i["description"] for i in items}  # Descriptor IDs are strings; event codes are integers

    print(f"\n{field}:")
    for code, n in Counter(e.get(field) for e in events).most_common():
        print(f"  {code}: {labels.get(code, '(missing)' if code is None else 'UNKNOWN CODE')} -> {n}")

USGS = "https://earthquake.usgs.gov/fdsnws/event/1"
usgs_params = dict(starttime="2010-01-01", endtime="2012-01-01", minmagnitude=6,
                   minlatitude=-60, maxlatitude=65,
                   minlongitude=100, maxlongitude=300)  # Longitude over 180 crosses the date line

# Request CSV explicitly; the default USGS format is QuakeML (XML)
raw = get_raw("usgs_earthquakes_2010_2011_m6_pacific.csv", f"{USGS}/query", format="csv", **usgs_params)