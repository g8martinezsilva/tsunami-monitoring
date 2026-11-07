import argparse
import csv
import io
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSHOP_DIR = PROJECT_ROOT / "workshop_1"
OUTPUT_DIR = WORKSHOP_DIR / "samples"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NOAA_EVENTS_URL = (
    "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events"
)
NOAA_RUNUPS_URL = (
    "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/runups"
)

HAZEL_BASE_URL = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1"
NOAA_DESCRIPTOR_URLS = {
    "causes": f"{HAZEL_BASE_URL}/descriptors/tsunami/causes",
    "validity": f"{HAZEL_BASE_URL}/descriptors/tsunami/validity",
    "warning_statuses": f"{HAZEL_BASE_URL}/descriptors/tsunami/warning-statuses",
    "regions": f"{HAZEL_BASE_URL}/descriptors/tsunami/regions",
}

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

USGS_COUNT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/count"

HEADERS = {"User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"}

NOAA_START_YEAR = 2010
NOAA_END_YEAR = 2025
PAGE_SIZE = 200
PACIFIC_REGION_CODES = "80,81,82,83,84,85,86,87,88,89"  # NOAA HazEL regions
PACIFIC_BOX = {"minlatitude": -60, "maxlatitude": 65,
               "minlongitude": 100, "maxlongitude": 300}

def fetch_data(url, params, retries=3):
    """Make an HTTP GET request, retrying temporary request failures."""
    full_url = f"{url}?{urlencode(params)}" if params else url

    for attempt in range(1, retries + 1):
        try:
            request = Request(full_url, headers=HEADERS)
            with urlopen(request, timeout=30) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError) as error:
            print(f"Attempt {attempt}/{retries} failed: {error}")
            if attempt == retries:
                raise
            time.sleep(attempt * 2)

def get_or_download(filename, url, params, force=False):
    """Return original response bytes from a cached file or the API."""
    output_path = OUTPUT_DIR / filename
    if output_path.exists() and not force:
        print(f"Using existing file: {filename}")
        return output_path.read_bytes()

    print(f"Downloading: {filename}")
    content = fetch_data(url, params)
    output_path.write_bytes(content)
    print(f"Saved: {filename} ({len(content)} bytes)")
    return content

def download_noaa_events(force=False):
    """Read all pages of Pacific NOAA tsunami events for 2010-2025."""
    print("\nNOAA HISTORICAL TSUNAMI EVENTS")
    base_params = {
        "minYear": NOAA_START_YEAR,
        "maxYear": NOAA_END_YEAR,
        "itemsPerPage": PAGE_SIZE,
        "regionCode": PACIFIC_REGION_CODES,
    }
    first_content = get_or_download(
        "noaa_tsunamis_pacific_2010_2025.json",
        NOAA_EVENTS_URL,
        {**base_params, "page": 1},
        force,
    )
    first_page = json.loads(first_content)
    total_pages = first_page["totalPages"]
    total_items = first_page["totalItems"]
    if not isinstance(total_pages, int) or total_pages < 1:
        raise ValueError("Invalid NOAA events page count")

    events = []
    for page in range(1, total_pages + 1):
        if page == 1:
            data = first_page
        else:
            content = get_or_download(
                f"noaa_tsunamis_pacific_page{page}.json",
                NOAA_EVENTS_URL,
                {**base_params, "page": page},
                force,
            )
            data = json.loads(content)

        if data.get("page") != page:
            raise ValueError(f"Unexpected NOAA events page: {data.get('page')}")
        page_events = data["items"]
        events.extend(page_events)
        print(f"Page {page}: {len(page_events)} records")

    ids = [event["id"] for event in events]
    if len(events) != total_items:
        raise ValueError("NOAA events record count mismatch")
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate NOAA event IDs detected")
    if any(not (NOAA_START_YEAR <= event["year"] <= NOAA_END_YEAR) for event in events):
        raise ValueError("NOAA returned an event outside the requested year range")

    print(f"NOAA events: {len(events)} unique records")

    # Keep a small, year-specific response as additional evidence.
    sample_2011 = json.loads(
        get_or_download(
            "noaa_tsunamis_pacific_2011.json",
            NOAA_EVENTS_URL,
            {"minYear": 2011, "maxYear": 2011, "itemsPerPage": PAGE_SIZE,
             "regionCode": PACIFIC_REGION_CODES},
            force,
        )
    )
    print(f"NOAA Pacific 2011 sample: {len(sample_2011['items'])} records")
    return events

def download_noaa_runup_sample(events, event_id=4682, force=False):
    """Retrieve ONE PAGE of runups for one known event; not the full archive."""
    print("\nNOAA RUNUP SAMPLE")
    selected_event = next((event for event in events if event["id"] == event_id), None)
    if selected_event is None:
        raise ValueError(f"NOAA tsunami event {event_id} is not in the event sample")
    if (selected_event.get("numRunups") or 0) == 0:
        raise ValueError(f"NOAA tsunami event {event_id} has no reported runups")

    filename = f"noaa_runups_event_{event_id}_page1.json"
    params = {"tsunamiEventId": event_id, "itemsPerPage": PAGE_SIZE, "page": 1}
    data = json.loads(get_or_download(filename, NOAA_RUNUPS_URL, params, force))
    items = data.get("items", [])
    if not items:
        raise ValueError(f"No runup observations returned for event {event_id}")
    if any(item.get("tsunamiEventId") != event_id for item in items):
        raise ValueError("NOAA runup filter returned observations for other events")
    ids = [item["id"] for item in items]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate runup IDs found in sample")

    total = data.get("totalItems")
    print(f"Tsunami event: {event_id}")
    print(f"Runups reported by event: {selected_event.get('numRunups')}")
    print(f"Runups reported by query: {total}")
    print(f"Runups saved (first page): {len(items)}")
    return {"event_id": event_id, "count": len(items), "total": total, "filename": filename}

def download_noaa_descriptors(events, force=False):
    """Download NOAA code tables and print how many events use each code."""
    print("NOAA DESCRIPTORS")
    fields = {
        "causes": "causeCode",
        "validity": "eventValidity",
        "warning_statuses": "warningStatusId",
        "regions": "regionCode",
    }
    descriptors = {}
    for name, url in NOAA_DESCRIPTOR_URLS.items():
        content = get_or_download(f"noaa_descriptor_{name}.json", url, None, force)
        items = json.loads(content).get("items", [])
        if not items:
            raise ValueError(f"NOAA descriptor '{name}' is empty")
        # The API returns ids as strings ("1"); events use integers (1)
        table = {int(item["id"]): item["description"] for item in items}
        descriptors[name] = table

        counts = {}
        for event in events:
            code = event.get(fields[name])
            counts[code] = counts.get(code, 0) + 1

        print(f"{fields[name]} ({len(table)} codes):")
        for code, count in sorted(counts.items(), key=lambda item: -item[1]):
            label = "(missing)" if code is None else table.get(code, "UNKNOWN CODE")
            print(f"  {code}: {label} -> {count}")
    return descriptors

def download_usgs(force=False):
    """Download Pacific USGS earthquakes of magnitude >= 6 in 2010-2011."""

    print("\nUSGS HISTORICAL EARTHQUAKES")

    params = {
        "format": "csv",
        "starttime": "2010-01-01",
        "endtime": "2012-01-01",
        "minmagnitude": 6,
        **PACIFIC_BOX,
    }

    filename = "usgs_earthquakes_2010_2011_m6_pacific.csv"

    content = get_or_download(
        filename,
        USGS_URL,
        params,
        force
    )

    reader = csv.DictReader(
        io.StringIO(content.decode("utf-8-sig"))
    )

    records = list(reader)

    if not records:
        raise ValueError("USGS returned no earthquake records")

    required = {
        "id",
        "time",
        "latitude",
        "longitude",
        "depth",
        "mag"
    }

    if not required.issubset(reader.fieldnames or []):
        raise ValueError("Missing required columns in USGS CSV")

    ids = [record["id"] for record in records]

    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate USGS earthquake IDs detected")

    # Compare against the count method (same filters, no format parameter)
    count_params = {k: v for k, v in params.items() if k != "format"}
    expected = int(fetch_data(USGS_COUNT_URL, count_params).decode().strip())
    if len(records) != expected:
        raise ValueError(
            f"USGS count mismatch: expected {expected}, downloaded {len(records)}"
        )
    print(f"Count check passed: {expected}")

    # Validate magnitudes and dates
    for record in records:

        magnitude = float(record["mag"])

        if magnitude < 6:
            raise ValueError(
                f"Invalid magnitude for event {record['id']}"
            )

        if not ("2010-01-01" <= record["time"][:10] < "2012-01-01"):
            raise ValueError(
                f"Event outside 2010-2011: {record['id']}"
            )

    print(f"USGS records: {len(records)}")
    print(f"Unique identifiers: {len(set(ids))}")

    print("USGS historical data validated successfully.")

    return records

def main():
    parser = argparse.ArgumentParser(
        description="Download and validate Workshop 1 historical data samples"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Refresh cached raw sample files"
    )

    parser.add_argument(
        "--runup-event-id",
        type=int,
        default=4682,
        help="NOAA tsunami event used to fetch one page of runup samples"
    )

    args = parser.parse_args()

    print("Starting Workshop 1 historical source exploration...")

    # 1. NOAA historical tsunami events
    noaa_events = download_noaa_events(force=args.force)

    # 2. NOAA descriptors
    download_noaa_descriptors(noaa_events, force=args.force)

    # 3. NOAA runup observations
    runup_sample = download_noaa_runup_sample(
        noaa_events,
        event_id=args.runup_event_id,
        force=args.force
    )

    # 4. USGS historical earthquakes
    usgs_records = download_usgs(force=args.force)

    # 5. Summary
    print("\nHISTORICAL SOURCE SUMMARY")

    print(f"NOAA tsunami events: {len(noaa_events)}")

    print(
        f"NOAA runups saved for event {runup_sample['event_id']}: "
        f"{runup_sample['count']} of {runup_sample['total']}"
    )

    print(f"USGS Pacific earthquakes (2010-2011, M>=6): {len(usgs_records)}")

    print("\nHistorical source exploration completed successfully.")


if __name__ == "__main__":
    main()
