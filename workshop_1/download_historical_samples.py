from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
import json
import csv
import io
import time
import argparse


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "workshop_1" / "samples"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NOAA_URL = (
    "https://www.ngdc.noaa.gov/hazel/hazard-service/"
    "api/v1/tsunamis/events"
)

USGS_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
)

HEADERS = {
    "User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"
}

# HTTP REQUEST
def fetch_data(url, params, retries=3):
    """Fetch data from an API with retries."""

    full_url = f"{url}?{urlencode(params)}"

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

# FILE HANDLING
def get_or_download(filename, url, params, force=False):
    """Reuse an existing sample or download it."""

    output_path = OUTPUT_DIR / filename

    if output_path.exists() and not force:
        print(f"Using existing file: {filename}")
        return output_path.read_bytes()

    print(f"Downloading: {filename}")

    content = fetch_data(url, params)

    output_path.write_bytes(content)

    print(f"Saved: {filename}")
    print(f"Size: {len(content)} bytes")

    return content


# NOAA HISTORICAL TSUNAMIS
def download_noaa(force=False):
    """Download historical tsunami records with pagination."""

    print("\n=== NOAA HISTORICAL TSUNAMIS ===")

    base_params = {
        "minYear": 2010,
        "maxYear": 2025,
        "itemsPerPage": 200,
    }

    all_events = []
    all_ids = []

    # First page
    params = {**base_params, "page": 1}

    content = get_or_download(
        "noaa_tsunamis_2010_2025.json",
        NOAA_URL,
        params,
        force,
    )

    first_page = json.loads(content)

    total_pages = first_page["totalPages"]
    total_items = first_page["totalItems"]

    print(f"Total pages: {total_pages}")
    print(f"Expected records: {total_items}")

    for page in range(1, total_pages + 1):

        if page == 1:
            data = first_page

        else:
            params = {**base_params, "page": page}

            content = get_or_download(
                f"noaa_tsunamis_page{page}.json",
                NOAA_URL,
                params,
                force,
            )

            data = json.loads(content)

        if data["page"] != page:
            raise ValueError(
                f"Unexpected NOAA page: {data['page']}"
            )

        events = data["items"]

        all_events.extend(events)
        all_ids.extend(event["id"] for event in events)

        print(f"Page {page}: {len(events)} records")

    # Validation
    unique_ids = len(set(all_ids))

    print("\nNOAA validation:")
    print(f"Expected records: {total_items}")
    print(f"Downloaded records: {len(all_events)}")
    print(f"Unique identifiers: {unique_ids}")

    if len(all_events) != total_items:
        raise ValueError("NOAA record count mismatch")

    if unique_ids != len(all_events):
        raise ValueError("Duplicate NOAA identifiers detected")

    years = [
        event["year"]
        for event in all_events
        if event.get("year") is not None
    ]

    if years:
        print(f"Historical coverage: {min(years)}-{max(years)}")

    print("NOAA historical download validated successfully.")

    # Additional 2011 sample
    params_2011 = {
        "minYear": 2011,
        "maxYear": 2011,
        "itemsPerPage": 200,
    }

    content = get_or_download(
        "noaa_tsunamis_2011.json",
        NOAA_URL,
        params_2011,
        force,
    )

    sample_2011 = json.loads(content)

    print(f"NOAA 2011 sample: {len(sample_2011['items'])} records")

    return all_events

# USGS HISTORICAL EARTHQUAKES
def download_usgs(force=False):
    """Download a historical earthquake sample."""

    print("\n=== USGS HISTORICAL EARTHQUAKES ===")

    params = {
        "format": "csv",
        "starttime": "2011-01-01",
        "endtime": "2011-12-31",
        "minmagnitude": 6,
        "limit": 200,
    }

    content = get_or_download(
        "usgs_earthquakes_2011.csv",
        USGS_URL,
        params,
        force,
    )

    # Parse CSV
    text = content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))

    records = list(reader)

    if not records:
        raise ValueError("USGS returned no earthquake records")

    required_fields = {
        "id", "time", "latitude",
        "longitude", "depth", "mag"
    }

    if not required_fields.issubset(reader.fieldnames or []):
        raise ValueError("Missing required USGS fields")

    ids = [record["id"] for record in records]

    print("\nUSGS validation:")
    print(f"Downloaded records: {len(records)}")
    print(f"Unique identifiers: {len(set(ids))}")

    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate USGS identifiers detected")

    print("USGS historical sample validated successfully.")

    return records

def main():

    parser = argparse.ArgumentParser(
        description="Download historical data samples for Workshop 1"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Download files again even if they already exist"
    )

    args = parser.parse_args()

    print("Starting historical data source downloads...")

    noaa_records = download_noaa(force=args.force)
    usgs_records = download_usgs(force=args.force)

    print("HISTORICAL DOWNLOAD SUMMARY")

    print(f"NOAA tsunami records: {len(noaa_records)}")
    print(f"USGS earthquake sample: {len(usgs_records)}")

    print("\nHistorical download process completed.")


if __name__ == "__main__":
    main()