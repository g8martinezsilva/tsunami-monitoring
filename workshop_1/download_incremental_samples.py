import re
from datetime import datetime, timezone
from pathlib import Path

import requests

HEADERS = {"User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"}
SAMPLES = Path("workshop_1/samples")  # Run from the repository root
SAMPLES.mkdir(parents=True, exist_ok=True)

# Use a timestamp to keep each snapshot for later comparison
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

SOURCES = {
    "usgs_realtime": (
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_month.geojson",
        "geojson",
    ),
    "ntwc": ("https://www.tsunami.gov/events/xml/PAAQAtom.xml", "xml"),
    "ptwc": ("https://www.tsunami.gov/events/xml/PHEBAtom.xml", "xml"),
}


def get_raw(url):
    """Download the original response and return its content and headers."""
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.content, response.headers


for name, (url, extension) in SOURCES.items():
    try:
        content, headers = get_raw(url)
        path = SAMPLES / f"{name}_{timestamp}.{extension}"
        path.write_bytes(content)
        print(f"Saved: {path.name} ({len(content)} bytes)")
        print(f"Last-Modified: {headers.get('Last-Modified')} | ETag: {headers.get('ETag')}")

        # Atom feeds may link to CAP messages with additional alert details
        if extension == "xml":
            cap_links = re.findall(r'href="([^"]+CAP\.xml)"', content.decode("utf-8"))
            print(f"CAP links found in {name}: {len(cap_links)}")

            for number, cap_url in enumerate(cap_links, start=1):
                cap_content, _ = get_raw(cap_url)
                cap_path = SAMPLES / f"{name}_cap{number}_{timestamp}.xml"
                cap_path.write_bytes(cap_content)
                print(f"Saved: {cap_path.name} ({len(cap_content)} bytes)")

    except requests.RequestException as error:
        print(f"Error downloading {name}: {error}")

    print()