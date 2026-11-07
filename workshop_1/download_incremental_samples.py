import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "workshop_1" / "samples"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Timestamp in UTC
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# Data sources
SOURCES = {
    "usgs_realtime": (
        "https://earthquake.usgs.gov/earthquakes/feed/"
        "v1.0/summary/4.5_month.geojson",
        "geojson",
    ),
    "ntwc": (
        "https://www.tsunami.gov/events/xml/PAAQAtom.xml",
        "xml",
    ),
    "ptwc": (
        "https://www.tsunami.gov/events/xml/PHEBAtom.xml",
        "xml",
    ),
}

def fetch(url):
    """Return the response body and headers of an HTTP GET request."""
    request = Request(
        url,
        headers={
            "User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"
        },
    )

    with urlopen(request, timeout=30) as response:
        return response.read(), response.headers

def download_sample(name, url, extension):
    print(f"Downloading {name}...")

    content, headers = fetch(url)

    output_path = OUTPUT_DIR / f"{name}_{timestamp}.{extension}"
    output_path.write_bytes(content)

    print(f"Saved: {output_path.name}")
    print(f"Size: {len(content)} bytes")
    print(
        f"Last-Modified: {headers.get('Last-Modified')} | "
        f"ETag: {headers.get('ETag')}"
    )

    # Each Atom entry links to a CAP message with the alert details
    if extension == "xml":
        text = content.decode("utf-8", "replace")
        cap_links = re.findall(r'href="([^"]+CAP\.xml)"', text)

        for number, link in enumerate(cap_links, start=1):
            cap_content, _ = fetch(link)
            cap_path = OUTPUT_DIR / f"{name}_cap{number}_{timestamp}.xml"
            cap_path.write_bytes(cap_content)
            print(f"Saved: {cap_path.name} ({len(cap_content)} bytes)")

    print()

def main():
    print("Starting incremental source downloads...")

    for name, (url, extension) in SOURCES.items():
        try:
            download_sample(name, url, extension)
        except Exception as error:
            print(f"Error downloading {name}: {error}")

    print("Download process finished.")

if __name__ == "__main__":
    main()
