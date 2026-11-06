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


def download_sample(name, url, extension):
    filename = f"{name}_{timestamp}.{extension}"
    output_path = OUTPUT_DIR / filename

    print(f"Downloading {name}...")

    request = Request(
        url,
        headers={
            "User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"
        },
    )

    with urlopen(request, timeout=30) as response:
        content = response.read()

    output_path.write_bytes(content)

    print(f"Saved: {output_path.name}")
    print(f"Size: {len(content)} bytes\n")


def main():
    print("Starting incremental source downloads...\n")

    for name, (url, extension) in SOURCES.items():
        try:
            download_sample(name, url, extension)
        except Exception as error:
            print(f"Error downloading {name}: {error}")

    print("Download process finished.")


if __name__ == "__main__":
    main()