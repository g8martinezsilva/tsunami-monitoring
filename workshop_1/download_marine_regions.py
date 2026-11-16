import json
import sys
from pathlib import Path

import requests

# Marine Regions, Global Oceans and Seas v1 (Flanders Marine Institute, 2021),
# CC BY 4.0, https://doi.org/10.14284/542. Served by the VLIZ WFS.
WFS = "https://geo.vliz.be/geoserver/MarineRegions/wfs"
HEADERS = {"User-Agent": "UDistrital-Team8-Tsunami-Monitoring/1.0"}

# "Easter" is the literal name stored in the dataset (published name: "Eastern")
REGIONS = [
    "North Pacific Ocean",
    "South Pacific Ocean",
    "South China and Easter Archipelagic Seas",
]

OUTPUT = Path("config/geographic_reference/pacific_maritime_regions.geojson")  # Run from the repository root
FORCE = "--force" in sys.argv

if OUTPUT.exists() and not FORCE:
    print(f"{OUTPUT} already exists; use --force to download it again.")
    sys.exit(0)

names = ", ".join(f"'{name}'" for name in REGIONS)
params = {
    "service": "WFS",
    "version": "1.0.0",  # WFS 1.0.0 keeps the longitude, latitude axis order
    "request": "GetFeature",
    "typeName": "MarineRegions:goas",
    "outputFormat": "application/json",
    "CQL_FILTER": f"name IN ({names})",
}

print("Downloading 3 Marine Regions polygons (about 85 MB, several minutes)...")
response = requests.get(WFS, params=params, headers=HEADERS, timeout=900)
response.raise_for_status()

data = json.loads(response.content)
found = sorted(feature["properties"]["name"] for feature in data["features"])
assert found == sorted(REGIONS), f"Unexpected regions: {found}"

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_bytes(response.content)
print(f"Saved: {OUTPUT} ({len(response.content) / 1e6:.1f} MB, {len(found)} regions)")
