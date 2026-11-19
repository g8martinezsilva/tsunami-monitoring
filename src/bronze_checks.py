"""Basic schema checks of saved Bronze files (shared by the Bronze DAGs)."""

import csv
import io
import json
import xml.etree.ElementTree as ET
from pathlib import Path

USGS_COLUMNS = {"time", "latitude", "longitude", "depth", "mag", "magType", "id", "updated"}


def check_file(path):
    """Basic schema check of a saved Bronze file; returns a problem description or None."""
    content = Path(path).read_bytes()
    if not content:
        return "empty file"
    if path.endswith(".json"):
        data = json.loads(content)
        if not {"items", "totalItems"} <= data.keys():
            return "HazEL JSON without items/totalItems"
    elif path.endswith(".geojson"):
        if "features" not in json.loads(content):
            return "GeoJSON without features"
    elif path.endswith(".csv"):
        header = next(csv.reader(io.StringIO(content.decode("utf-8"))), [])
        missing = USGS_COLUMNS - set(header)
        if missing:
            return f"CSV missing columns {sorted(missing)}"
    elif path.endswith(".xml"):
        ET.fromstring(content)  # raises on malformed XML
    return None
