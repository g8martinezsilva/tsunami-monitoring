import json
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import Counter

SAMPLES_DIR = Path(__file__).resolve().parent / "samples"

FIRST_TIMESTAMP = "20261107_050434"
SECOND_TIMESTAMP = "20261107_140301"

ATOM = {"atom": "http://www.w3.org/2005/Atom"}

def compare_usgs():
    print("\n========== USGS REAL-TIME ==========")

    first_file = SAMPLES_DIR / f"usgs_realtime_{FIRST_TIMESTAMP}.geojson"
    second_file = SAMPLES_DIR / f"usgs_realtime_{SECOND_TIMESTAMP}.geojson"

    with open(first_file, encoding="utf-8") as f:
        first = json.load(f)

    with open(second_file, encoding="utf-8") as f:
        second = json.load(f)

    first_events = {
        event["id"]: event
        for event in first["features"]
    }

    second_events = {
        event["id"]: event
        for event in second["features"]
    }

    first_ids = set(first_events)
    second_ids = set(second_events)

    new_ids = second_ids - first_ids
    removed_ids = first_ids - second_ids
    common_ids = first_ids & second_ids

    window_start = second["metadata"]["generated"] - 30 * 24 * 3600 * 1000
    aged_out = {i for i in removed_ids
                if first_events[i]["properties"]["time"] < window_start}
    print(f"Removed because older than 30 days: {len(aged_out)}")
    print(f"Removed for another reason: {len(removed_ids - aged_out)}")

    updated_ids = {
        event_id
        for event_id in common_ids
        if first_events[event_id]["properties"].get("updated")
        != second_events[event_id]["properties"].get("updated")
        or first_events[event_id]["properties"]
        != second_events[event_id]["properties"]
        or first_events[event_id]["geometry"]
        != second_events[event_id]["geometry"]
    }

    unchanged_ids = common_ids - updated_ids

    print(f"First snapshot: {len(first_events)} records")
    print(f"Second snapshot: {len(second_events)} records")
    print(f"New events: {len(new_ids)}")
    print(f"Updated events: {len(updated_ids)}")
    print(f"Unchanged events: {len(unchanged_ids)}")
    print(f"Removed from feed: {len(removed_ids)}")

    print("\nNew event examples:")
    for event_id in sorted(new_ids)[:5]:
        event = second_events[event_id]
        print(
            event_id,
            event["properties"].get("mag"),
            event["properties"].get("place")
        )
    print("Updated event examples:")

    for event_id in sorted(updated_ids)[:5]:
        old = first_events[event_id]["properties"]
        new = second_events[event_id]["properties"]
        changed = [k for k in new if old.get(k) != new.get(k)]
        print(event_id, "changed fields:", changed)
        for k in changed:
            print(f"   {k}: {old.get(k)} -> {new.get(k)}")

        print(
            event_id,
            "Magnitude:",
            old.get("mag"),
            "->",
            new.get("mag")
        )
        field_counts = Counter()
        
    for event_id in updated_ids:
        old = first_events[event_id]["properties"]
        new = second_events[event_id]["properties"]
        field_counts.update(k for k in new if old.get(k) != new.get(k))
    print("Fields changed across all updated events:", dict(field_counts))

def read_atom_entries(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    entries = {}

    for entry in root.findall("atom:entry", ATOM):
        event_id = entry.findtext(
            "atom:id",
            namespaces=ATOM
        )

        if not event_id:
            continue

        entries[event_id] = {
            "title": entry.findtext(
                "atom:title",
                namespaces=ATOM
            ),
            "updated": entry.findtext(
                "atom:updated",
                namespaces=ATOM
            ),
            "content": ET.tostring(
                entry,
                encoding="unicode"
            )
        }

    return entries

def compare_tsunami_feed(prefix):
    print(f"\n========== {prefix.upper()} ==========")

    first_file = SAMPLES_DIR / f"{prefix}_{FIRST_TIMESTAMP}.xml"
    second_file = SAMPLES_DIR / f"{prefix}_{SECOND_TIMESTAMP}.xml"

    first = read_atom_entries(first_file)
    second = read_atom_entries(second_file)

    first_ids = set(first)
    second_ids = set(second)

    new_ids = second_ids - first_ids
    removed_ids = first_ids - second_ids

    updated_ids = {
        event_id
        for event_id in first_ids & second_ids
        if first[event_id] != second[event_id]
    }

    print(f"First snapshot: {len(first)} messages")
    print(f"Second snapshot: {len(second)} messages")
    print(f"New messages: {len(new_ids)}")
    print(f"Updated messages: {len(updated_ids)}")
    print(f"Removed from feed: {len(removed_ids)}")

    for event_id in sorted(new_ids | updated_ids):
        print(
            "Event:",
            second[event_id]["title"]
        )

def main():
    print("INCREMENTAL SOURCE COMPARISON")
    print(f"First snapshot: {FIRST_TIMESTAMP}")
    print(f"Second snapshot: {SECOND_TIMESTAMP}")

    compare_usgs()
    compare_tsunami_feed("ntwc")
    compare_tsunami_feed("ptwc")

    print("\nComparison completed.")


if __name__ == "__main__":
    main()