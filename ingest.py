"""Command-line entry point of the Team 8 tsunami monitoring system.

    python ingest.py --mode historical [--start 2010-01-01 --end 2025-12-31]
    python ingest.py --mode incremental
    python ingest.py --mode status

Every mode downloads or inspects the raw landing zone (the Bronze layer,
datalake_bronze/ by default; set RAW_DIR to change it), following the
ingestion contract of the Workshop 1 report. The download code lives in src/
and is shared with the Airflow DAGs. Run it from the repository root.
"""

import argparse
import logging
import sys
from datetime import date, datetime, timezone

import requests

from src.downloads import bronze_dir
from src.sources import FEED_EXTENSIONS, SOURCE_IDS, fetch_feed, fetch_noaa_hazel, fetch_usgs_fdsn


def run_historical(start, end, stamp):
    fetch_noaa_hazel(start, end, stamp)
    fetch_usgs_fdsn(start, end, stamp)


def run_incremental(stamp):
    failed = []
    for source in FEED_EXTENSIONS:
        try:
            fetch_feed(source, stamp)
        except requests.RequestException as error:
            # One failed source does not stop the others
            logging.error("%s FAILED: %s", source, error)
            failed.append(source)
    return failed


def run_status():
    folder = bronze_dir()
    print(f"Raw landing zone: {folder.resolve()}")
    for source in SOURCE_IDS:
        files = sorted(folder.glob(f"{source}_*"))
        latest = files[-1].name if files else "none"
        print(f"  {source:<14} files: {len(files):>4} | latest: {latest}")
    print("Watermarks, run history and row counts need the database load (Silver DAG).")


def parse_args():
    parser = argparse.ArgumentParser(description="Tsunami monitoring ingestion (Team 8).")
    parser.add_argument("--mode", required=True, choices=["historical", "incremental", "status"])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2010, 1, 1),
                        help="historical mode only, YYYY-MM-DD (default 2010-01-01)")
    parser.add_argument("--end", type=date.fromisoformat, default=date(2025, 12, 31),
                        help="historical mode only, YYYY-MM-DD (default 2025-12-31)")
    args = parser.parse_args()
    if args.start > args.end:
        parser.error("--start must not be after --end")
    return args


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    if args.mode == "status":
        run_status()
        return 0

    logging.info("run %s UTC, mode %s", stamp, args.mode)
    try:
        if args.mode == "historical":
            run_historical(args.start, args.end, stamp)
            failed = []
        else:
            failed = run_incremental(stamp)
    except (requests.RequestException, RuntimeError) as error:
        logging.error("run failed: %s", error)
        return 1

    logging.info("raw files saved in %s", bronze_dir())
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
