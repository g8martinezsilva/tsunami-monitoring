"""Run the Bronze, Silver and PostgreSQL ingestion steps or inspect their state."""

import argparse
import logging
import os
import sys
from contextlib import closing
from datetime import date, datetime, timezone

import psycopg2

from src.incremental import STAMP_FORMAT, fetch_fdsn_updates, fetch_feed_if_new, publish_run, read_watermarks
from src.load import load_silver_run, require_workshop3_schema
from src.matching import match_events
from src.silver import process_bronze_run
from src.sources import FEED_EXTENSIONS, fetch_noaa_hazel, fetch_usgs_fdsn

log = logging.getLogger(__name__)


def connect_database():
    """Use the PostgreSQL environment variables documented in .env.example."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5433"),
        dbname=os.environ.get("POSTGRES_DB", "tsunami_monitoring"),
        user=os.environ.get("POSTGRES_USER", "tsunami"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        connect_timeout=5,
    )


def poll_incremental(connection, stamp):
    with connection:
        watermark = read_watermarks(connection).get("usgs_fdsn")
    summaries, errors = [], []
    for source in (*FEED_EXTENSIONS, "usgs_fdsn"):
        try:
            if source == "usgs_fdsn":
                summaries.append(fetch_fdsn_updates(stamp, watermark))
            else:
                with connection:
                    summaries.append(fetch_feed_if_new(source, stamp, connection))
        except Exception as error:
            errors.append(f"{source}: {error}")
    return summaries, errors


def run_ingestion(connection, args):
    with connection, connection.cursor() as cursor:
        require_workshop3_schema(cursor)

    now = datetime.now(timezone.utc)
    tag = f"cli_{now.microsecond:06d}"
    stamp = f"{now.strftime(STAMP_FORMAT)}_{tag}"
    period = None
    errors = []
    if args.mode == "historical":
        period = {"start": args.start.isoformat(), "end": args.end.isoformat()}
        summaries = [fetch_noaa_hazel(args.start, args.end, stamp), fetch_usgs_fdsn(args.start, args.end, stamp)]
    else:
        summaries, errors = poll_incremental(connection, stamp)

    # The CLI processes its own manifest directly. Airflow only reads manifests with arrival markers.
    manifest = publish_run(summaries, now, stamp, tag, mode=args.mode, period=period, notify_silver=False)
    if manifest:
        silver_run = process_bronze_run(manifest, os.environ.get("SILVER_DIR", "datalake_silver"), stamp)
        load_silver_run(connection, silver_run)
    elif not errors:
        log.info("No new or modified observations to load")

    # A no-change run can also finish matching work left after a previously committed load.
    if manifest or not errors:
        match_events(connection)
    if errors:
        raise RuntimeError("Source errors: " + ", ".join(errors))
    log.info("%s run finished", args.mode)


def run_status(connection):
    connection.set_session(readonly=True)
    with connection, connection.cursor() as cursor:
        cursor.execute("""
            SELECT s.source_id, s.name, i.watermark,
                   (SELECT count(*) FROM event e WHERE e.source = s.source_id) AS observations,
                   r.run_id, r.mode, r.status, r.started_at, r.finished_at,
                   r.accepted_count, r.updated_count, to_jsonb(r)->>'unchanged_count', r.rejected_count
            FROM source s
            LEFT JOIN ingestion_state i ON i.source_id = s.source_id
            LEFT JOIN LATERAL (
                SELECT * FROM ingestion_run
                WHERE source_id = s.source_id
                ORDER BY started_at DESC, run_id DESC
                LIMIT 1
            ) r ON true
            ORDER BY s.source_id
        """)
        sources = cursor.fetchall()
        cursor.execute("SELECT status, count(*) FROM ingestion_run GROUP BY status ORDER BY status")
        statuses = cursor.fetchall()

    for (source, name, watermark, count, run_id, mode, status, started, finished,
         new, updated, unchanged, rejected) in sources:
        print(f"{source} ({name})  observations={count}  watermark={watermark or 'none'}")
        if run_id is None:
            print("  Last run: none")
        else:
            ended = finished.isoformat() if finished is not None else "none"
            print(f"  Last run {run_id} ({mode}, {status})  started={started.isoformat()}  finished={ended}")
            unchanged = unchanged if unchanged is not None else "n/a"
            print(f"  new={new}  updated={updated}  unchanged={unchanged}  rejected={rejected}")
    print("Recorded run statuses: " + (", ".join(f"{status}={count}" for status, count in statuses) or "none"))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Tsunami monitoring ingestion (Team 8).")
    parser.add_argument("--mode", required=True, choices=["historical", "incremental", "status"])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2010, 1, 1),
                        help="historical mode only, YYYY-MM-DD (default 2010-01-01)")
    parser.add_argument("--end", type=date.fromisoformat, default=date(2025, 12, 31),
                        help="historical mode only, YYYY-MM-DD (default 2025-12-31)")
    args = parser.parse_args(argv)
    if args.start > args.end:
        parser.error("--start must not be after --end")
    return args


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        with closing(connect_database()) as connection:
            if args.mode == "status":
                run_status(connection)
            else:
                run_ingestion(connection, args)
    except Exception as error:
        log.error("%s failed: %s", args.mode, error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
