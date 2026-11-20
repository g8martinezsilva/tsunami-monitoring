"""Exercise the CLI with recorded responses and an isolated PostgreSQL/PostGIS schema."""

import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import timedelta, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import psycopg2
import pyarrow.parquet as pq
from requests import RequestException, Response

import ingest
from src import incremental, sources
from src.config import load_sources
from test_workshop3_ingestion import FIXTURES, first_feature
from test_workshop3_postgresql import DSN, PostgreSQLTestCase

DAY = "2011-12-26"
HISTORICAL = ["historical", "--start", DAY, "--end", DAY]


class CLIArgumentTests(unittest.TestCase):
    def test_inverted_period_exits_with_usage_error(self):
        with (redirect_stderr(StringIO()),
              patch.object(ingest, "connect_database") as connect,
              self.assertRaises(SystemExit) as error):
            ingest.main(["--mode", "historical", "--start", "2025-01-02", "--end", "2025-01-01"])
        self.assertEqual(error.exception.code, 2)
        connect.assert_not_called()


class CLIIngestionTests(PostgreSQLTestCase):
    def setUp(self):
        super().setUp()
        self.migrate()
        scratch = Path(self.enterContext(TemporaryDirectory()))
        self.bronze, self.silver = scratch / "bronze", scratch / "silver"
        self.enterContext(patch.dict(os.environ, {"RAW_DIR": str(self.bronze), "SILVER_DIR": str(self.silver)}))
        self.enterContext(patch.object(ingest, "connect_database", side_effect=self.connect))
        self.enterContext(patch.object(sources, "get_with_retries", side_effect=self.respond))
        self.enterContext(patch.object(incremental, "get_with_retries", side_effect=self.respond))

        self.config = load_sources()
        self.feature = first_feature()
        self.calls = []
        self.catalog_count = 1
        self.failed_source = None
        csv = FIXTURES / "usgs_earthquakes_2010_2011_m6_pacific.csv"
        self.catalog = "\n".join(csv.read_text().splitlines()[:2]).encode() + b"\n"
        tsunami = {"id": 9001, "year": 2011, "month": 12, "day": 26, "hour": 4, "minute": 48, "second": 8.87,
                   "latitude": -16.198, "longitude": -173.862, "eqMagnitude": 6, "causeCode": 1, "eventValidity": 4}
        self.noaa = [tsunami, {**tsunami, "id": 9002, "day": 27}, {**tsunami, "id": 9003, "day": 25}]

    def connect(self):
        return psycopg2.connect(DSN, options=f"-c search_path={self.schema.string},public")

    def respond(self, url, **query):
        self.calls.append((url, query))
        if self.failed_source and url == self.config[self.failed_source]["url"]:
            raise RequestException("fixture source unavailable")

        fdsn = self.config["usgs_fdsn"]
        if url == self.config["noaa_hazel"]["url"]:
            data = {"items": self.noaa, "totalPages": 1, "totalItems": len(self.noaa)}
            content = json.dumps(data).encode()
        elif url == fdsn["count_url"]:
            content = b"0" if "updatedafter" in query else str(self.catalog_count).encode()
        elif url == fdsn["url"]:
            content = self.catalog
        elif url == self.config["usgs_realtime"]["url"]:
            content = json.dumps({"features": [self.feature]}).encode()
        elif url in {self.config["ntwc"]["url"], self.config["ptwc"]["url"]}:
            content = b'<feed xmlns="http://www.w3.org/2005/Atom" />'
        else:
            raise AssertionError(f"Unexpected fixture URL {url}")

        response = Response()
        response.status_code = 200
        response._content = content
        return response

    def cli(self, *args):
        return ingest.main(["--mode", *args])

    def test_historical_filters_the_dates_loads_and_matches_without_duplicates(self):
        for _ in range(2):
            self.assertEqual(self.cli(*HISTORICAL), 0)
        self.assertEqual(self.query("SELECT source, external_id FROM event ORDER BY source"),
                         [("noaa_hazel", "9001"), ("usgs_fdsn", "usp000jcna")])
        self.assertEqual(self.query("SELECT relation_type, review_status FROM event_relation"),
                         [("caused_by", "accepted")])
        self.assertEqual(self.query("SELECT count(*) FROM ingestion_run WHERE accepted_count = 1"), [(2,)])
        self.assertEqual(self.query("SELECT count(*) FROM ingestion_run WHERE unchanged_count = 1"), [(2,)])

        file = next(self.bronze.glob("noaa_hazel_*.json"))
        self.assertEqual(json.loads(file.read_text())["items"], self.noaa)
        count_url = self.config["usgs_fdsn"]["count_url"]
        for url, query in self.calls:
            if url == count_url:
                self.assertEqual(query["starttime"], DAY)
                self.assertEqual(query["endtime"], f"{DAY}T23:59:59.999")
        self.assertEqual(len(list(self.silver.glob("_manifests/silver_manifest_*.json"))), 2)
        self.assertFalse((self.bronze / "_new").exists())

    def test_short_historical_period_loads_a_partial_noaa_date_with_no_fdsn_events(self):
        self.catalog_count = 0
        self.noaa[0].update(hour=None, minute=None, second=None)
        self.assertEqual(self.cli(*HISTORICAL), 0)
        self.assertEqual(self.query('SELECT source, "timestamp", time_precision FROM event'),
                         [("noaa_hazel", None, "day")])
        parquet = next((self.silver / "tsunami_events").glob("*.parquet"))
        self.assertEqual(pq.read_schema(parquet).field("event_time").type.tz, "UTC")
        self.assertNotIn(self.config["usgs_fdsn"]["url"], [url for url, _ in self.calls])

    def test_incremental_replay_keeps_counts_and_watermarks_then_applies_revision(self):
        self.assertEqual(self.cli(*HISTORICAL), 0)
        initial_watermark = self.query("SELECT watermark::timestamptz FROM ingestion_state WHERE source_id = 'usgs_fdsn'")[0][0]
        self.calls.clear()
        self.assertEqual(self.cli("incremental"), 0)
        before = self.query("SELECT source_id, watermark, last_successful_run_id FROM ingestion_state ORDER BY source_id")
        event = self.query("SELECT event_id FROM event WHERE source = 'usgs_realtime'")[0]

        with self.assertLogs(ingest.log, level="INFO") as logs:
            self.assertEqual(self.cli("incremental"), 0)
        self.assertIn("No new or modified observations to load", "\n".join(logs.output))
        self.assertEqual(before, self.query("SELECT source_id, watermark, last_successful_run_id FROM ingestion_state ORDER BY source_id"))

        self.feature["properties"]["mag"] = 5.75
        self.feature["properties"]["updated"] += 3_600_000
        self.assertEqual(self.cli("incremental"), 0)
        self.assertEqual(self.query("SELECT count(*) FROM event"), [(3,)])
        self.assertEqual(self.query("SELECT event_id FROM event WHERE source = 'usgs_realtime'"), [event])
        self.assertEqual(self.query("SELECT magnitude::float FROM earthquake WHERE event_id = %s", event), [(5.75,)])
        self.assertEqual(self.query("""
            SELECT accepted_count, updated_count FROM ingestion_run
            WHERE source_id = 'usgs_realtime' ORDER BY run_id DESC
        """), [(0, 1), (1, 0)])
        fdsn_count = self.config["usgs_fdsn"]["count_url"]
        expected = (initial_watermark.astimezone(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
        self.assertEqual([q["updatedafter"] for url, q in self.calls if url == fdsn_count], [expected] * 3)
        self.assertNotIn(self.config["noaa_hazel"]["url"], [url for url, _ in self.calls])
        self.assertFalse((self.bronze / "_new").exists())

    def test_source_failure_returns_error_and_loads_other_sources(self):
        self.failed_source = "ntwc"
        with self.assertLogs(ingest.log, level="ERROR") as logs:
            self.assertEqual(self.cli("incremental"), 1)
        self.assertIn("ntwc: fixture source unavailable", "\n".join(logs.output))
        self.assertEqual(self.query("SELECT source FROM event"), [("usgs_realtime",)])
        self.assertEqual(self.query("SELECT source_id FROM ingestion_state"), [("usgs_realtime",)])

    def test_status_reports_postgresql_observations_watermarks_and_run_counts(self):
        self.assertEqual(self.cli("incremental"), 0)
        watermark = self.query("SELECT watermark FROM ingestion_state WHERE source_id = 'usgs_realtime'")[0][0]
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(self.cli("status"), 0)
        status = output.getvalue()
        self.assertIn(f"observations=1  watermark={watermark}", status)
        self.assertIn("(incremental, success)", status)
        self.assertIn("new=1  updated=0  unchanged=0  rejected=0", status)
        self.assertIn("Recorded run statuses: success=1", status)
        for source in self.config:
            self.assertIn(source, status)
        self.assertEqual(self.query("SELECT count(*) FROM ingestion_run"), [(1,)])
