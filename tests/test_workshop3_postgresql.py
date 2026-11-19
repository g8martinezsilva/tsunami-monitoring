"""Run with TSUNAMI_TEST_DSN pointing to a disposable PostgreSQL/PostGIS database."""

import os
import unittest
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import psycopg2
from psycopg2 import sql

from src import load, silver
from test_workshop3_ingestion import FIXTURES, first_feature, silver_manifest

ROOT = Path(__file__).resolve().parents[1]
DSN = os.environ.get("TSUNAMI_TEST_DSN")


@unittest.skipUnless(DSN, "set TSUNAMI_TEST_DSN to a disposable PostgreSQL/PostGIS database")
class PostgreSQLTests(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg2.connect(DSN)
        self.addCleanup(self.connection.close)
        self.schema = sql.Identifier(f"tsunami_test_{uuid4().hex}")
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(self.schema))
        self.addCleanup(self.drop_test_schema)
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute(sql.SQL("SET search_path TO {}, public").format(self.schema))
            for filename in ("01_schema.sql", "02_reference_data.sql"):
                cursor.execute((ROOT / "sql" / filename).read_text())
        self.path = Path(self.enterContext(TemporaryDirectory())) / "input.parquet"
        self.earthquake, _ = silver.clean_realtime_features([first_feature()], "original.geojson")

    def drop_test_schema(self):
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(self.schema))

    def migrate(self):
        # The migration owns its own transaction, as it does when fed to psql.
        self.connection.autocommit = True
        try:
            with self.connection.cursor() as cursor:
                cursor.execute((ROOT / "sql/03_workshop3.sql").read_text())
        finally:
            self.connection.autocommit = False

    def query(self, statement, params=None):
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute(statement, params)
            if cursor.description:
                return cursor.fetchall()

    def ingest(self, frame, topic="earthquakes"):
        frame.to_parquet(self.path, index=False)
        return load.load_silver_run(self.connection, silver_manifest(topic, str(self.path)))[frame.iloc[0]["source"]]

    def test_upgrade_workshop2_and_replay_legacy_hash(self):
        event, row = load._event_row(self.earthquake.to_dict("records")[0], "earthquake")
        event["content_hash"] = "legacy-hash"
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute(load.EVENT_UPSERT, event)
            event_id = cursor.fetchone()[0]
            cursor.execute(load.DOMAIN_UPSERTS["earthquake"], {**row, "event_id": event_id})
            cursor.execute("""
                INSERT INTO ingestion_run (source_id, mode, status, read_count, accepted_count, rejected_count)
                VALUES ('usgs_realtime', 'historical', 'success', 5, 1, 1)
            """)
        with self.assertRaisesRegex(RuntimeError, "unchanged_count"):
            self.ingest(self.earthquake)
        before = self.query("SELECT event_id, updated_at, raw_reference FROM event")

        self.migrate()
        self.migrate()
        self.assertEqual(self.query("SELECT unchanged_count FROM ingestion_run"), [(3,)])
        cols = self.query("""
            SELECT data_type, is_nullable, column_default FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'ingestion_run' AND column_name = 'unchanged_count'
        """)
        self.assertEqual(cols, [("integer", "NO", "0")])

        legacy_frame = self.earthquake.assign(content_hash="legacy-hash")
        run = self.ingest(legacy_frame)
        self.assertEqual((run["accepted"], run["updated"], run["unchanged"]), (0, 0, 1))
        self.assertEqual(self.query("SELECT event_id, updated_at, raw_reference FROM event"), before)
        self.assertEqual(self.query("SELECT content_hash FROM event"), [(row["content_hash"],)])

    def test_insert_replay_metadata_then_stale_usgs_revision(self):
        self.migrate()
        original = self.earthquake
        self.assertEqual(self.ingest(original)["accepted"], 1)
        self.assertEqual(self.ingest(original)["unchanged"], 1)
        self.assertEqual(self.query("SELECT count(*) FROM event"), [(1,)])

        changed = original.assign(tsunami_bulletin_flag=not bool(original.iloc[0]["tsunami_bulletin_flag"]))
        self.assertEqual(self.ingest(changed)["updated"], 1)
        flag, = self.query("SELECT tsunami_bulletin_flag FROM earthquake")[0]
        self.assertEqual(flag, changed.iloc[0]["tsunami_bulletin_flag"])
        event_updated_at, digest = self.query("SELECT updated_at, content_hash FROM event")[0]

        revision = original["source_updated_at"].iloc[0].to_pydatetime() + timedelta(milliseconds=1)
        metadata = changed.assign(source_updated_at=revision, bronze_file="revision.geojson")
        counts = self.ingest(metadata)
        self.assertEqual((counts["updated"], counts["unchanged"]), (0, 1))
        self.assertEqual(self.query("SELECT updated_at, content_hash FROM event"), [(event_updated_at, digest)])
        self.assertEqual(self.query("SELECT source_updated_at FROM earthquake"), [(revision,)])
        self.assertEqual(self.query("SELECT raw_reference ->> 'bronze_file' FROM event"), [("revision.geojson",)])

        self.assertEqual(self.ingest(original)["unchanged"], 1)
        self.assertEqual(self.query("SELECT tsunami_bulletin_flag, source_updated_at FROM earthquake"), [(flag, revision)])
        earlier_id = original.assign(external_id="unseen-older-id")
        self.assertEqual(self.ingest(earlier_id)["accepted"], 1)
        self.assertEqual(self.query("SELECT count(*) FROM event"), [(2,)])
        self.assertEqual(self.query("SELECT watermark::timestamptz FROM ingestion_state"), [(revision,)])

    def test_noaa_and_cap_changes_persist_without_new_ids(self):
        self.migrate()
        tsunami, _ = silver.clean_tsunamis([FIXTURES / "noaa_tsunamis_pacific_page2.json"])
        tsunami = tsunami.iloc[:1]
        self.assertEqual(self.ingest(tsunami, "tsunami_events")["accepted"], 1)
        self.assertEqual(self.ingest(tsunami.assign(country="REVISED", warning_status_id=1),
                                     "tsunami_events")["updated"], 1)
        self.assertEqual(self.query("SELECT country, warning_status_id FROM tsunami_event"), [("REVISED", 1)])

        cap, _ = silver.clean_bulletins({"ntwc": FIXTURES / "ntwc_20261116_013756.xml"},
                                        {"ntwc": [FIXTURES / "ntwc_cap1_20261116_013756.xml"]})
        self.assertEqual(self.ingest(cap, "tsunami_bulletins")["accepted"], 1)
        revised = cap.assign(cap_event="Tsunami Warning", urgency="Immediate",
                             reported_depth_text="13 kilometers", reported_depth_km=13.0)
        self.assertEqual(self.ingest(revised, "tsunami_bulletins")["updated"], 1)
        self.assertEqual(self.ingest(revised, "tsunami_bulletins")["unchanged"], 1)
        self.assertEqual(self.query("SELECT cap_event, urgency, reported_depth_text, reported_depth_km, sent_at "
                                    "FROM tsunami_bulletin"),
                         [("Tsunami Warning", "Immediate", "13 kilometers", 13.0, cap.iloc[0]["sent_at"])])

    def test_failed_watermark_write_rolls_back_data_and_run(self):
        self.migrate()
        self.ingest(self.earthquake)
        before = self.query("""
            SELECT e.content_hash, q.magnitude, s.watermark, s.last_successful_run_id,
                   (SELECT count(*) FROM ingestion_run)
            FROM event e JOIN earthquake q USING (event_id)
            JOIN ingestion_state s ON e.source = s.source_id
        """)
        self.query("ALTER TABLE ingestion_state ADD CONSTRAINT fail_watermark CHECK (false) NOT VALID")
        revision = self.earthquake["source_updated_at"].iloc[0].to_pydatetime() + timedelta(hours=1)
        after = self.earthquake.assign(magnitude=8.8, source_updated_at=revision)
        with self.assertRaises(psycopg2.errors.CheckViolation):
            self.ingest(after)
        self.assertEqual(before, self.query("""
            SELECT e.content_hash, q.magnitude, s.watermark, s.last_successful_run_id,
                   (SELECT count(*) FROM ingestion_run)
            FROM event e JOIN earthquake q USING (event_id)
            JOIN ingestion_state s ON e.source = s.source_id
        """))
