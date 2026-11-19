"""Offline tests for incremental admission and the Silver loader."""

import json
import os
import unittest
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import ANY, MagicMock, Mock, patch

import pandas as pd

from src import incremental, load, silver

FIXTURES = Path(__file__).parent / "fixtures"
STAMP = "20261119_120000"
TOPICS = ("earthquakes", "tsunami_events", "tsunami_bulletins", "rejected_records")


def first_feature():
    return json.loads((FIXTURES / "usgs_realtime_20261107_050434.geojson").read_text())["features"][0]


def feed_response(features):
    payload = {"type": "FeatureCollection", "features": features}
    return Mock(content=json.dumps(payload).encode(), json=Mock(return_value=payload))


def known_rows(frame):
    return {row["external_id"]: {**row, "event_id": 5, "stored_hash": row["content_hash"]}
            for row in frame.to_dict("records")}


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.bronze = Path(self.enterContext(TemporaryDirectory()))
        self.enterContext(patch.dict(os.environ, {"RAW_DIR": str(self.bronze)}))
        self.connection = MagicMock()

    def poll(self, source, responses, known):
        with (patch.object(incremental, "get_with_retries", side_effect=responses),
              patch.object(incremental, "read_observations", return_value=known)):
            return incremental.fetch_feed_if_new(source, STAMP, self.connection)

    def test_new_ids_at_or_below_global_watermark_are_admitted(self):
        recorded = first_feature()
        frame, _ = silver.clean_realtime_features([recorded], "recorded.geojson")
        earlier, equal = first_feature(), first_feature()
        earlier["id"] = "late-new-id"
        earlier["properties"]["updated"] -= 60_000
        equal["id"] = "new-id-at-watermark"
        response = feed_response([recorded, earlier, equal])

        summary = self.poll("usgs_realtime", [response], known_rows(frame))

        self.assertEqual(summary["candidate_records"], 2)
        self.assertEqual(summary["records"], 3)
        self.assertEqual(Path(summary["files"][0]).read_bytes(), response.content)

    def test_unchanged_and_older_realtime_features_do_not_publish(self):
        recorded = first_feature()
        frame, _ = silver.clean_realtime_features([recorded], "recorded.geojson")
        older = first_feature()
        older["properties"]["updated"] -= 1
        older["properties"]["mag"] -= 0.5
        for feature in (recorded, older):
            with self.subTest(feature=feature["properties"]["updated"]):
                summary = self.poll("usgs_realtime", [feed_response([feature])], known_rows(frame))
                self.assertEqual(summary["files"], [])
        self.assertEqual(list(self.bronze.iterdir()), [])

    def test_cap_correction_with_same_sent_reaches_silver(self):
        atom_path = FIXTURES / "ntwc_20261116_013756.xml"
        cap_path = FIXTURES / "ntwc_cap1_20261116_013756.xml"
        frame, _ = silver.clean_bulletins({"ntwc": atom_path}, {"ntwc": [cap_path]})
        original = known_rows(frame)
        atom, cap = atom_path.read_bytes(), cap_path.read_bytes()
        revised = cap.replace(b"<urgency>Unknown</urgency>", b"<urgency>Immediate</urgency>")
        self.assertNotEqual(revised, cap)
        self.assertEqual(self.poll("ntwc", [Mock(content=atom), Mock(content=cap)], original)["files"], [])

        summary = self.poll("ntwc", [Mock(content=atom), Mock(content=revised)], original)
        self.assertEqual(summary["candidate_records"], 1)
        manifest = incremental.publish_run(
            [summary], datetime(2026, 11, 19, 12, tzinfo=timezone.utc), "test-run", "feeds"
        )
        self.assertTrue((self.bronze / "_new" / Path(manifest).name).is_file())
        with TemporaryDirectory() as silver_dir:
            output = silver.process_bronze_run(manifest, silver_dir, STAMP)
            actual = pd.read_parquet(output["outputs"]["tsunami_bulletins"]["file"]).iloc[0]
            self.assertEqual(actual["sent_at"], frame.iloc[0]["sent_at"])
            self.assertEqual(actual["atom_updated_at"], frame.iloc[0]["atom_updated_at"])
            self.assertEqual(actual["urgency"], "Immediate")
            self.assertNotEqual(actual["content_hash"], frame.iloc[0]["content_hash"])
        self.assertEqual(Path(summary["files"][1]).read_bytes(), revised)

    def test_empty_atom_is_distinct_from_invalid_atom(self):
        empty = b'<feed xmlns="http://www.w3.org/2005/Atom" />'
        self.assertEqual(self.poll("ptwc", [Mock(content=empty)], {})["files"], [])
        with self.assertRaisesRegex(ValueError, "expected an Atom feed"):
            self.poll("ptwc", [Mock(content=b"<html>maintenance</html>")], {})
        with self.assertRaises(ET.ParseError):
            self.poll("ptwc", [Mock(content=b"not XML")], {})
        self.assertEqual(list(self.bronze.iterdir()), [])

    def test_fdsn_overlap_and_empty_count(self):
        with patch.object(incremental, "get_with_retries", return_value=Mock(text="0")) as fetch:
            summary = incremental.fetch_fdsn_updates(STAMP, "2026-11-19T12:00:00+00:00")
        self.assertEqual(summary["files"], [])
        fetch.assert_called_once()
        self.assertEqual(fetch.call_args.kwargs["updatedafter"], "2026-11-18T12:00:00")
        self.assertEqual(fetch.call_args.kwargs["starttime"], "2010-01-01")


def silver_manifest(topic, path):
    return {"bronze_manifest": f"test-{STAMP}.json", "mode": "incremental",
            "outputs": {name: {"file": path if topic == name else None} for name in TOPICS}}


class LoaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.earthquake, _ = silver.clean_realtime_features([first_feature()], "original.geojson")
        cls.tsunami, _ = silver.clean_tsunamis([FIXTURES / "noaa_tsunamis_pacific_page2.json"])
        cls.tsunami = cls.tsunami.iloc[:1].copy()
        cls.bulletin, _ = silver.clean_bulletins(
            {"ntwc": FIXTURES / "ntwc_20261116_013756.xml"},
            {"ntwc": [FIXTURES / "ntwc_cap1_20261116_013756.xml"]},
        )

    def setup_load(self, frame, topic, previous=None, error=None, migrated=True):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [(migrated,), (123,), (5,)]
        cursor.execute.side_effect = error
        self.enterContext(patch.object(load.pd, "read_parquet", return_value=frame))
        self.enterContext(patch.object(load, "read_observations", return_value=previous or {}))
        return connection, cursor, silver_manifest(topic, "mocked.parquet")

    def test_first_load_and_replay_use_different_write_paths(self):
        connection, cursor, manifest = self.setup_load(self.earthquake, "earthquakes")
        self.assertEqual(load.load_silver_run(connection, manifest)["usgs_realtime"]["accepted"], 1)
        cursor.execute.assert_any_call(load.EVENT_UPSERT, ANY)
        cursor.execute.assert_any_call(load.DOMAIN_UPSERTS["earthquake"], ANY)

        connection, cursor, manifest = self.setup_load(self.earthquake, "earthquakes", known_rows(self.earthquake))
        counts = load.load_silver_run(connection, manifest)["usgs_realtime"]
        self.assertEqual((counts["accepted"], counts["updated"], counts["unchanged"]), (0, 0, 1))
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertNotIn(load.EVENT_UPSERT, statements)
        self.assertNotIn(load.DOMAIN_UPSERTS["earthquake"], statements)

    def test_business_field_changes_are_written(self):
        for kind, topic, original, changes in [
            ("earthquake", "earthquakes", self.earthquake, {"tsunami_bulletin_flag": True}),
            ("tsunami", "tsunami_events", self.tsunami, {"country": "UPDATED", "warning_status_id": 1}),
            ("bulletin", "tsunami_bulletins", self.bulletin,
             {"urgency": "Immediate", "cap_event": "Tsunami Warning", "reported_depth_text": "13 kilometers"}),
        ]:
            with self.subTest(kind=kind):
                revised = original.assign(**changes)
                connection, cursor, manifest = self.setup_load(revised, topic, known_rows(original))
                counts = load.load_silver_run(connection, manifest)[original.iloc[0]["source"]]
                self.assertEqual(counts["updated"], 1)
                upsert = next(call.args[1] for call in cursor.execute.call_args_list
                              if call.args[0] == load.DOMAIN_UPSERTS[kind])
                for field, value in changes.items():
                    self.assertEqual(upsert[field], value)
                self.assertNotEqual(upsert["content_hash"], original.iloc[0]["content_hash"])

    def test_revision_only_updates_metadata_and_watermark(self):
        revised_at = self.earthquake["source_updated_at"].iloc[0].to_pydatetime() + timedelta(seconds=1)
        revised = self.earthquake.assign(source_updated_at=revised_at, bronze_file="revised.geojson")
        connection, cursor, manifest = self.setup_load(revised, "earthquakes", known_rows(self.earthquake))
        counts = load.load_silver_run(connection, manifest)["usgs_realtime"]
        self.assertEqual((counts["updated"], counts["unchanged"]), (0, 1))
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertIn(load.REVISION_UPDATES["earthquake"], statements)
        self.assertNotIn(load.EVENT_UPSERT, statements)
        cursor.execute.assert_any_call(load.STATE_UPSERT, ("usgs_realtime", revised_at.isoformat(), 123))

    def test_older_version_cannot_overwrite_newer_observation(self):
        older_at = self.earthquake["source_updated_at"].iloc[0].to_pydatetime() - timedelta(seconds=1)
        older = self.earthquake.assign(source_updated_at=older_at, magnitude=9.0)
        connection, cursor, manifest = self.setup_load(older, "earthquakes", known_rows(self.earthquake))
        counts = load.load_silver_run(connection, manifest)["usgs_realtime"]
        self.assertEqual((counts["updated"], counts["unchanged"]), (0, 1))
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertNotIn(load.EVENT_UPSERT, statements)
        self.assertNotIn(load.REVISION_UPDATES["earthquake"], statements)
        cursor.execute.assert_any_call(load.STATE_UPSERT, ("usgs_realtime", None, 123))

    def test_domain_failure_never_reaches_watermark_write(self):
        def fail_domain(statement, params=None):
            if statement == load.DOMAIN_UPSERTS["earthquake"]:
                raise RuntimeError("domain write failed")

        connection, cursor, manifest = self.setup_load(self.earthquake, "earthquakes", error=fail_domain)
        with self.assertRaisesRegex(RuntimeError, "domain write failed"):
            load.load_silver_run(connection, manifest)
        self.assertNotIn(load.STATE_UPSERT, [call.args[0] for call in cursor.execute.call_args_list])
        self.assertIs(connection.__exit__.call_args.args[0], RuntimeError)

    def test_workshop2_schema_fails_before_load_with_migration_hint(self):
        connection, cursor, manifest = self.setup_load(self.earthquake, "earthquakes", migrated=False)
        with self.assertRaisesRegex(RuntimeError, "unchanged_count.*workshop_3/README.md"):
            load.load_silver_run(connection, manifest)
        cursor.execute.assert_called_once()
