"""Gold integration tests using local Spark, Airflow and isolated Parquet fixtures."""

import unittest
from datetime import date, datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def airflow_is_installed():
    try:
        version("apache-airflow")
    except PackageNotFoundError:
        return False
    return True


@unittest.skipUnless(airflow_is_installed(), "Run with Apache Airflow installed")
class GoldDAGTests(unittest.TestCase):
    def test_dag_import(self):
        from airflow.configuration import conf
        from airflow.models.dagbag import DagBag

        dag_file = Path(__file__).resolve().parents[1] / "airflow/dags/gold_processing_dag.py"
        if not dag_file.is_file():
            dag_file = Path(conf.get("core", "dags_folder")) / dag_file.name

        bag = DagBag(dag_folder=str(dag_file), include_examples=False)
        self.assertFalse(bag.import_errors)
        dag = bag.dags["gold_processing"]
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.task_ids, ["build_gold"])
        self.assertEqual(dag.get_task("build_gold").retries, 1)


TIMESTAMP = pa.timestamp("us", tz="UTC")
COMMON = [
    ("source", pa.string()), ("external_id", pa.string()),
    ("latitude", pa.float64()), ("longitude", pa.float64()),
    ("bronze_manifest", pa.string()), ("silver_processed_at", TIMESTAMP),
]
SCHEMAS = {
    "earthquakes": pa.schema(COMMON + [
        ("event_time", TIMESTAMP), ("source_updated_at", TIMESTAMP), ("magnitude", pa.float64()),
        ("magnitude_type", pa.string()), ("depth_km_iqr_outlier", pa.bool_()),
    ]),
    "tsunami_events": pa.schema(COMMON + [
        ("event_time", TIMESTAMP), ("event_date", pa.date32()), ("known_hour", pa.int64()),
        ("known_minute", pa.int64()), ("time_precision", pa.string()), ("max_water_height", pa.float64()),
        ("max_water_height_iqr_outlier", pa.bool_()),
    ]),
    "tsunami_bulletins": pa.schema(COMMON + [
        ("center", pa.string()), ("sent_at", TIMESTAMP), ("atom_updated_at", TIMESTAMP), ("cap_event", pa.string()),
    ]),
    "rejected_records": pa.schema([
        ("source", pa.string()), ("external_id", pa.string()), ("reason", pa.string()),
        ("bronze_file", pa.string()), ("locator", pa.string()), ("silver_processed_at", TIMESTAMP),
    ]),
}
ARCHIVED = "20261117_015000"
FIRST = "20261119_010000"
SECOND = "20261119_020000"
ORIGIN = datetime(2004, 12, 26, 1, 1, tzinfo=timezone.utc)
OLDER_REVISION = datetime(2026, 11, 18, tzinfo=timezone.utc)
NEWER_REVISION = datetime(2026, 11, 19, tzinfo=timezone.utc)


def save_snapshot(root, topic, stamp, records, *, flat=False, schema=None):
    folder = root if flat else root / topic
    folder.mkdir(parents=True, exist_ok=True)
    # Reprocessing the first Bronze snapshot happened after the second one.
    processed_at = NEWER_REVISION if stamp == FIRST else OLDER_REVISION
    rows = [
        {"bronze_manifest": f"bronze_manifest_{stamp}.json", "silver_processed_at": processed_at, **row}
        for row in records
    ]
    path = folder / f"{topic}_{stamp}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema if schema is not None else SCHEMAS[topic]), path)
    return path


def nanosecond_schema(topic):
    return pa.schema([
        field.with_type(pa.timestamp("ns", tz=field.type.tz))
        if pa.types.is_timestamp(field.type) and field.name != "silver_processed_at" else field
        for field in SCHEMAS[topic]
    ])


def save_fixtures(root):
    earthquake = {"external_id": "shared-id", "event_time": ORIGIN, "latitude": 1.0, "longitude": 2.0,
                  "source_updated_at": NEWER_REVISION, "magnitude": 5.75, "magnitude_type": "mw",
                  "depth_km_iqr_outlier": True}
    save_snapshot(root, "earthquakes", FIRST, [
        {**earthquake, "source": "usgs_fdsn"},
        {**earthquake, "source": "usgs_realtime"},
    ])
    save_snapshot(root, "earthquakes", SECOND, [
        {**earthquake, "source": "usgs_realtime", "source_updated_at": OLDER_REVISION, "magnitude": 4.0},
    ])

    tsunamis = [
        {"external_id": str(i), "source": "noaa_hazel", "event_date": date(2004, 12, 26), "event_time": ORIGIN,
         "known_hour": 1, "known_minute": 1, "time_precision": "second", "max_water_height": 1.25,
         "latitude": 1.0, "longitude": 2.0, "max_water_height_iqr_outlier": False}
        for i in range(221)
    ]
    save_snapshot(root, "tsunami_events", FIRST, tsunamis)
    tsunamis[0].update(event_time=None, known_hour=None, known_minute=None, time_precision="day", latitude=None,
                      max_water_height=None)
    tsunamis[1].update(max_water_height=5.25, max_water_height_iqr_outlier=True)
    save_snapshot(root, "tsunami_events", SECOND, tsunamis)

    bulletin = {"external_id": "cap-id", "source": "ptwc", "center": "ptwc",
                "sent_at": ORIGIN, "atom_updated_at": NEWER_REVISION}
    save_snapshot(root, "tsunami_bulletins", FIRST, [
        {**bulletin, "cap_event": "initial"},
        {**bulletin, "external_id": "no-atom", "sent_at": NEWER_REVISION, "atom_updated_at": None,
         "cap_event": "newer issue"},
    ])
    save_snapshot(root, "tsunami_bulletins", SECOND, [
        {**bulletin, "cap_event": "corrected"},
        {**bulletin, "external_id": "no-atom", "sent_at": OLDER_REVISION, "atom_updated_at": None,
         "cap_event": "older issue"},
    ])
    reject = {"source": "noaa_hazel", "external_id": None, "reason": "missing id or date",
              "bronze_file": "noaa.json", "locator": "items[0]"}
    reject_schema = SCHEMAS["rejected_records"]
    empty_schema = pa.schema([(field.name, pa.null()) for field in reject_schema])
    save_snapshot(root, "rejected_records", ARCHIVED, [], flat=True, schema=empty_schema)
    save_snapshot(root, "rejected_records", FIRST, [reject],
                  schema=reject_schema.set(1, pa.field("external_id", pa.null())))
    save_snapshot(root, "rejected_records", SECOND, [
        reject,
        {**reject, "external_id": "known-id", "locator": "items[1]"},
        {**reject, "locator": "items[2]"},
        {**reject, "reason": "coordinates out of range"},
        {**reject, "bronze_file": "noaa_redownload.json"},
    ])


@unittest.skipUnless(find_spec("pyspark"), "Run with PySpark and Java in the Airflow image")
class GoldSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src import gold
        from src.spark_session import get_spark

        cls.gold = gold
        cls.root = Path(cls.enterClassContext(TemporaryDirectory()))
        cls.silver_dir = cls.root / "silver"
        save_fixtures(cls.silver_dir)
        cls.spark = get_spark("tsunami-gold-tests")
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.paths = gold.run_gold(cls.spark, cls.silver_dir, cls.root / "gold")

    def current_topic(self, topic):
        files = self.gold.discover_silver(self.silver_dir)[topic]
        self.assertEqual(len(files), 2)
        # Invert read order to check that revision and capture metadata drive selection.
        return self.gold.consolidate(self.spark.read.parquet(*map(str, reversed(files))), topic)

    def test_current_versions_use_source_revision_and_bronze_capture(self):
        earthquakes = self.current_topic("earthquakes").collect()
        self.assertEqual({(row.source, row.magnitude) for row in earthquakes}, {
            ("usgs_fdsn", 5.75), ("usgs_realtime", 5.75),
        })
        bulletin_labels = {row.external_id: row.cap_event for row in self.current_topic("tsunami_bulletins").collect()}
        self.assertEqual(bulletin_labels, {"cap-id": "corrected", "no-atom": "newer issue"})

    def test_noaa_442_rows_consolidate_to_221_and_keep_partial_dates(self):
        frame = self.current_topic("tsunami_events")
        self.assertEqual(frame.count(), 221)
        partial = frame.where("external_id = '0'").first()
        self.assertEqual(partial._snapshot_rows, 2)
        self.assertEqual(partial.event_date, date(2004, 12, 26))
        self.assertEqual(partial.time_precision, "day")
        for field in ("event_time", "known_hour", "known_minute", "latitude", "max_water_height"):
            self.assertIsNone(partial[field])

    def test_governance_history_and_quality(self):
        governance = pd.read_parquet(self.paths["governance"])
        noaa = governance.loc[governance["topic"].eq("tsunami_events")].iloc[0]
        self.assertEqual((noaa.rows_processed, noaa.unique_observations, noaa.repeated_rows), (442, 221, 221))
        self.assertEqual((noaa.missing_coordinates, noaa.missing_coordinates_pct), (1, 0.45))
        self.assertEqual((noaa.incomplete_times, noaa.incomplete_times_pct), (1, 0.45))
        self.assertEqual((noaa.iqr_field, noaa.iqr_outliers), ("max_water_height", 1))
        self.assertTrue(governance.loc[governance["topic"].eq("tsunami_bulletins"), "iqr_outliers"].isna().all())

    def test_rejection_counts_distinguish_processed_rows_and_unique_evidence(self):
        governance = pd.read_parquet(self.paths["governance"])
        rejected = governance["topic"].eq("rejected_records")
        row = governance.loc[rejected].iloc[0]
        self.assertEqual(row.source, "noaa_hazel")
        self.assertEqual(row.rejected_records, 6)
        self.assertEqual(row.unique_rejected_records, 5)
        self.assertTrue(governance.loc[~rejected, "unique_rejected_records"].isna().all())

    def test_historical_snapshots_fill_coverage_without_rewriting_or_duplicating(self):
        root = self.root / "historical-silver"
        earthquake = {
            "source": "usgs_realtime", "external_id": "shared-eq", "event_time": ORIGIN,
            "source_updated_at": NEWER_REVISION, "magnitude": 5.75, "magnitude_type": "mw",
        }
        archive_eq = save_snapshot(root, "earthquakes", ARCHIVED, [
            {**earthquake, "source_updated_at": OLDER_REVISION, "magnitude": 4.0},
            {**earthquake, "external_id": "archived-eq"},
        ], flat=True, schema=nanosecond_schema("earthquakes"))
        save_snapshot(root, "earthquakes", FIRST, [earthquake])
        # A flat file with compatible timestamps can be read directly.
        compatible_eq = save_snapshot(root, "earthquakes", SECOND, [
            {**earthquake, "external_id": "compatible-flat-eq"},
        ], flat=True)

        bulletin = {"source": "ptwc", "center": "ptwc", "external_id": "shared-cap", "sent_at": ORIGIN}
        archive_cap = save_snapshot(root, "tsunami_bulletins", ARCHIVED, [
            bulletin, {**bulletin, "external_id": "archived-cap"},
        ], flat=True, schema=nanosecond_schema("tsunami_bulletins"))
        save_snapshot(root, "tsunami_bulletins", FIRST, [bulletin])

        originals = {path: path.read_bytes() for path in root.rglob("*.parquet")}
        snapshots = self.gold.discover_silver(root)
        scratch = self.root / "compatibility-copies"
        frame = self.gold.read_silver(self.spark, "earthquakes", snapshots["earthquakes"], scratch)
        selected = {row.external_id: row for row in self.gold.consolidate(frame, "earthquakes").collect()}
        self.assertEqual(selected["archived-eq"]._silver_file, archive_eq.as_uri())
        self.assertEqual(selected["compatible-flat-eq"]._silver_file, compatible_eq.as_uri())
        self.assertEqual(selected["shared-eq"].magnitude, 5.75)
        self.assertEqual(selected["shared-eq"]._snapshot_rows, 2)
        self.assertEqual([path.name for path in scratch.rglob("*.parquet")], [archive_eq.name])

        outputs = self.gold.run_gold(self.spark, root, self.root / "historical-gold")
        counts = pd.read_parquet(outputs["governance"]).set_index("source")
        fields = ["rows_processed", "unique_observations", "repeated_rows"]
        self.assertEqual(counts.loc["usgs_realtime", fields].tolist(), [4, 3, 1])
        self.assertEqual(counts.loc["ptwc", fields].tolist(), [3, 2, 1])
        story = pd.read_parquet(outputs["storytelling"])
        self.assertEqual(story.groupby("metric")["count"].sum().to_dict(), {
            "bulletins_by_center": 2, "earthquake_magnitude_distribution": 3, "earthquakes_by_year": 3,
        })
        self.assertEqual(pq.read_schema(archive_cap).field("sent_at").type.unit, "ns")
        self.assertEqual({path: path.read_bytes() for path in root.rglob("*.parquet")}, originals)

    def test_indicators_include_partial_dates_and_separate_units(self):
        story = pd.read_parquet(self.paths["storytelling"])
        self.assertEqual(story.groupby("metric")["count"].sum().to_dict(), {
            "bulletins_by_center": 2,
            "earthquake_magnitude_distribution": 2,
            "earthquakes_by_year": 2,
            "historical_tsunamis_by_year": 221,
            "tsunami_max_water_height_distribution": 220,
        })
        year_row = story.loc[story["metric"].eq("historical_tsunamis_by_year")].iloc[0]
        self.assertEqual(year_row.year, 2004)
        magnitude = story.loc[story["metric"].eq("earthquake_magnitude_distribution")]
        self.assertEqual(set(magnitude["bin_start"]), {5.5})
        self.assertEqual(set(magnitude["bin_end"]), {6.0})
        self.assertTrue(magnitude["unit"].isna().all())
        heights = story.loc[story["metric"].eq("tsunami_max_water_height_distribution")]
        self.assertEqual(set(heights["unit"]), {"m"})
        self.assertEqual(set(zip(heights["bin_start"], heights["bin_end"])), {(1.0, 2.0), (5.0, 6.0)})

    def test_replay_creates_a_new_pair_with_the_same_spark_and_pandas_results(self):
        replay = self.gold.run_gold(self.spark, self.silver_dir, self.root / "gold")
        for name, path in self.paths.items():
            with self.subTest(dataset=name):
                self.assertNotEqual(path, replay[name])
                first = pd.read_parquet(path)
                second = pd.read_parquet(replay[name])
                self.assertEqual(self.spark.read.parquet(str(path)).count(), len(first))
                self.assertEqual(self.spark.read.parquet(str(replay[name])).count(), len(first))
                pd.testing.assert_frame_equal(
                    first.sort_values(list(first.columns)).reset_index(drop=True),
                    second.sort_values(list(second.columns)).reset_index(drop=True),
                )
