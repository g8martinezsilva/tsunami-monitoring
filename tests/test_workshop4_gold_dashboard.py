"""Workshop 4 Gold additions for the dashboards (src/gold_dashboard.py).

Run inside the Airflow image (PySpark and Java):
docker compose run --rm --no-deps -e CONNECTION_CHECK_MAX_COUNT=0 airflow-scheduler \
    python -m unittest tests.test_workshop4_gold_dashboard
"""

import math
import unittest
from datetime import date, datetime, timedelta, timezone
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

NOW = datetime(2026, 11, 27, 12, tzinfo=timezone.utc)
RULES = {"min_magnitude": 6.0}
GOLD = {"region_cell_degrees": 10, "historical_years": [2010, 2025]}
SETTINGS = {
    "current_window_days": 30,
    "outlier_fields": {"earthquakes": ["magnitude", "depth_km"]},
    "schema": {"earthquakes": {"required": ["external_id", "event_time", "latitude", "longitude", "magnitude"],
                               "ranges": {"latitude": [-90, 90], "magnitude": [0, 10], "depth_km": [-10, 800]}}},
    "thresholds": {"null_rate_pct_max": 20, "structural_nulls": ["usgs_fdsn.tsunami_bulletin_flag"],
                   "schema_compliance_pct_min": 99, "outlier_rate_pct_max": 10,
                   "watermark_age_hours_max": {"usgs_realtime": 1}, "run_compliance_pct_min": 90,
                   "coverage_pct_min": 80},
}
EQ = ("source STRING, external_id STRING, event_time TIMESTAMP, latitude DOUBLE, longitude DOUBLE, "
      "magnitude DOUBLE, depth_km DOUBLE, bronze_manifest STRING")


class PoissonTests(unittest.TestCase):
    def test_upper_tail(self):
        from src.gold_dashboard import poisson_upper_tail  # pure Python, no Spark needed
        self.assertAlmostEqual(poisson_upper_tail(0, 2.0), 1.0)
        self.assertAlmostEqual(poisson_upper_tail(1, 2.0), 1 - math.exp(-2))
        self.assertAlmostEqual(poisson_upper_tail(3, 0.5), 1 - math.exp(-0.5) * (1 + 0.5 + 0.125))
        self.assertIsNone(poisson_upper_tail(2, 0))


@unittest.skipUnless(find_spec("pyspark"), "Run with PySpark and Java in the Airflow image")
class GoldDashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src import gold, gold_dashboard
        from src.spark_session import get_spark

        cls.gd, cls.gold = gold_dashboard, gold
        cls.spark = get_spark("tsunami-gold-dashboard-tests")
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        recent = NOW - timedelta(days=3)
        rows = [
            # 16 historical M>=6 earthquakes in one cell (one per year): expected 16 * 30 / 5844 in 30 days
            *[("usgs_fdsn", f"h{y}", datetime(y, 6, 1, tzinfo=timezone.utc), 35.0, 142.0, 6.5, 20.0,
               "bronze_manifest_20261117_001544.json") for y in range(2010, 2026)],
            ("usgs_fdsn", "h2026", datetime(2026, 11, 1, tzinfo=timezone.utc), 35.0, 142.0, 6.1, 20.0,
             "bronze_manifest_20261117_001544.json"),          # reconciliation event: not historical
            ("usgs_realtime", "r1", recent, 35.5, 142.5, 6.2, 10.0, "bronze_manifest_20261125_100000_feeds.json"),
            ("usgs_realtime", "r2", recent, 36.0, 141.0, 6.8, 30.0, "bronze_manifest_20261126_100000_feeds.json"),
            ("usgs_realtime", "r-old", NOW - timedelta(days=45), 35.0, 142.0, 6.0, 10.0,
             "bronze_manifest_20261126_100000_feeds.json"),   # outside the 30-day window
            ("usgs_realtime", "r-small", recent, 35.0, 142.0, 5.0, 10.0,
             "bronze_manifest_20261126_100000_feeds.json"),   # below M6
            ("usgs_realtime", "r-bad", recent, 95.0, 142.0, 12.0, None,
             "bronze_manifest_20261126_100000_feeds.json"),   # out of range: not schema compliant
        ]
        cls.current = {"earthquakes": cls.spark.createDataFrame(rows, EQ)}
        # Raw Silver: r1 captured twice (two snapshots), the others once
        raw_rows = rows + [("usgs_realtime", "r1", recent, 35.5, 142.5, 6.2, 10.0,
                            "bronze_manifest_20261126_100000_feeds.json")]
        cls.raw = {"earthquakes": cls.spark.createDataFrame(raw_rows, EQ)}

    def frame(self, parts):
        return pd.concat([part.toPandas() for part in parts], ignore_index=True)

    def test_historical_vs_current_uses_same_threshold_window_and_period(self):
        story = self.frame(self.gd.historical_vs_current(self.current, RULES, GOLD, SETTINGS, NOW)).set_index("region_cell")
        japan = story.loc["lat 30..40, lon 140..150"]
        self.assertEqual((japan["count"], japan["historical_count"]), (2, 16))  # r1, r2; h2026 excluded
        expected = 16 * 30 / 5844
        self.assertAlmostEqual(japan["expected_count"], round(expected, 3))
        self.assertAlmostEqual(japan["value"], round(2 / expected, 2))
        self.assertAlmostEqual(japan["p_value"], round(1 - math.exp(-expected) * (1 + expected), 4))
        self.assertEqual(story.loc["All Pacific query area", "count"], 2)
        self.assertEqual(set(story["window_days"]), {30})

    def test_schema_compliance_rows_and_files(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            good, bad = Path(tmp) / "good.parquet", Path(tmp) / "bad.parquet"
            pq.write_table(pa.table({c: [1] for c in SETTINGS["schema"]["earthquakes"]["required"]}), good)
            pq.write_table(pa.table({"external_id": ["x"]}), bad)
            gov = self.frame(self.gd.schema_compliance(self.current, {"earthquakes": [good, bad]}, SETTINGS))
        realtime = gov.set_index("source").loc["usgs_realtime"]
        self.assertEqual((realtime.schema_rows, realtime.schema_compliant), (5, 4))
        self.assertEqual((realtime.files_checked, realtime.files_compliant), (2, 1))

    def test_outlier_rate_uses_iqr_per_source(self):
        gov = self.frame(self.gd.outlier_rates(self.current, SETTINGS)).set_index(["source", "field"])
        fdsn = gov.loc[("usgs_fdsn", "magnitude")]
        self.assertEqual((fdsn.outlier_values, fdsn.outlier_count), (17, 1))  # 6.1 below a constant 6.5
        self.assertEqual(gov.loc[("usgs_realtime", "depth_km"), "outlier_values"], 4)  # null depth skipped

    def test_box_plot_uses_observed_whiskers_and_preserves_repeated_outliers(self):
        values = [-20, -20, 1, 3, 3, 3, 4, 4, 5, 5, 5, 5, 6, 8, 30, 30, None, float("nan")]
        frame = self.spark.createDataFrame(
            [("usgs_fdsn", float(value) if value is not None else None) for value in values],
            "source STRING, depth_km DOUBLE",
        )
        parts = self.gd.outlier_rates({"earthquakes": frame}, SETTINGS)
        governance = self.gold._union(self.spark.createDataFrame([], self.gold.GOVERNANCE_SCHEMA), parts)
        row = self.gd.apply_thresholds(governance, SETTINGS).first()
        self.assertEqual((row.q1, row.median, row.q3, row.iqr), (3.0, 4.0, 5.0, 2.0))
        self.assertEqual((row.lower_whisker, row.upper_whisker), (1.0, 8.0))  # IQR limits are 0 and 8
        self.assertEqual(row.outlier_points, [-20.0, -20.0, 30.0, 30.0])
        self.assertEqual((row.outlier_values, row.outlier_count, row.outlier_rate_pct), (16, len(row.outlier_points), 25.0))
        self.assertEqual((row.threshold, row.status), (10.0, "review"))

    def test_volume_counts_rows_and_first_captures_per_day(self):
        gov = self.frame(self.gd.volume_over_time(self.raw, self.gold.snapshot_timestamp))
        day = gov[(gov["source"] == "usgs_realtime") & (gov["period"] == date(2026, 11, 26))].iloc[0]
        self.assertEqual((day.volume_rows, day.new_observations), (5, 4))  # r1 again, first seen on the 25th
        first = gov[(gov["source"] == "usgs_realtime") & (gov["period"] == date(2026, 11, 25))].iloc[0]
        self.assertEqual((first.volume_rows, first.new_observations), (1, 1))

    def test_thresholds_and_status(self):
        rows = [("null_rate", "usgs_fdsn", "tsunami_bulletin_flag", 100.0, None, None, None, None, None, None),
                ("null_rate", "noaa_hazel", "warning_status_id", 76.0, None, None, None, None, None, None),
                ("null_rate", "noaa_hazel", "latitude", 4.0, None, None, None, None, None, None),
                ("schema_compliance", "usgs_realtime", None, None, 99.5, 3, 2, None, None, None),
                ("outlier_rate", "usgs_fdsn", "depth_km", None, None, None, None, 15.4, None, None),
                ("pipeline_state", "usgs_realtime", None, None, None, None, None, None, 2.5, None),
                ("pipeline_state", "ntwc", None, None, None, None, None, None, 30.0, None),
                ("matching", "noaa_hazel", None, None, None, None, None, None, None, 88.1)]
        frame = self.spark.createDataFrame(rows, "topic STRING, source STRING, field STRING, null_rate_pct DOUBLE, "
                                                 "schema_compliance_pct DOUBLE, files_checked BIGINT, files_compliant BIGINT, "
                                                 "outlier_rate_pct DOUBLE, watermark_age_hours DOUBLE, coverage_pct DOUBLE")
        frame = frame.withColumn("run_compliance_pct", frame["coverage_pct"])
        status = self.gd.apply_thresholds(frame, SETTINGS).toPandas()["status"].tolist()
        self.assertEqual(status, ["expected", "warning", "ok", "warning", "review", "warning", "n/a", "ok"])


if __name__ == "__main__":
    unittest.main()
