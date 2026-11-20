"""Gold indicators for the user stories and pipeline state (src/gold_indicators.py).

Run inside the Airflow image (PySpark and Java):
docker compose run --rm --no-deps -e CONNECTION_CHECK_MAX_COUNT=0 airflow-scheduler \
    python -m unittest tests.test_workshop3_gold_indicators
"""

import unittest
from datetime import datetime, timedelta, timezone
from importlib.util import find_spec

import pandas as pd

NOW = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
TOHOKU = datetime(2011, 3, 11, 5, 46, 24, tzinfo=timezone.utc)
RULES = {
    "min_magnitude": 6.0, "magnitude_points": [[6.0, 1], [7.0, 2], [8.0, 3]], "shallow_depth_km": 70,
    "deep_limit_km": 300, "history_radius_km": 300, "history_validity_codes": [3, 4], "history_cause_codes": [1],
    "moderate_points": 3, "high_points": 4, "official_alert_severities": ["Moderate", "Severe", "Extreme"],
}
SETTINGS = {"recent_min_magnitude": 4.5, "region_cell_degrees": 10, "run_window_days": 7,
            "expected_runs_per_day": {"incremental_ingestion": 144, "gold_processing": 1}}
MATCHING = {"tsunami_to_earthquake": {"cause_codes": [1, 2, 3, 4, 5]}}

EQ_SCHEMA = ("source STRING, external_id STRING, event_time TIMESTAMP, latitude DOUBLE, longitude DOUBLE, "
             "magnitude DOUBLE, magnitude_type STRING, depth_km DOUBLE, place STRING")
TS_SCHEMA = ("source STRING, external_id STRING, event_time TIMESTAMP, event_date DATE, latitude DOUBLE, "
             "longitude DOUBLE, cause_code BIGINT, event_validity BIGINT, max_water_height DOUBLE")
BU_SCHEMA = "source STRING, external_id STRING, sent_at TIMESTAMP, cap_severity STRING"


def links(*rows):
    return pd.DataFrame(rows, columns=["from_source", "from_external_id", "to_source", "to_external_id",
                                       "relation_type", "review_status"])


@unittest.skipUnless(find_spec("pyspark"), "Run with PySpark and Java in the Airflow image")
class GoldIndicatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src import gold_indicators
        from src.spark_session import get_spark

        cls.gi = gold_indicators
        cls.spark = get_spark("tsunami-gold-indicator-tests")
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        s = cls.spark
        cls.current = {
            "earthquakes": s.createDataFrame([
                # Tohoku: M9.1, 29 km -> 3 + 1 + 1 (earlier tsunami 5412 within 300 km) = 5 -> HIGH
                ("usgs_fdsn", "usp000hvnu", TOHOKU, 38.297, 142.373, 9.1, "mww", 29.0, "near the east coast of Honshu"),
                # Bonin: M6.5, 475 km deep -> at most LOW
                ("usgs_realtime", "us6000u4r9", NOW - timedelta(days=2), 27.9, 140.0, 6.5, "mww", 475.0, "Bonin"),
                # M6.2, 20 km, no history but a Moderate bulletin -> OFFICIAL ALERT
                ("usgs_realtime", "us-alert", NOW - timedelta(hours=3), -20.0, -175.0, 6.2, "mww", 20.0, "Tonga"),
                # Small Pacific earthquake with a Minor bulletin: on the map, no relevance level
                ("usgs_realtime", "us7000ty16", NOW - timedelta(days=7), 18.6, -69.1 + 360 - 360, 4.8, "mb", 160.0, "Mona"),
                # Outside the FDSN query area (Tibet): not on the recent map
                ("usgs_realtime", "us-tibet", NOW - timedelta(days=1), 32.0, 91.0, 7.0, "mww", 10.0, "Nagqu"),
            ], EQ_SCHEMA),
            "tsunami_events": s.createDataFrame([
                ("noaa_hazel", "5412", datetime(2011, 3, 9, 2, 45, tzinfo=timezone.utc), None, 38.4, 142.8, 1, 4, 0.6),
                ("noaa_hazel", "5413", TOHOKU, None, 38.3, 142.4, 1, 4, 39.26),  # Tohoku's own tsunami
                ("noaa_hazel", "9000", None, None, None, None, 8, 2, None),       # landslide, no coordinates
            ], TS_SCHEMA),
            "tsunami_bulletins": s.createDataFrame([
                ("ptwc", "cap-alert", NOW - timedelta(hours=3), "Moderate"),
                ("ptwc", "cap-minor", NOW - timedelta(days=7), "Minor"),
                ("ntwc", "cap-unlinked", NOW - timedelta(days=1), "Minor"),
            ], BU_SCHEMA),
        }
        cls.frames = gold_indicators.context_frames(s, {
            "links": links(
                ("noaa_hazel", "5413", "usgs_fdsn", "usp000hvnu", "caused_by", "accepted"),
                ("ptwc", "cap-alert", "usgs_realtime", "us-alert", "reports_on", "accepted"),
                ("ptwc", "cap-minor", "usgs_realtime", "us7000ty16", "reports_on", "accepted"),
            ),
            "runs": pd.DataFrame([
                ("usgs_realtime", "incremental", "success", NOW - timedelta(hours=5), NOW - timedelta(hours=5)),
                ("usgs_realtime", "incremental", "failed", NOW - timedelta(hours=1), None),
            ], columns=["source", "mode", "status", "started_at", "finished_at"]),
            "state": pd.DataFrame([("usgs_realtime", (NOW - timedelta(hours=6)).isoformat())],
                                  columns=["source", "watermark"]),
            "dag_runs": pd.DataFrame(
                [("gold_processing", "success", NOW - timedelta(days=d)) for d in range(3)]
                + [("gold_processing", "failed", NOW - timedelta(days=4))],
                columns=["dag_id", "state", "start_date"]),
        })
        cls.relevance = {row.external_id: row for row in
                         gold_indicators.tsunami_relevance(cls.current, cls.frames["links"], RULES).collect()}

    def test_relevance_follows_the_workshop1_rule(self):
        tohoku = self.relevance["usp000hvnu"]
        self.assertEqual((tohoku.magnitude_points, tohoku.depth_point, tohoku.history_point), (3, 1, 1))
        self.assertEqual((tohoku.relevance_points, tohoku.relevance_level), (5, "HIGH"))
        self.assertEqual(tohoku.history_tsunamis, 1)  # 5412 only: its own tsunami 5413 is excluded
        self.assertTrue(tohoku.tsunami_linked)

    def test_deep_earthquakes_are_at_most_low(self):
        self.assertEqual(self.relevance["us6000u4r9"].relevance_level, "LOW")

    def test_linked_moderate_bulletin_is_an_official_alert(self):
        alert = self.relevance["us-alert"]
        self.assertEqual((alert.bulletin_source, alert.bulletin_severity), ("ptwc", "Moderate"))
        self.assertEqual(alert.relevance_level, "OFFICIAL ALERT")
        self.assertNotIn("us7000ty16", self.relevance)  # below M6: no level

    def test_recent_map_keeps_the_pacific_area_and_links_bulletins_at_any_magnitude(self):
        parts = self.gi.storytelling_indicators(self.current, self.gi.tsunami_relevance(
            self.current, self.frames["links"], RULES), self.frames["links"], RULES, SETTINGS)
        story = pd.concat([part.toPandas() for part in parts], ignore_index=True)
        recent = story[story["metric"].eq("recent_earthquakes")].set_index("external_id")
        self.assertEqual(set(recent.index), {"us6000u4r9", "us-alert", "us7000ty16"})
        self.assertEqual(recent.loc["us7000ty16", "bulletin_severity"], "Minor")
        self.assertTrue(pd.isna(recent.loc["us7000ty16", "relevance_level"]))
        self.assertEqual(recent.loc["us-alert", "region_cell"], "lat -20..-10, lon -180..-170")
        baseline = story[story["metric"].eq("regional_baseline")].set_index("region_cell")
        self.assertEqual(baseline.loc["lat 30..40, lon 140..150", ["count", "historical_tsunamis"]].tolist(), [1, 2])
        validation = story[story["metric"].eq("relevance_validation")]
        self.assertEqual(validation.set_index("tsunami_linked").loc[True, "value"], 100.0)

    def test_governance_pipeline_state_orchestration_and_matching(self):
        parts = self.gi.governance_indicators(self.current, self.frames, NOW, SETTINGS, MATCHING, with_database=True)
        gov = pd.concat([part.toPandas() for part in parts], ignore_index=True)
        state = gov[gov["topic"].eq("pipeline_state")].set_index("source").loc["usgs_realtime"]
        self.assertEqual((state.watermark_age_hours, state.load_runs_window, state.failed_load_runs_window),
                         (6.0, 2, 1))
        runs = gov[gov["topic"].eq("orchestration")].set_index("dag_id")
        self.assertEqual(runs.loc["gold_processing", ["expected_dag_runs", "successful_dag_runs",
                                                      "failed_dag_runs"]].tolist(), [7, 3, 1])
        self.assertEqual(runs.loc["incremental_ingestion", "run_compliance_pct"], 0.0)
        match = gov[gov["topic"].eq("matching")].set_index(["source", "relation_type"])
        self.assertEqual(match.loc[("noaa_hazel", "caused_by"), ["eligible", "accepted_links", "unmatched"]].tolist(),
                         [2, 1, 1])  # landslide 9000 is not eligible
        self.assertEqual(match.loc[("ntwc", "reports_on"), "coverage_pct"], 0.0)
        nulls = gov[gov["topic"].eq("null_rate")].set_index(["source", "field"])
        self.assertAlmostEqual(nulls.loc[("noaa_hazel", "latitude"), "null_rate_pct"], 33.33)


if __name__ == "__main__":
    unittest.main()
