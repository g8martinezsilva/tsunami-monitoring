"""Shared dashboard code (dashboard/gold.py and dashboard/theme.py). Run with the project venv:
python -m unittest tests.test_workshop4_dashboard_common
"""

import os
import unittest
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pandas as pd


def write_pair(folder, stamp, complete=True, extra=None):
    """A Spark-style pair: one directory per dataset with a part file and, if complete, _SUCCESS."""
    for name, frame in (("governance", pd.DataFrame({"topic": ["earthquakes"], "source": ["usgs_fdsn"], **(extra or {})})),
                        ("storytelling", pd.DataFrame({"metric": ["recent_earthquakes"], "count": [1]}))):
        directory = Path(folder) / f"{name}_{stamp}.parquet"
        directory.mkdir(parents=True)
        frame.to_parquet(directory / "part-00000.snappy.parquet")
        if complete:
            (directory / "_SUCCESS").touch()


@unittest.skipUnless(find_spec("dash"), "pip install -r requirements.txt")
class GoldLoadingTests(unittest.TestCase):
    def setUp(self):
        from dashboard import gold
        self.gold = gold
        self.root = Path(self.enterContext(TemporaryDirectory()))

    def test_newest_complete_pair_wins(self):
        write_pair(self.root, "20261120_100000")
        write_pair(self.root, "20261121_100000")
        write_pair(self.root, "20261122_100000", complete=False)  # still being written by Spark
        pair = self.gold.latest_pair(self.root)
        self.assertEqual((pair.stamp, pair.from_samples), ("20261121_100000", False))

    def test_samples_are_the_fallback_on_a_fresh_clone(self):
        write_pair(self.root / "samples", "20261121_142619")
        pair = self.gold.latest_pair(self.root)
        self.assertTrue(pair.from_samples)
        self.assertIn("committed sample", pair.label)

    def test_no_pair_explains_what_to_run(self):
        with self.assertRaisesRegex(FileNotFoundError, "gold_processing"):
            self.gold.latest_pair(self.root)

    def test_older_pairs_get_the_newer_columns_as_empty(self):
        write_pair(self.root, "20261121_100000")
        pair = self.gold.load(self.gold.latest_pair(self.root))
        self.assertTrue({"status", "schema_compliance_pct", "duplicate_rate_pct"} <= set(pair.governance))
        self.assertTrue(pair.governance["status"].isna().all())
        box_columns = ["q1", "median", "q3", "iqr", "lower_whisker", "upper_whisker", "outlier_points"]
        self.assertTrue(pair.governance[box_columns].isna().all().all())
        self.assertEqual(len(pair.topic("earthquakes")), 1)
        self.assertEqual(len(pair.metric("recent_earthquakes")), 1)

    def test_cache_reloads_only_when_a_newer_pair_appears(self):
        write_pair(self.root, "20261121_100000")
        cache = self.gold.GoldCache(self.root)
        first = cache.get()
        self.assertIs(cache.get(), first)
        write_pair(self.root, "20261122_100000")
        self.assertEqual(cache.get().stamp, "20261122_100000")

    def test_gold_dir_reads_the_environment(self):
        with mock.patch.dict(os.environ, {"GOLD_DIR": "datalake_gold/samples"}):
            self.assertEqual(self.gold.gold_dir(), self.gold.PROJECT_DIR / "datalake_gold" / "samples")


@unittest.skipUnless(find_spec("dash"), "pip install -r requirements.txt")
class ThemeTests(unittest.TestCase):
    def test_every_level_source_and_status_has_a_colour(self):
        from dashboard import theme
        self.assertEqual(set(theme.LEVEL_ORDER), set(theme.LEVEL_COLORS))
        self.assertEqual(set(theme.SOURCE_COLORS), set(theme.SOURCE_NAMES))
        self.assertTrue({"ok", "warning", "review", "expected", "n/a"} <= set(theme.STATUS_COLORS))

    def test_number_format(self):
        from dashboard.theme import fmt
        self.assertEqual(fmt(15632), "15,632")
        self.assertEqual(fmt(88.112, 1, " %"), "88.1 %")
        self.assertEqual(fmt(None), "—")
        self.assertEqual(fmt(float("nan")), "—")


if __name__ == "__main__":
    unittest.main()
