"""Storytelling dashboard (dashboard/storytelling_app.py). Run with the project venv:
python -m unittest tests.test_workshop4_storytelling_app
"""

import unittest
from datetime import datetime, timedelta, timezone
from importlib.util import find_spec
from pathlib import Path

import pandas as pd

PUBLISHED = datetime(2026, 11, 28, 4, 9, 49, tzinfo=timezone.utc)


def pair_with(recent_rows, comparison=None):
    from dashboard.gold import STORYTELLING_COLUMNS, GoldPair

    rows = [{"metric": "recent_earthquakes", **row} for row in recent_rows]
    if comparison:
        rows.append({"metric": "historical_vs_current", "region_cell": "All Pacific query area", **comparison})
    frame = pd.DataFrame(rows).reindex(columns=STORYTELLING_COLUMNS)
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    pair = GoldPair("20261128_040949", Path("g"), Path("s"), True)
    pair.storytelling, pair.governance = frame, pd.DataFrame()
    return pair


def quake(hours_ago, magnitude, level=None, bulletin=None):
    return {"event_time": PUBLISHED - timedelta(hours=hours_ago), "magnitude": magnitude, "relevance_level": level,
            "bulletin_source": bulletin, "place": "Somewhere", "latitude": 0.0, "longitude": 170.0, "depth_km": 10.0}


@unittest.skipUnless(find_spec("dash"), "pip install -r requirements.txt")
class StorytellingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from dashboard import storytelling_app
        cls.app = storytelling_app

    def test_window_counts_back_from_the_gold_publication(self):
        pair = pair_with([quake(2, 4.8), quake(30, 6.2, "LOW"), quake(24 * 8, 5.0)])
        self.assertEqual(len(self.app.recent(pair, "24h")), 1)
        self.assertEqual(len(self.app.recent(pair, "7d")), 2)
        self.assertEqual(self.app.recent(pair, "24h").loc[0, "level"], "NOT RATED")

    def test_main_message_wording_follows_the_rules(self):
        pair = pair_with([quake(2, 6.4, "MODERATE", "ptwc"), quake(3, 4.7)],
                         {"count": 12, "expected_count": 9.902, "p_value": 0.29, "window_days": 30})
        first, second, third = self.app.main_message(pair, "24h")
        self.assertIn("2 earthquakes", first)
        self.assertIn("1 deserves a closer look and 1 has a linked official bulletin", first)
        self.assertIn("not unusually frequent: 12, compared with about 9.9 expected", second)
        self.assertIn("not a tsunami prediction", third)

    def test_busier_than_usual_only_below_five_percent(self):
        pair = pair_with([], {"count": 20, "expected_count": 9.9, "p_value": 0.003, "window_days": 30})
        first, second, _ = self.app.main_message(pair, "7d")
        self.assertIn("no earthquakes (magnitude 4.5 or higher) were recorded", first)
        self.assertIn("more frequent than usual", second)

    def test_missing_comparison_is_stated(self):
        _, second, _ = self.app.main_message(pair_with([quake(1, 5.0)]), "24h")
        self.assertIn("not available", second)

    def test_bulletins_first_then_level_and_magnitude(self):
        pair = pair_with([quake(1, 6.9, "HIGH"), quake(2, 5.1, None, "ntwc"), quake(3, 6.1, "LOW")])
        rows = self.app.bulletin_rows(pair, "24h")
        self.assertEqual([r["Level"] for r in rows], ["Not Rated", "High", "Low"])
        self.assertTrue(rows[0]["Official bulletin"].startswith("NTWC"))
        self.assertEqual(rows[1]["Official bulletin"], "None linked")

    def test_every_chart_handles_an_empty_publication(self):
        pair = pair_with([])
        for build in (lambda: self.app.recent_map(pair, "7d"), lambda: self.app.daily_activity(pair),
                      lambda: self.app.busier_than_usual(pair)[0], lambda: self.app.common_regions(pair),
                      lambda: self.app.magnitudes(pair), lambda: self.app.tsunamis_per_year(pair)):
            self.assertTrue(build().layout.annotations)  # explanatory message instead of an error


if __name__ == "__main__":
    unittest.main()
