"""Gold layer access for both dashboards (Workshop 4).

The dashboards read only the Gold Parquet files, never Silver, the database or a source API.
`latest_pair()` finds the newest complete pair: a governance and a storytelling dataset with the
same UTC stamp, each one a Parquet file or a Spark directory with its _SUCCESS marker. On a fresh
clone without local Gold runs it falls back to the committed samples. GOLD_DIR overrides the folder.

`GoldCache.get()` checks the folder on every call but reads the Parquet files again only when a
newer pair appears, so the dashboards refresh cheaply (manual button or interval).
"""

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent.parent
STAMP = re.compile(r"^governance_(\d{8}_\d{6})\.parquet$")

# Columns each dashboard expects; an older Gold pair may lack the newer ones (they are added as empty)
GOVERNANCE_COLUMNS = [
    "topic", "source", "rows_processed", "unique_observations", "repeated_rows", "duplicate_rate_pct",
    "missing_coordinates", "missing_coordinates_pct", "incomplete_times", "incomplete_times_pct",
    "rejected_records", "unique_rejected_records", "iqr_field", "iqr_outliers", "field", "null_count",
    "null_rate_pct", "last_successful_run_at", "watermark_at", "watermark_age_hours", "load_runs_window",
    "failed_load_runs_window", "dag_id", "successful_dag_runs", "failed_dag_runs", "expected_dag_runs",
    "run_compliance_pct", "relation_type", "eligible", "accepted_links", "pending_links", "unmatched",
    "coverage_pct", "silver_topic", "schema_rows", "schema_compliant", "schema_compliance_pct",
    "files_checked", "files_compliant", "outlier_values", "outlier_count", "outlier_rate_pct", "period",
    "volume_rows", "new_observations", "threshold", "status",
]
STORYTELLING_COLUMNS = [
    "metric", "source", "year", "center", "magnitude_type", "bin_start", "bin_end", "unit", "count",
    "external_id", "event_time", "latitude", "longitude", "magnitude", "depth_km", "place", "region_cell",
    "relevance_level", "relevance_points", "history_tsunamis", "tsunami_linked", "bulletin_source",
    "bulletin_severity", "bulletin_sent_at", "value", "historical_tsunamis", "max_magnitude",
    "max_water_height_m", "historical_count", "expected_count", "p_value", "window_days",
]


def gold_dir():
    """GOLD_DIR (absolute, or relative to the repository root), else datalake_gold/."""
    folder = Path(os.environ.get("GOLD_DIR", "datalake_gold"))
    return folder if folder.is_absolute() else PROJECT_DIR / folder


def _complete(path):
    return path.is_file() or ((path / "_SUCCESS").is_file() and any(path.glob("part-*.parquet")))


def find_pairs(folder):
    """Complete (stamp, governance, storytelling) pairs in a folder, newest first."""
    pairs = []
    for governance in Path(folder).glob("governance_*.parquet"):
        match = STAMP.match(governance.name)
        if not match:
            continue
        storytelling = governance.with_name(f"storytelling_{match.group(1)}.parquet")
        if _complete(governance) and _complete(storytelling):
            pairs.append((match.group(1), governance, storytelling))
    return sorted(pairs, reverse=True)


@dataclass
class GoldPair:
    stamp: str
    governance_path: Path
    storytelling_path: Path
    from_samples: bool
    governance: pd.DataFrame = field(repr=False, default=None)
    storytelling: pd.DataFrame = field(repr=False, default=None)

    @property
    def published_at(self):
        return datetime.strptime(self.stamp, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)

    @property
    def label(self):
        origin = "committed sample" if self.from_samples else "latest Gold run"
        return f"Gold {self.published_at:%Y-%m-%d %H:%M} UTC ({origin})"

    def topic(self, name):
        """Governance rows of one indicator topic."""
        return self.governance[self.governance["topic"] == name].reset_index(drop=True)

    def metric(self, name):
        """Storytelling rows of one metric."""
        return self.storytelling[self.storytelling["metric"] == name].reset_index(drop=True)


def _read(path, columns):
    frame = pd.read_parquet(path)
    for column in columns:
        if column not in frame:
            frame[column] = pd.NA
    for column in [c for c in frame if c.endswith("_at") or c in {"event_time", "bulletin_sent_at"}]:
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    return frame


def latest_pair(folder=None):
    """Newest complete pair in the Gold folder, or in its samples/ subfolder when there is none."""
    folder = Path(folder) if folder else gold_dir()
    for candidate, from_samples in ((folder, False), (folder / "samples", True)):
        pairs = find_pairs(candidate)
        if pairs:
            stamp, governance, storytelling = pairs[0]
            return GoldPair(stamp, governance, storytelling, from_samples)
    raise FileNotFoundError(f"No complete Gold pair in {folder} or {folder / 'samples'}: run the gold_processing DAG")


def load(pair):
    pair.governance = _read(pair.governance_path, GOVERNANCE_COLUMNS)
    pair.storytelling = _read(pair.storytelling_path, STORYTELLING_COLUMNS)
    return pair


class GoldCache:
    """Reads a Gold pair once and again only when a newer complete pair is published."""

    def __init__(self, folder=None):
        self.folder = folder
        self.pair = None

    def get(self):
        newest = latest_pair(self.folder)
        if self.pair is None or (newest.stamp, newest.from_samples) != (self.pair.stamp, self.pair.from_samples):
            self.pair = load(newest)
        return self.pair
