"""Bronze ingestion DAG: historical load of the four sources into datalake_bronze/.

Extract (NOAA HazEL, USGS FDSN one task per year, and a snapshot of the USGS real-time,
NTWC and PTWC feeds) -> validate the saved files -> persist a JSON manifest of the run.
Every response is saved unchanged as source_YYYYMMDD_HHMMSS.ext, where the timestamp is the
run's logical date, so re-running the same DAG run overwrites the same files (idempotent).
Triggered manually for the historical load; scheduled incremental polling is in
incremental_ingestion_dag.py (Workshop 3).
"""

import json
import logging
from datetime import date, datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from src.downloads import bronze_dir
from src.bronze_checks import check_file as _check_file
from src.sources import FEED_EXTENSIONS, fetch_feed, fetch_noaa_hazel, fetch_usgs_fdsn_year

log = logging.getLogger(__name__)

STAMP_FORMAT = "%Y%m%d_%H%M%S"


def _period(params):
    start, end = date.fromisoformat(params["start"]), date.fromisoformat(params["end"])
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    return start, end


@dag(
    dag_id="bronze_ingestion",
    description="Historical load of NOAA HazEL, USGS FDSN and a feed snapshot into datalake_bronze/",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    params={
        "start": Param("2010-01-01", type="string", format="date", description="First day (UTC)"),
        "end": Param("2025-12-31", type="string", format="date", description="Last day (UTC)"),
    },
    tags=["bronze", "historical", "workshop2"],
)
def bronze_ingestion():

    @task
    def plan_years(params=None):
        start, end = _period(params)
        return list(range(start.year, end.year + 1))

    @task
    def extract_noaa_hazel(params=None, logical_date=None):
        start, end = _period(params)
        return fetch_noaa_hazel(start, end, logical_date.strftime(STAMP_FORMAT))

    @task
    def extract_usgs_fdsn(year, params=None, logical_date=None):
        start, end = _period(params)
        return fetch_usgs_fdsn_year(year, start, end, logical_date.strftime(STAMP_FORMAT))

    @task
    def extract_feed(source, logical_date=None):
        return fetch_feed(source, logical_date.strftime(STAMP_FORMAT))

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def validate(noaa, usgs_years, feeds):
        """Check every saved file; a failed extraction arrives as a missing result."""
        summaries = [noaa, *(usgs_years or []), *(feeds or [])]
        checked, problems = [], []
        for summary in summaries:
            if not summary:
                problems.append("an extraction task failed (see its log)")
                continue
            for path in summary["files"]:
                problem = _check_file(path)
                if problem:
                    problems.append(f"{path}: {problem}")
            checked.append(summary)
            log.info("%s: %d records in %d file(s)", summary["source"], summary["records"], len(summary["files"]))
        return {"summaries": checked, "problems": problems}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def persist_manifest(result, params=None, logical_date=None, run_id=None):
        """Write the run manifest (files, record counts, problems) next to the Bronze files."""
        stamp = logical_date.strftime(STAMP_FORMAT)
        manifest = {
            "run_id": run_id,
            "logical_date": logical_date.isoformat(),
            "period": {"start": params["start"], "end": params["end"]},
            "sources": result["summaries"] if result else [],
            "problems": result["problems"] if result else ["validation did not run"],
        }
        path = bronze_dir() / f"bronze_manifest_{stamp}.json"
        path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        log.info("manifest saved: %s", path)
        if manifest["problems"]:
            raise RuntimeError(f"{len(manifest['problems'])} problem(s): {manifest['problems']}")
        # Arrival marker watched by the Silver DAG's FileSensor; Silver removes it once processed
        marker = bronze_dir() / "_new" / path.name
        marker.parent.mkdir(exist_ok=True)
        marker.write_text(str(path), encoding="utf-8")
        return str(path)

    noaa = extract_noaa_hazel()
    usgs_years = extract_usgs_fdsn.expand(year=plan_years())
    feeds = extract_feed.expand(source=list(FEED_EXTENSIONS))
    persist_manifest(validate(noaa, usgs_years, feeds))


bronze_ingestion()
