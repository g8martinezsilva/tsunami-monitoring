"""Project configuration: source definitions and event-matching rules from config/sources.yaml."""

import os
from functools import cache
from pathlib import Path

import yaml

# Repository root locally; /opt/airflow/project inside the Airflow containers
PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parent.parent))


@cache
def load_config():
    """The whole config/sources.yaml."""
    with open(PROJECT_DIR / "config" / "sources.yaml", encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_sources():
    """Return the sources of config/sources.yaml indexed by their id."""
    return {source["id"]: source for source in load_config()["sources"]}


def load_matching_rules():
    """Thresholds of the event identification across sources (event_matching block)."""
    return load_config()["event_matching"]
