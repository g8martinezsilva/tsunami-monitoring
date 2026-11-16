"""Project configuration: source definitions from config/sources.yaml."""

import os
from functools import cache
from pathlib import Path

import yaml

# Repository root locally; /opt/airflow/project inside the Airflow containers
PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parent.parent))


@cache
def load_sources():
    """Return the sources of config/sources.yaml indexed by their id."""
    with open(PROJECT_DIR / "config" / "sources.yaml", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return {source["id"]: source for source in config["sources"]}
