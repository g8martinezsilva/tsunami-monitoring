"""HTTP downloads with retries, and the writer for the Bronze layer (raw landing zone)."""

import logging
import os
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

USER_AGENT = os.environ.get("HTTP_USER_AGENT", "UDistrital-Team8-Tsunami-Monitoring/1.0")
ATTEMPTS = 3  # Team policy: three attempts with increasing waits


def bronze_dir():
    """Bronze folder: datalake_bronze/ unless RAW_DIR says otherwise (one place to change it)."""
    return Path(os.environ.get("RAW_DIR", "datalake_bronze"))


def get_with_retries(url, **params):
    """GET a URL, retrying with exponential backoff (2 s, 4 s) before failing."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            if attempt == ATTEMPTS:
                raise
            wait = 2 ** attempt
            log.warning("attempt %d failed (%s); retrying in %d s", attempt, error, wait)
            time.sleep(wait)


def save_raw(source, content, extension, stamp, suffix=""):
    """Store a response unchanged in the Bronze layer as source_YYYYMMDD_HHMMSS.ext."""
    folder = bronze_dir()
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{source}{suffix}_{stamp}.{extension}"
    path.write_bytes(content)
    log.info("saved %s (%s bytes)", path, f"{len(content):,}")
    return path
