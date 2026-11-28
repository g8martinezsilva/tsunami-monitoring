"""Gold additions for the Workshop 4 dashboards (integration adjustments).

Governance (dashboard/governance_app.py):
- schema_compliance: current observations that meet the Silver contract (required fields and valid
  ranges), and Silver files that contain the required columns.
- outlier_rate: share of current observations beyond the IQR fences, per numeric field and source.
- volume: Silver rows processed and new observations (first capture of a key) per day and source.
- a threshold and an ok / warning status on every indicator that has one (config: gold_dashboard).

Storytelling (dashboard/storytelling_app.py):
- historical_vs_current: M>=6 earthquakes per region in the last N days against the count expected
  from the 2010-2025 rate in a window of the same length, with a Poisson upper-tail probability.
  Both sides use the same magnitude threshold and the same Pacific query area. The comparison
  describes activity; it is not a forecast.

Every threshold and window is a parameter in config/sources.yaml.
"""

import math
from datetime import date, timedelta

import pyarrow.parquet as pq
from pyspark.sql import functions as F

from src.gold_indicators import in_historical_period, in_query_area, region_cell

ALL_AREA = "All Pacific query area"


# ---------------------------------------------------------------------------
# Governance
# ---------------------------------------------------------------------------

def _file_compliant(path, required):
    try:
        names = set(pq.read_schema(path).names)
    except Exception:  # unreadable file: not compliant
        return False
    return set(required) <= names


def schema_compliance(current, paths, settings):
    """Row and file compliance with the Silver contract, per Silver topic and source."""
    parts = []
    for topic, rules in settings["schema"].items():
        frame = current.get(topic)
        if frame is None:
            continue
        files = paths.get(topic, [])
        files_ok = sum(_file_compliant(path, rules["required"]) for path in files)
        valid = F.lit(True)
        for field in rules["required"]:
            column = F.col(field) if field in frame.columns else F.lit(None)
            valid = valid & column.isNotNull()
        for field, (low, high) in rules.get("ranges", {}).items():
            if field in frame.columns:  # missing optional values are allowed; present ones must be in range
                valid = valid & (F.col(field).isNull() | F.col(field).between(low, high))
        parts.append(
            frame.groupBy("source").agg(F.count("*").alias("schema_rows"),
                                        F.sum(valid.cast("long")).alias("schema_compliant"))
            .withColumn("schema_compliance_pct", F.round(100.0 * F.col("schema_compliant") / F.col("schema_rows"), 2))
            .withColumn("files_checked", F.lit(len(files)).cast("long"))
            .withColumn("files_compliant", F.lit(files_ok).cast("long"))
            .withColumn("silver_topic", F.lit(topic))
            .withColumn("topic", F.lit("schema_compliance")))
    return parts


def outlier_rates(current, settings):
    """IQR outlier rates and box plot statistics over current observations, per field, topic and source."""
    parts = []
    for topic, fields in settings["outlier_fields"].items():
        frame = current.get(topic)
        if frame is None:
            continue
        for field in [f for f in fields if f in frame.columns]:
            values = frame.select("source", F.col(field).cast("double").alias("v")).where(
                F.col("v").isNotNull() & ~F.isnan("v"))
            fences = (values.groupBy("source")
                      .agg(F.percentile_approx("v", [0.25, 0.5, 0.75], 10000).alias("q"))
                      .select("source", F.col("q")[0].alias("q1"), F.col("q")[1].alias("median"),
                              F.col("q")[2].alias("q3"))
                      .withColumn("iqr", F.col("q3") - F.col("q1")))
            beyond = (F.col("v") < F.col("q1") - 1.5 * F.col("iqr")) | (F.col("v") > F.col("q3") + 1.5 * F.col("iqr"))
            inliers = F.when(~beyond, F.col("v"))
            parts.append(
                values.join(fences, "source")
                .groupBy("source", "q1", "median", "q3", "iqr")
                .agg(F.count("*").alias("outlier_values"),
                     F.sum(beyond.cast("long")).alias("outlier_count"),
                     F.min(inliers).alias("lower_whisker"),
                     F.max(inliers).alias("upper_whisker"),
                     F.sort_array(F.collect_list(F.when(beyond, F.col("v")))).alias("outlier_points"))
                .withColumn("outlier_rate_pct", F.round(100.0 * F.col("outlier_count") / F.col("outlier_values"), 2))
                .withColumn("field", F.lit(field))
                .withColumn("silver_topic", F.lit(topic))
                .withColumn("topic", F.lit("outlier_rate")))
    return parts


def volume_over_time(raw, snapshot_at):
    """Per day and source: Silver rows processed, and observations captured for the first time."""
    parts = []
    for topic, frame in raw.items():
        stamped = frame.withColumn("_captured", snapshot_at(frame)).where(F.col("_captured").isNotNull())
        processed = (stamped.groupBy("source", F.to_date("_captured").alias("period"))
                     .agg(F.count("*").alias("volume_rows")))
        first = (stamped.groupBy("source", "external_id").agg(F.min("_captured").alias("_first"))
                 .groupBy("source", F.to_date("_first").alias("period"))
                 .agg(F.count("*").alias("new_observations")))
        parts.append(
            processed.join(first, ["source", "period"], "left")
            .withColumn("new_observations", F.coalesce(F.col("new_observations"), F.lit(0)).cast("long"))
            .withColumn("silver_topic", F.lit(topic))
            .withColumn("topic", F.lit("volume")))
    return parts


def apply_thresholds(governance, settings):
    """Add the threshold that applies to each indicator row and its status."""
    t = settings["thresholds"]
    structural = F.concat_ws(".", F.col("source"), F.col("field")).isin(t["structural_nulls"])
    age_limit = F.create_map(*[F.lit(v) for pair in t["watermark_age_hours_max"].items() for v in pair])
    topic = F.col("topic")
    threshold = (F.when(topic == "null_rate", F.lit(float(t["null_rate_pct_max"])))
                 .when(topic == "schema_compliance", F.lit(float(t["schema_compliance_pct_min"])))
                 .when(topic == "outlier_rate", F.lit(float(t["outlier_rate_pct_max"])))
                 .when(topic == "pipeline_state", F.element_at(age_limit, F.col("source")).cast("double"))
                 .when(topic == "orchestration", F.lit(float(t["run_compliance_pct_min"])))
                 .when(topic == "matching", F.lit(float(t["coverage_pct_min"]))))
    status = (F.when((topic == "null_rate") & structural, "expected")
              .when(topic == "null_rate", F.when(F.col("null_rate_pct") > F.col("threshold"), "warning").otherwise("ok"))
              .when(topic == "schema_compliance",
                    F.when((F.col("schema_compliance_pct") < F.col("threshold"))
                           | (F.col("files_compliant") < F.col("files_checked")), "warning").otherwise("ok"))
              .when(topic == "outlier_rate",
                    F.when(F.col("outlier_rate_pct") > F.col("threshold"), "review").otherwise("ok"))
              .when(topic == "pipeline_state",
                    F.when(F.col("threshold").isNull() | F.col("watermark_age_hours").isNull(), "n/a")
                    .when(F.col("watermark_age_hours") > F.col("threshold"), "warning").otherwise("ok"))
              .when(topic == "orchestration",
                    F.when(F.col("run_compliance_pct") < F.col("threshold"), "warning").otherwise("ok"))
              .when(topic == "matching",
                    F.when(F.col("coverage_pct") < F.col("threshold"), "warning").otherwise("ok")))
    return governance.withColumn("threshold", threshold).withColumn("status", status)


# ---------------------------------------------------------------------------
# Storytelling
# ---------------------------------------------------------------------------

def poisson_upper_tail(k, expected):
    """P(X >= k) for X ~ Poisson(expected): how unusual k events are for that expected count."""
    if expected <= 0:
        return None
    term, below = math.exp(-expected), 0.0
    for i in range(k):
        below += term
        term *= expected / (i + 1)
    return max(0.0, 1.0 - below)


def historical_vs_current(current, rules, gold_settings, settings, now):
    """Recent M>=min earthquakes per region against the 2010-2025 rate for a window of the same length."""
    eq = current.get("earthquakes")
    if eq is None:
        return []
    spark = eq.sparkSession
    days = settings["current_window_days"]
    degrees = gold_settings["region_cell_degrees"]
    first, last = gold_settings["historical_years"]
    period_days = (date(last, 12, 31) - date(first, 1, 1)).days + 1
    strong = (F.col("magnitude") >= rules["min_magnitude"]) & in_query_area(F.col("latitude"), F.col("longitude"))
    cell = region_cell(F.col("latitude"), F.col("longitude"), degrees)

    recent = (eq.where((F.col("source") == "usgs_realtime") & strong
                       & F.col("event_time").between(F.lit(now - timedelta(days=days)).cast("timestamp"),
                                                     F.lit(now).cast("timestamp")))
              .groupBy(cell.alias("region_cell")).count().collect())
    history = (eq.where((F.col("source") == "usgs_fdsn") & strong & in_historical_period(F.col("event_time"), gold_settings))
               .groupBy(cell.alias("region_cell")).count().collect())
    counts = {}
    for row in history:
        counts.setdefault(row.region_cell, [0, 0])[0] = row["count"]
    for row in recent:
        counts.setdefault(row.region_cell, [0, 0])[1] = row["count"]
    if not counts:
        return []
    counts[ALL_AREA] = [sum(c[0] for c in counts.values()), sum(c[1] for c in counts.values())]

    rows = []
    for name, (historical, now_count) in counts.items():
        expected = historical * days / period_days
        rows.append((name, int(now_count), int(historical), round(expected, 3),
                     round(now_count / expected, 2) if expected > 0 else None,
                     poisson_upper_tail(int(now_count), expected), days))
    frame = spark.createDataFrame(rows, "region_cell STRING, count BIGINT, historical_count BIGINT, "
                                        "expected_count DOUBLE, value DOUBLE, p_value DOUBLE, window_days INT")
    return [frame.withColumn("p_value", F.round("p_value", 4))
            .withColumn("metric", F.lit("historical_vs_current"))
            .withColumn("source", F.lit("usgs_realtime vs usgs_fdsn"))
            .withColumn("unit", F.lit(f"M>={rules['min_magnitude']} earthquakes in {days} days; value = observed/expected"))]
