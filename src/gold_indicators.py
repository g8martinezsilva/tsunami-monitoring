"""Gold indicators tied to the Workshop 1 user stories and the pipeline state (Workshop 3).

Storytelling (functional user, a Pacific monitoring analyst):
- recent_earthquakes (US1, US2, US4): USGS real-time earthquakes with their map position, the
  official bulletin linked to them and their Tsunami Relevance Level.
- regional_baseline (US3): historical (2010-2025) M>=6 earthquakes and tsunamis per latitude/longitude cell.
- relevance_level_distribution and relevance_validation (US4): the indicator over the catalog and
  the two validation measures proposed in Workshop 1.

Governance (data health, US5 and US6):
- null_rate: share of missing values per field, topic and source.
- pipeline_state: last successful load and watermark age per source (from PostgreSQL).
- orchestration: successful Airflow runs against the runs expected from each DAG schedule.
- matching: how many tsunamis and bulletins are linked to an earthquake (event_relation).

The Tsunami Relevance Level is a monitoring aid, never a tsunami prediction or warning.
"""

from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

EARTH_RADIUS_KM = 6371.0
LINEAGE = {"bronze_file", "locator", "bronze_manifest", "silver_processed_at", "content_hash", "_silver_file",
           "_snapshot_rows"}

CONTEXT_SCHEMAS = {
    "links": "from_source STRING, from_external_id STRING, to_source STRING, to_external_id STRING, "
             "relation_type STRING, review_status STRING",
    "runs": "source STRING, mode STRING, status STRING, started_at TIMESTAMP, finished_at TIMESTAMP",
    "state": "source STRING, watermark STRING",
    "dag_runs": "dag_id STRING, state STRING, start_date TIMESTAMP",
}


def context_frames(spark, context):
    """pandas tables from src/gold_context.py -> Spark DataFrames (empty ones keep their schema)."""
    frames = {}
    for name, schema in CONTEXT_SCHEMAS.items():
        table = (context or {}).get(name)
        rows = [] if table is None else [
            tuple(_python(value) for value in row)
            for row in table[[field.split()[0] for field in schema.split(", ")]].astype(object).itertuples(index=False)
        ]
        frames[name] = spark.createDataFrame(rows, schema)
    return frames


def _python(value):
    """pandas/NumPy values -> plain Python values Spark accepts (NaN and NaT -> None)."""
    if value is None or value != value:
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        return value.item()
    return value


def _col(frame, name, data_type="string"):
    return F.col(name) if name in frame.columns else F.lit(None).cast(data_type)


def _distance_km(lat1, lon1, lat2, lon2):
    """Great-circle distance (haversine) between two coordinate columns."""
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def in_query_area(latitude, longitude):
    """Inside the Pacific rectangle of the USGS FDSN query (config/sources.yaml). The real-time
    feed is global, so the recent-earthquake map uses the same area as the historical catalog."""
    from src.config import load_sources

    area = load_sources()["usgs_fdsn"]["query"]
    west, east = area["minlongitude"], area["maxlongitude"]  # 100..300 crosses the antimeridian
    lon360 = F.when(longitude < 0, longitude + 360).otherwise(longitude)
    return (latitude.between(area["minlatitude"], area["maxlatitude"])
            & (lon360.between(west, east) | longitude.between(west, east)))


def in_historical_period(time, settings):
    """Inside the historical baseline years. The daily FDSN reconciliation has no end date, so the
    catalog also receives recent earthquakes: they must not be part of their own baseline."""
    first, last = settings["historical_years"]
    return F.year(time).between(first, last)


def region_cell(latitude, longitude, degrees):
    """Grid-cell label such as 'lat 30..40, lon 140..150' (lower bound included)."""
    lat_low = F.floor(latitude / degrees) * degrees
    lon_low = F.floor(longitude / degrees) * degrees
    return F.when(latitude.isNotNull() & longitude.isNotNull(), F.format_string(
        "lat %d..%d, lon %d..%d", lat_low.cast("int"), (lat_low + degrees).cast("int"),
        lon_low.cast("int"), (lon_low + degrees).cast("int")))


# ---------------------------------------------------------------------------
# Tsunami Relevance Level
# ---------------------------------------------------------------------------

def linked_bulletins(current, links):
    """Latest NTWC/PTWC bulletin linked (reports_on, accepted) to each USGS earthquake id."""
    bulletins = current.get("tsunami_bulletins")
    spark = links.sparkSession
    if bulletins is None:
        return spark.createDataFrame([], "external_id STRING, bulletin_source STRING, bulletin_severity STRING, "
                                         "bulletin_sent_at TIMESTAMP")
    reported = (links.where((F.col("relation_type") == "reports_on") & (F.col("review_status") == "accepted"))
                .join(bulletins.select(F.col("source").alias("from_source"),
                                       F.col("external_id").alias("from_external_id"),
                                       _col(bulletins, "cap_severity").alias("bulletin_severity"),
                                       F.col("sent_at").alias("bulletin_sent_at")),
                      ["from_source", "from_external_id"]))
    return (reported.withColumn("_rank", F.row_number().over(
                Window.partitionBy("to_external_id").orderBy(F.col("bulletin_sent_at").desc_nulls_last())))
            .where("_rank = 1")
            .select(F.col("to_external_id").alias("external_id"),
                    F.col("from_source").alias("bulletin_source"), "bulletin_severity", "bulletin_sent_at"))


def tsunami_relevance(current, links, rules):
    """Score every earthquake with M >= min_magnitude (Workshop 1, Indicator Proposal)."""
    if "earthquakes" not in current:
        return None
    eq = current["earthquakes"]
    quakes = eq.where(F.col("magnitude") >= rules["min_magnitude"]).select(
        "source", "external_id", "event_time", "latitude", "longitude", "magnitude",
        _col(eq, "magnitude_type").alias("magnitude_type"), _col(eq, "depth_km", "double").alias("depth_km"),
        _col(eq, "place").alias("place"))

    magnitude_points = F.lit(0)
    for lower, points in sorted(rules["magnitude_points"]):
        magnitude_points = F.when(F.col("magnitude") >= lower, F.lit(points)).otherwise(magnitude_points)
    quakes = (quakes.withColumn("magnitude_points", magnitude_points)
              .withColumn("depth_point", F.when(F.col("depth_km") <= rules["shallow_depth_km"], 1).otherwise(0)))

    # Links are matched on the USGS id: the real-time and FDSN copies of an earthquake share it
    caused_by = links.where(F.col("relation_type") == "caused_by")
    own_tsunamis = caused_by.select(F.col("from_external_id").alias("tsunami_id"),
                                    F.col("to_external_id").alias("external_id"))
    linked = (caused_by.where(F.col("review_status") == "accepted")
              .select(F.col("to_external_id").alias("external_id")).distinct()
              .withColumn("tsunami_linked", F.lit(True)))

    history = F.lit(0).cast("long")
    if "tsunami_events" in current:
        ts = current["tsunami_events"]
        candidates = ts.where(
            _col(ts, "event_validity", "long").isin(rules["history_validity_codes"])
            & _col(ts, "cause_code", "long").isin(rules["history_cause_codes"])
            & F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
        ).select(F.col("external_id").alias("tsunami_id"), F.col("latitude").alias("t_lat"),
                 F.col("longitude").alias("t_lon"),
                 F.coalesce(F.col("event_time"), _col(ts, "event_date", "date").cast("timestamp")).alias("t_time"))
        nearby = (quakes.select("external_id", "event_time", "latitude", "longitude")
                  .join(F.broadcast(candidates), how="cross")
                  # Earlier tsunamis only: an earthquake's own tsunami is not part of its history
                  .where(F.col("t_time") < F.col("event_time") - F.expr("INTERVAL 1 HOUR"))
                  .where(_distance_km(F.col("latitude"), F.col("longitude"), F.col("t_lat"), F.col("t_lon"))
                         <= rules["history_radius_km"])
                  .join(own_tsunamis, ["tsunami_id", "external_id"], "left_anti")
                  .groupBy("external_id").agg(F.countDistinct("tsunami_id").alias("history_tsunamis")))
        quakes = quakes.join(nearby, "external_id", "left")
        history = F.coalesce(F.col("history_tsunamis"), F.lit(0)).cast("long")
    quakes = (quakes.withColumn("history_tsunamis", history)
              .withColumn("history_point", F.when(F.col("history_tsunamis") > 0, 1).otherwise(0))
              .withColumn("relevance_points",
                          F.col("magnitude_points") + F.col("depth_point") + F.col("history_point")))

    level = (F.when(F.col("depth_km") > rules["deep_limit_km"], "LOW")
             .when(F.col("relevance_points") >= rules["high_points"], "HIGH")
             .when(F.col("relevance_points") >= rules["moderate_points"], "MODERATE")
             .otherwise("LOW"))
    quakes = quakes.withColumn("relevance_level", level)

    quakes = quakes.join(linked_bulletins(current, links), "external_id", "left").withColumn(
        "relevance_level",
        F.when(F.col("bulletin_severity").isin(rules["official_alert_severities"]), "OFFICIAL ALERT")
        .otherwise(F.col("relevance_level")))

    return (quakes.join(linked, "external_id", "left")
            .withColumn("tsunami_linked", F.coalesce(F.col("tsunami_linked"), F.lit(False))))


# ---------------------------------------------------------------------------
# Storytelling
# ---------------------------------------------------------------------------

def storytelling_indicators(current, relevance, links, rules, settings):
    parts = []
    degrees = settings["region_cell_degrees"]

    if "earthquakes" in current:
        eq = current["earthquakes"]
        scored = (relevance.select("source", "external_id", "relevance_level", "relevance_points",
                                   "history_tsunamis", "tsunami_linked")
                  if relevance is not None else None)
        recent = eq.where((F.col("source") == "usgs_realtime")
                          & (F.col("magnitude") >= settings["recent_min_magnitude"])
                          & in_query_area(F.col("latitude"), F.col("longitude"))).select(
            "source", "external_id", "event_time", "latitude", "longitude", "magnitude",
            _col(eq, "magnitude_type").alias("magnitude_type"), _col(eq, "depth_km", "double").alias("depth_km"),
            _col(eq, "place").alias("place"))
        if scored is not None:
            recent = recent.join(scored, ["source", "external_id"], "left")
        recent = recent.join(linked_bulletins(current, links), "external_id", "left")  # US2, any magnitude
        parts.append(recent.withColumn("region_cell", region_cell(F.col("latitude"), F.col("longitude"), degrees))
                     .withColumn("metric", F.lit("recent_earthquakes")).withColumn("count", F.lit(1).cast("long")))

        # US3: historical baseline per region cell (FDSN catalog and HazEL tsunamis)
        historical = (eq.where((F.col("source") == "usgs_fdsn") & (F.col("magnitude") >= rules["min_magnitude"])
                               & in_historical_period(F.col("event_time"), settings))
                      .withColumn("region_cell", region_cell(F.col("latitude"), F.col("longitude"), degrees))
                      .groupBy("region_cell").agg(F.count("*").alias("count"),
                                                  F.max("magnitude").alias("max_magnitude")))
        if "tsunami_events" in current:
            ts = current["tsunami_events"]
            ts_time = F.coalesce(_col(ts, "event_date", "date").cast("timestamp"), F.col("event_time"))
            tsunamis = (ts.where(_col(ts, "event_validity", "long").isin(rules["history_validity_codes"])
                                 & in_historical_period(ts_time, settings))
                        .withColumn("region_cell", region_cell(F.col("latitude"), F.col("longitude"), degrees))
                        .where(F.col("region_cell").isNotNull())
                        .groupBy("region_cell").agg(F.count("*").alias("historical_tsunamis"),
                                                    F.max("max_water_height").alias("max_water_height_m")))
            historical = historical.join(tsunamis, "region_cell", "full_outer")
        parts.append(historical.where(F.col("region_cell").isNotNull())
                     .withColumn("count", F.coalesce(F.col("count"), F.lit(0)).cast("long"))
                     .withColumn("source", F.lit("usgs_fdsn+noaa_hazel"))
                     .withColumn("metric", F.lit("regional_baseline")))

    if relevance is not None:
        # The FDSN catalog counts only its historical period; real-time keeps every current event
        scored = relevance.where((F.col("source") != "usgs_fdsn")
                                 | in_historical_period(F.col("event_time"), settings))
        parts.append(scored.groupBy("source", "relevance_level").count()
                     .withColumn("metric", F.lit("relevance_level_distribution")))
        # Workshop 1 validation measures, only meaningful once tsunamis are linked to earthquakes
        if links.where(F.col("relation_type") == "caused_by").limit(1).count():
            catalog = scored.where(F.col("source") == "usgs_fdsn")
            raised = F.col("relevance_level").isin("MODERATE", "HIGH", "OFFICIAL ALERT")
            high = F.col("relevance_level").isin("HIGH", "OFFICIAL ALERT")
            validation = catalog.groupBy("tsunami_linked").agg(
                F.count("*").alias("count"),
                F.round(100.0 * F.avg(F.when(F.col("tsunami_linked"), raised).otherwise(high).cast("double")), 2)
                .alias("value"))
            parts.append(validation.select(
                F.lit("usgs_fdsn").alias("source"), "count", "value", "tsunami_linked",
                F.lit("relevance_validation").alias("metric"),
                F.when(F.col("tsunami_linked"), "tsunami-linked rated MODERATE or higher (%)")
                .otherwise("not linked rated HIGH or higher (%)").alias("unit")))
    return parts


# ---------------------------------------------------------------------------
# Governance
# ---------------------------------------------------------------------------

def null_rates(current):
    """Share of missing values (null or NaN) per field, topic and source."""
    parts = []
    for topic, frame in current.items():
        fields = [name for name, kind in frame.dtypes
                  if name not in LINEAGE and name not in {"source", "external_id"} and not name.endswith("_outlier")]
        if not fields:
            continue
        numeric = {name for name, kind in frame.dtypes if kind in {"double", "float"}}
        missing = [F.sum((F.col(f).isNull() | (F.isnan(f) if f in numeric else F.lit(False))).cast("long"))
                   .alias(f) for f in fields]
        totals = frame.groupBy("source").agg(F.count("*").alias("_rows"), *missing)
        parts.append(totals.unpivot(["source", "_rows"], fields, "field", "null_count")
                     .withColumn("null_rate_pct", F.round(100.0 * F.col("null_count") / F.col("_rows"), 2))
                     .drop("_rows").withColumn("topic", F.lit("null_rate")))
    return parts


def pipeline_state(frames, now, window_days):
    runs, state = frames["runs"], frames["state"]
    since = F.lit(now - timedelta(days=window_days)).cast("timestamp")
    loads = runs.groupBy("source").agg(
        F.max(F.when(F.col("status") == "success", F.col("finished_at"))).alias("last_successful_run_at"),
        F.sum((F.col("started_at") >= since).cast("long")).alias("load_runs_window"),
        F.sum(((F.col("started_at") >= since) & (F.col("status") == "failed")).cast("long"))
        .alias("failed_load_runs_window"))
    marks = state.select("source", F.to_timestamp("watermark").alias("watermark_at"))
    return (loads.join(marks, "source", "full_outer")
            .withColumn("watermark_age_hours",
                        F.round((F.lit(now).cast("timestamp").cast("double") - F.col("watermark_at").cast("double"))
                                / 3600, 2))
            .withColumn("topic", F.lit("pipeline_state")))


def orchestration(frames, now, settings):
    days = settings["run_window_days"]
    since = F.lit(now - timedelta(days=days)).cast("timestamp")
    expected = frames["dag_runs"].sparkSession.createDataFrame(
        [(dag, int(per_day * days)) for dag, per_day in settings["expected_runs_per_day"].items()],
        "dag_id STRING, expected_dag_runs BIGINT")
    observed = (frames["dag_runs"].where(F.col("start_date") >= since).groupBy("dag_id").agg(
        F.sum((F.col("state") == "success").cast("long")).alias("successful_dag_runs"),
        F.sum((F.col("state") == "failed").cast("long")).alias("failed_dag_runs")))
    return (expected.join(observed, "dag_id", "left")
            .fillna(0, ["successful_dag_runs", "failed_dag_runs"])
            .withColumn("run_compliance_pct",
                        F.round(100.0 * F.col("successful_dag_runs") / F.col("expected_dag_runs"), 2))
            .withColumn("topic", F.lit("orchestration")))


def matching_coverage(current, links, matching_rules):
    """Eligible observations, accepted and pending links, and coverage per relation and source."""
    eligible = []
    if "tsunami_events" in current:
        ts = current["tsunami_events"]
        causes = matching_rules["tsunami_to_earthquake"]["cause_codes"]
        eligible.append(ts.where(_col(ts, "cause_code", "long").isin(causes))
                        .select("source", "external_id", F.lit("caused_by").alias("relation_type")))
    if "tsunami_bulletins" in current:
        eligible.append(current["tsunami_bulletins"]
                        .select("source", "external_id", F.lit("reports_on").alias("relation_type")))
    if not eligible:
        return None
    items = eligible[0]
    for part in eligible[1:]:
        items = items.unionByName(part)
    status = (links.where(F.col("relation_type").isin("caused_by", "reports_on"))
              .groupBy(F.col("from_source").alias("source"), F.col("from_external_id").alias("external_id"),
                       "relation_type")
              .agg(F.max((F.col("review_status") == "accepted").cast("int")).alias("accepted"),
                   F.max((F.col("review_status") == "pending").cast("int")).alias("pending")))
    joined = items.join(status, ["source", "external_id", "relation_type"], "left")
    return (joined.groupBy("source", "relation_type").agg(
                F.count("*").alias("eligible"),
                F.sum(F.coalesce(F.col("accepted"), F.lit(0))).cast("long").alias("accepted_links"),
                F.sum(F.when(F.coalesce(F.col("accepted"), F.lit(0)) == 0, F.coalesce(F.col("pending"), F.lit(0)))
                      .otherwise(0)).cast("long").alias("pending_links"))
            .withColumn("unmatched", F.col("eligible") - F.col("accepted_links") - F.col("pending_links"))
            .withColumn("coverage_pct", F.round(100.0 * F.col("accepted_links") / F.col("eligible"), 2))
            .withColumn("topic", F.lit("matching")))


def governance_indicators(current, frames, now, settings, matching_rules, with_database):
    parts = null_rates(current)
    if with_database:
        parts.append(pipeline_state(frames, now, settings["run_window_days"]))
        parts.append(orchestration(frames, now, settings))
        coverage = matching_coverage(current, frames["links"], matching_rules)
        if coverage is not None:
            parts.append(coverage)
    return parts
