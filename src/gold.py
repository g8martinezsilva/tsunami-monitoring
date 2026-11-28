"""Consolidate Silver snapshots and publish the Gold datasets (Workshop 3, with the Workshop 4 additions
of src/gold_dashboard.py)."""

import logging
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import Window
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

TIME_COLUMNS = {
    "earthquakes": "event_time",
    "tsunami_events": "event_time",
    "tsunami_bulletins": "sent_at",
}
REJECTED = "rejected_records"
IQR_FIELDS = {"earthquakes": "depth_km", "tsunami_events": "max_water_height"}
STAMP_FORMAT = "%Y%m%d_%H%M%S"
SPARK_STAMP_FORMAT = "yyyyMMdd_HHmmss"

GOVERNANCE_SCHEMA = (
    "topic STRING, source STRING, rows_processed BIGINT, unique_observations BIGINT, repeated_rows BIGINT, "
    "missing_coordinates BIGINT, missing_coordinates_pct DOUBLE, incomplete_times BIGINT, "
    "incomplete_times_pct DOUBLE, rejected_records BIGINT, iqr_field STRING, iqr_outliers BIGINT, "
    "unique_rejected_records BIGINT, field STRING, null_count BIGINT, null_rate_pct DOUBLE, "
    "last_successful_run_at TIMESTAMP, watermark_at TIMESTAMP, watermark_age_hours DOUBLE, "
    "load_runs_window BIGINT, failed_load_runs_window BIGINT, dag_id STRING, successful_dag_runs BIGINT, "
    "failed_dag_runs BIGINT, expected_dag_runs BIGINT, run_compliance_pct DOUBLE, relation_type STRING, "
    "eligible BIGINT, accepted_links BIGINT, pending_links BIGINT, unmatched BIGINT, coverage_pct DOUBLE, "
    # Workshop 4 (src/gold_dashboard.py)
    "duplicate_rate_pct DOUBLE, silver_topic STRING, schema_rows BIGINT, schema_compliant BIGINT, "
    "schema_compliance_pct DOUBLE, files_checked BIGINT, files_compliant BIGINT, outlier_values BIGINT, "
    "outlier_count BIGINT, outlier_rate_pct DOUBLE, period DATE, volume_rows BIGINT, new_observations BIGINT, "
    "threshold DOUBLE, status STRING, q1 DOUBLE, median DOUBLE, q3 DOUBLE, iqr DOUBLE, "
    "lower_whisker DOUBLE, upper_whisker DOUBLE, outlier_points ARRAY<DOUBLE>"
)
STORYTELLING_SCHEMA = (
    "metric STRING, source STRING, year INT, center STRING, magnitude_type STRING, "
    "bin_start DOUBLE, bin_end DOUBLE, unit STRING, count BIGINT, external_id STRING, event_time TIMESTAMP, "
    "latitude DOUBLE, longitude DOUBLE, magnitude DOUBLE, depth_km DOUBLE, place STRING, region_cell STRING, "
    "relevance_level STRING, relevance_points INT, history_tsunamis BIGINT, tsunami_linked BOOLEAN, "
    "bulletin_source STRING, bulletin_severity STRING, bulletin_sent_at TIMESTAMP, value DOUBLE, "
    "historical_tsunamis BIGINT, max_magnitude DOUBLE, max_water_height_m DOUBLE, "
    # Workshop 4 (src/gold_dashboard.py)
    "historical_count BIGINT, expected_count DOUBLE, p_value DOUBLE, window_days INT"
)


def discover_silver(silver_dir):
    """Find both flat historical snapshots and snapshots in the topic directories."""
    root = Path(silver_dir).resolve()
    return {
        topic: sorted(
            path
            for directory in (root, root / topic)
            for path in directory.glob(f"{topic}_*.parquet")
            if path.is_file()
        )
        for topic in (*TIME_COLUMNS, REJECTED)
    }


def read_silver(spark, topic, files, scratch):
    """Read snapshots, using temporary copies for Parquet types Spark cannot merge."""
    paths = []
    originals = {}
    for number, path in enumerate(files):
        with pq.ParquetFile(path) as parquet:
            # Old empty rejection files have untyped null columns and contribute no records.
            if parquet.metadata.num_rows == 0:
                continue
            schema = parquet.schema_arrow
            has_nanoseconds = any(pa.types.is_timestamp(field.type) and field.type.unit == "ns" for field in schema)
            null_rejection_ids = topic == REJECTED and pa.types.is_null(schema.field("external_id").type)
            if has_nanoseconds or null_rejection_ids:
                copy = scratch / str(number) / path.name
                copy.parent.mkdir(parents=True)
                table = parquet.read()
                if null_rejection_ids:
                    index = schema.get_field_index("external_id")
                    table = table.cast(schema.set(index, schema.field(index).with_type(pa.string())))
                pq.write_table(table, copy, coerce_timestamps="us", allow_truncated_timestamps=True)
                originals[copy.as_uri()] = path.as_uri()
                log.info("Gold reading temporary Parquet compatibility copy for %s", path)
                path = copy
        paths.append(str(path))

    if not paths:
        return None
    frame = spark.read.parquet(*paths)
    if originals:
        # Keep the source filename and path for ordering, even when Spark reads a copy.
        aliases = F.create_map(*(F.lit(value) for pair in originals.items() for value in pair))
        filename = F.input_file_name()
        frame = frame.withColumn("_silver_file", F.coalesce(F.element_at(aliases, filename), filename))
    return frame


def _optional_column(frame, name, data_type):
    return F.col(name) if name in frame.columns else F.lit(None).cast(data_type)


def snapshot_timestamp(frame):
    """Capture time of each Silver row: Bronze manifest stamp, then Silver processing time, then file stamp."""
    silver_file = F.coalesce(_optional_column(frame, "_silver_file", "string"), F.input_file_name())
    bronze_stamp = F.regexp_extract(
        _optional_column(frame, "bronze_manifest", "string"), r"^bronze_manifest_(\d{8}_\d{6})(?:_|\.)", 1
    )
    file_stamp = F.regexp_extract(silver_file, r"_(\d{8}_\d{6})\.parquet$", 1)
    return F.coalesce(
        F.try_to_timestamp(bronze_stamp, F.lit(SPARK_STAMP_FORMAT)),
        _optional_column(frame, "silver_processed_at", "timestamp"),
        F.try_to_timestamp(file_stamp, F.lit(SPARK_STAMP_FORMAT)),
    )


def consolidate(frame, topic):
    """Select one current Silver row per source key and retain its occurrence count."""
    row_values = F.to_json(F.struct(*sorted(frame.columns)), {"ignoreNullFields": "false"})
    frame = frame.withColumn(
        "_silver_file", F.coalesce(_optional_column(frame, "_silver_file", "string"), F.input_file_name())
    )
    silver_file = F.col("_silver_file")
    processed_at = _optional_column(frame, "silver_processed_at", "timestamp")
    snapshot_at = snapshot_timestamp(frame)

    order = []
    if topic == "earthquakes":
        order.append(_optional_column(frame, "source_updated_at", "timestamp").desc_nulls_last())
    elif topic == "tsunami_bulletins":
        revision = F.coalesce(_optional_column(frame, "atom_updated_at", "timestamp"), F.col("sent_at"))
        order.append(revision.desc_nulls_last())

    # The file name and row values break ties even if Spark reads the files in a different order.
    order.extend([snapshot_at.desc_nulls_last(), processed_at.desc_nulls_last(), silver_file.desc(), row_values.desc()])
    key = Window.partitionBy("source", "external_id")
    return (
        frame.withColumn("_snapshot_rows", F.count("*").over(key))
        .withColumn("_revision_rank", F.row_number().over(key.orderBy(*order)))
        .where(F.col("_revision_rank") == 1)
        .drop("_revision_rank")
    )


def governance_metrics(spark, current, rejected=None):
    """Calculate processing history and current-state quality by topic and source."""
    result = spark.createDataFrame([], GOVERNANCE_SCHEMA)
    for topic, frame in current.items():
        missing_coordinates = (
            F.col("latitude").isNull() | F.isnan("latitude") | F.col("longitude").isNull() | F.isnan("longitude")
        )
        iqr_field = IQR_FIELDS.get(topic)
        iqr_flag = f"{iqr_field}_iqr_outlier"
        has_iqr_flag = iqr_flag in frame.columns
        metrics = (
            frame.groupBy("source")
            .agg(
                F.sum("_snapshot_rows").alias("rows_processed"),
                F.count("*").alias("unique_observations"),
                F.sum(missing_coordinates.cast("long")).alias("missing_coordinates"),
                F.sum(F.col(TIME_COLUMNS[topic]).isNull().cast("long")).alias("incomplete_times"),
                F.sum(_optional_column(frame, iqr_flag, "boolean").cast("long")).alias("iqr_outliers"),
            )
            .withColumn("topic", F.lit(topic))
            .withColumn("iqr_field", F.lit(iqr_field if has_iqr_flag else None))
            .withColumn("repeated_rows", F.col("rows_processed") - F.col("unique_observations"))
            # Workshop 4: share of processed rows that repeat a key already seen (before consolidation)
            .withColumn("duplicate_rate_pct", F.round(100.0 * F.col("repeated_rows") / F.col("rows_processed"), 2))
            .withColumn(
                "missing_coordinates_pct", F.round(100.0 * F.col("missing_coordinates") / F.col("unique_observations"), 2)
            )
            .withColumn(
                "incomplete_times_pct", F.round(100.0 * F.col("incomplete_times") / F.col("unique_observations"), 2)
            )
        )
        result = result.unionByName(metrics, allowMissingColumns=True)

    if rejected is not None:
        rejection = F.struct("external_id", "bronze_file", "locator", "reason")
        counts = rejected.groupBy("source").agg(
            F.count("*").alias("rejected_records"),
            F.countDistinct(rejection).alias("unique_rejected_records"),
        )
        result = result.unionByName(counts.withColumn("topic", F.lit(REJECTED)), allowMissingColumns=True)
    return result


def _year_counts(frame, metric, year):
    return (
        frame.withColumn("year", year).where(F.col("year").isNotNull())
        .groupBy("source", "year").count().withColumn("metric", F.lit(metric))
    )


def _distribution(frame, metric, field, width, unit=None, extra_groups=()):
    value = F.col(field)
    return (
        frame.where(value.isNotNull() & ~F.isnan(value) & (F.abs(value) < float("inf")))
        .withColumn("bin_start", F.floor(value / width) * width)
        .groupBy("source", "bin_start", *extra_groups).count()
        .withColumn("bin_end", F.col("bin_start") + width)
        .withColumn("metric", F.lit(metric))
        .withColumn("unit", F.lit(unit))
    )


def storytelling_metrics(spark, current):
    """Summarize unique observations, with a separate metric and unit for each measure."""
    parts = []
    if "earthquakes" in current:
        earthquakes = current["earthquakes"]
        parts.append(_year_counts(earthquakes, "earthquakes_by_year", F.year("event_time")))
        parts.append(_distribution(
            earthquakes, "earthquake_magnitude_distribution", "magnitude", 0.5, extra_groups=("magnitude_type",)
        ))
    if "tsunami_events" in current:
        tsunamis = current["tsunami_events"]
        year = F.coalesce(F.year("event_date"), F.year("event_time"))
        parts.append(_year_counts(tsunamis, "historical_tsunamis_by_year", year))
        parts.append(_distribution(tsunamis, "tsunami_max_water_height_distribution", "max_water_height", 1.0, unit="m"))
    if "tsunami_bulletins" in current:
        parts.append(
            current["tsunami_bulletins"].groupBy("source", "center").count()
            .withColumn("metric", F.lit("bulletins_by_center"))
        )

    result = spark.createDataFrame([], STORYTELLING_SCHEMA)
    for part in parts:
        result = result.unionByName(part, allowMissingColumns=True)
    return result


def write_gold(governance, storytelling, gold_dir):
    """Stage both Parquet directories and publish a new pair with a shared UTC stamp."""
    root = Path(gold_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    datasets = {"governance": governance, "storytelling": storytelling}
    with TemporaryDirectory(prefix=".gold-", dir=root) as staging:
        staged = {name: Path(staging) / name for name in datasets}
        for name, frame in datasets.items():
            frame.coalesce(1).write.mode("errorifexists").parquet(str(staged[name]))

        stamp = datetime.now(timezone.utc).strftime(STAMP_FORMAT)
        paths = {name: root / f"{name}_{stamp}.parquet" for name in datasets}
        if any(path.exists() for path in paths.values()):
            raise FileExistsError(f"Gold output already exists for {stamp}")

        staged["governance"].rename(paths["governance"])
        try:
            staged["storytelling"].rename(paths["storytelling"])
        except OSError:
            paths["governance"].rename(staged["governance"])
            raise
    return paths


def _union(base, parts):
    for part in parts:
        base = base.unionByName(part, allowMissingColumns=True)
    return base.select(*base.columns)


def run_gold(spark, silver_dir, gold_dir, context=None, now=None):
    """Run one Silver-to-Gold refresh and return the paths of the published datasets.

    context: optional database inputs (src/gold_context.py: links, runs, state, dag_runs). Without it,
    the indicators that need PostgreSQL or Airflow (pipeline state, orchestration, matching coverage,
    linked bulletins and tsunamis) are left out and everything else is still computed."""
    from src.config import load_dashboard_settings, load_gold_settings, load_matching_rules, load_relevance_rules
    from src.gold_dashboard import (apply_thresholds, historical_vs_current, outlier_rates, schema_compliance,
                                    volume_over_time)
    from src.gold_indicators import (context_frames, governance_indicators, storytelling_indicators,
                                     tsunami_relevance)

    now = now or datetime.now(timezone.utc)
    frames = context_frames(spark, context)
    rules, settings = load_relevance_rules(), load_gold_settings()
    dashboard = load_dashboard_settings()
    paths = discover_silver(silver_dir)
    if not any(paths.values()):
        raise FileNotFoundError(f"No Workshop 3 Silver Parquet snapshots under {silver_dir}")

    current, raw = {}, {}
    rejected = None
    with TemporaryDirectory(prefix="tsunami-gold-silver-") as scratch, ExitStack() as cached:
        for topic, files in paths.items():
            if not files:
                continue
            log.info("Gold input %s: %s Silver snapshots", topic, len(files))
            frame = read_silver(spark, topic, files, Path(scratch) / topic)
            if frame is None:
                continue
            if topic == REJECTED:
                rejected = frame
            else:
                raw[topic] = frame
                current[topic] = consolidate(frame, topic).persist()
                cached.callback(current[topic].unpersist)

        relevance = tsunami_relevance(current, frames["links"], rules)
        if relevance is not None:
            relevance = relevance.persist()
            cached.callback(relevance.unpersist)
        governance = _union(governance_metrics(spark, current, rejected), governance_indicators(
            current, frames, now, settings, load_matching_rules(), with_database=context is not None)
            + schema_compliance(current, paths, dashboard) + outlier_rates(current, dashboard)
            + volume_over_time(raw, snapshot_timestamp))
        governance = apply_thresholds(governance, dashboard)
        storytelling = _union(storytelling_metrics(spark, current),
                              storytelling_indicators(current, relevance, frames["links"], rules, settings)
                              + historical_vs_current(current, rules, settings, dashboard, now))
        outputs = write_gold(governance, storytelling, gold_dir)
        governance_rows = spark.read.parquet(str(outputs["governance"])).collect()
        for row in governance_rows:
            if row.rows_processed is None and row.rejected_records is None:
                continue  # only the per-topic history rows; the indicator rows are summarized below
            log.info(
                "Gold input %s/%s: %s processed rows, %s unique observations, %s repeated rows, "
                "%s processed rejections, %s unique rejections",
                row.topic, row.source, row.rows_processed, row.unique_observations, row.repeated_rows, row.rejected_records,
                row.unique_rejected_records,
            )
        log.info("Gold output governance: %s indicator rows in %s", len(governance_rows), outputs["governance"])
        story_rows = spark.read.parquet(str(outputs["storytelling"])).count()
        log.info("Gold output storytelling: %s indicator rows in %s", story_rows, outputs["storytelling"])
        return outputs
