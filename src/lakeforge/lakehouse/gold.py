"""Aggregate one Silver snapshot into Gold activity tables."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakeforge.config import Settings
from lakeforge.ingest.landing import read_manifest
from lakeforge.lakehouse.catalog import DATASETS, GOLD_TABLES, LANDING_RUN_COLUMN
from lakeforge.lakehouse.paths import resolve_delta_run
from lakeforge.lakehouse.session import build_session, default_warehouse_dir
from lakeforge.lakehouse.snapshot import (
    dataset_counts,
    landing_run_id,
    overwrite_delta,
    write_snapshot_manifest,
)

_LOGGER = logging.getLogger("lakeforge.gold")
_COUNT_COLUMNS = ("event_count", "timed_event_count", "alarm_count", "work_order_count")


@dataclass(frozen=True, slots=True)
class GoldResult:
    """Outcome of one Gold build."""

    landing_run_id: str
    silver_dir: Path
    gold_dir: Path
    row_counts: tuple[tuple[str, int], ...]


def run_gold(
    settings: Settings,
    silver_run: Path,
    *,
    spark: SparkSession | None = None,
) -> GoldResult:
    """Overwrite the Gold activity tables for one Silver run."""
    silver_dir = resolve_delta_run(settings.silver_dir, silver_run, DATASETS, layer="silver")
    manifest = read_manifest(silver_dir / "manifest.json")
    run_id = landing_run_id(manifest, silver_dir)
    expected = dataset_counts(manifest, DATASETS, layer="silver")
    gold_dir = settings.gold_dir.resolve() / silver_dir.name
    gold_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    _LOGGER.info("gold started", extra={"run_id": run_id, "landing_path": str(silver_dir)})
    session = spark if spark is not None else build_session(warehouse_dir=default_warehouse_dir())
    try:
        prepared = _prepare(session, silver_dir, expected)
        counts: list[tuple[str, int]] = []
        for dataset, frame in prepared:
            count = frame.count()
            overwrite_delta(frame, gold_dir / dataset)
            counts.append((dataset, count))
            _LOGGER.info(
                "gold written",
                extra={"run_id": run_id, "dataset": dataset, "row_count": count},
            )
    finally:
        if spark is None:
            session.stop()
    finished_at = datetime.now(UTC)
    written = tuple(counts)
    write_snapshot_manifest(
        gold_dir / "manifest.json",
        run_id=run_id,
        source_key="silver_path",
        source_path=silver_dir,
        counts=written,
        started_at=started_at,
        finished_at=finished_at,
    )
    _LOGGER.info(
        "gold finished",
        extra={"run_id": run_id, "row_count": written[0][1], "landing_path": str(gold_dir)},
    )
    return GoldResult(
        landing_run_id=run_id,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        row_counts=written,
    )


def _prepare(
    spark: SparkSession,
    silver_dir: Path,
    expected: dict[str, int],
) -> tuple[tuple[str, DataFrame], ...]:
    sites = _read_checked(spark, silver_dir, "sites", expected["sites"])
    vehicles = _read_checked(spark, silver_dir, "vehicles", expected["vehicles"])
    telemetry = _read_checked(spark, silver_dir, "telemetry_events", expected["telemetry_events"])
    alarms = _read_checked(spark, silver_dir, "alarms", expected["alarms"])
    work_orders = _read_checked(spark, silver_dir, "work_orders", expected["work_orders"])
    built = {
        "vehicle_activity": _vehicle_activity(vehicles, telemetry, alarms, work_orders),
        "site_activity": _site_activity(sites, vehicles, telemetry, alarms, work_orders),
    }
    return tuple((name, built[name]) for name in GOLD_TABLES)


def _vehicle_activity(
    vehicles: DataFrame,
    telemetry: DataFrame,
    alarms: DataFrame,
    work_orders: DataFrame,
) -> DataFrame:
    events = telemetry.groupBy("vehicle_id").agg(
        F.count(F.lit(1)).alias("event_count"),
        F.count("event_time").alias("timed_event_count"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
    )
    alarm_counts = alarms.groupBy("vehicle_id").agg(F.count(F.lit(1)).alias("alarm_count"))
    order_counts = work_orders.groupBy("vehicle_id").agg(
        F.count(F.lit(1)).alias("work_order_count")
    )
    activity = (
        vehicles.select(
            "vehicle_id",
            "site_id",
            "asset_type",
            "model_code",
            "status",
            LANDING_RUN_COLUMN,
        )
        .join(events, "vehicle_id", "left")
        .join(alarm_counts, "vehicle_id", "left")
        .join(order_counts, "vehicle_id", "left")
    )
    return _fill_counts(activity).select(
        "vehicle_id",
        "site_id",
        "asset_type",
        "model_code",
        "status",
        "event_count",
        "timed_event_count",
        "first_event_time",
        "last_event_time",
        "alarm_count",
        "work_order_count",
        LANDING_RUN_COLUMN,
    )


def _site_activity(
    sites: DataFrame,
    vehicles: DataFrame,
    telemetry: DataFrame,
    alarms: DataFrame,
    work_orders: DataFrame,
) -> DataFrame:
    vehicle_counts = vehicles.groupBy("site_id").agg(F.count(F.lit(1)).alias("vehicle_count"))
    event_counts = (
        telemetry.filter(F.col("site_id").isNotNull())
        .groupBy("site_id")
        .agg(F.count(F.lit(1)).alias("event_count"))
    )
    alarm_counts = (
        alarms.join(vehicles.select("vehicle_id", "site_id"), "vehicle_id", "inner")
        .groupBy("site_id")
        .agg(F.count(F.lit(1)).alias("alarm_count"))
    )
    order_counts = work_orders.groupBy("site_id").agg(F.count(F.lit(1)).alias("work_order_count"))
    activity = (
        sites.select("site_id", "site_name", LANDING_RUN_COLUMN)
        .join(vehicle_counts, "site_id", "left")
        .join(event_counts, "site_id", "left")
        .join(alarm_counts, "site_id", "left")
        .join(order_counts, "site_id", "left")
    )
    return _fill_counts(activity).select(
        "site_id",
        "site_name",
        "vehicle_count",
        "event_count",
        "alarm_count",
        "work_order_count",
        LANDING_RUN_COLUMN,
    )


def _fill_counts(frame: DataFrame) -> DataFrame:
    filled = frame
    for name in ("vehicle_count", *_COUNT_COLUMNS):
        if name in filled.columns:
            filled = filled.withColumn(name, F.coalesce(F.col(name), F.lit(0)).cast("long"))
    return filled


def _read_checked(
    spark: SparkSession,
    silver_dir: Path,
    dataset: str,
    expected_rows: int,
) -> DataFrame:
    try:
        frame = spark.read.format("delta").load((silver_dir / dataset).as_posix())
        row_count = frame.count()
    except PySparkException as exc:
        raise ValueError(f"{dataset} does not match the silver schema") from exc
    if row_count != expected_rows:
        raise ValueError(
            f"{dataset} row count {row_count} does not match the silver manifest {expected_rows}"
        )
    return frame
