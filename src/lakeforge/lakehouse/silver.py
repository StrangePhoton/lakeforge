"""Conform one Bronze snapshot into Silver Delta tables."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakeforge.config import Settings
from lakeforge.ingest.landing import read_manifest
from lakeforge.lakehouse.catalog import BUSINESS_KEYS, DATASETS, LANDING_RUN_COLUMN, TABLES
from lakeforge.lakehouse.paths import resolve_delta_run
from lakeforge.lakehouse.session import build_session, default_warehouse_dir
from lakeforge.lakehouse.snapshot import (
    dataset_counts,
    landing_run_id,
    overwrite_delta,
    write_snapshot_manifest,
)

_LOGGER = logging.getLogger("lakeforge.silver")


@dataclass(frozen=True, slots=True)
class SilverResult:
    """Outcome of one Silver build."""

    landing_run_id: str
    bronze_dir: Path
    silver_dir: Path
    row_counts: tuple[tuple[str, int], ...]


def run_silver(
    settings: Settings,
    bronze_run: Path,
    *,
    spark: SparkSession | None = None,
) -> SilverResult:
    """Overwrite the Silver tables for one Bronze run."""
    bronze_dir = resolve_delta_run(settings.bronze_dir, bronze_run, DATASETS, layer="bronze")
    manifest = read_manifest(bronze_dir / "manifest.json")
    run_id = landing_run_id(manifest, bronze_dir)
    expected = dataset_counts(manifest, DATASETS, layer="bronze")
    silver_dir = settings.silver_dir.resolve() / bronze_dir.name
    silver_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    _LOGGER.info("silver started", extra={"run_id": run_id, "landing_path": str(bronze_dir)})
    session = spark if spark is not None else build_session(warehouse_dir=default_warehouse_dir())
    try:
        prepared = _prepare(session, bronze_dir, run_id, expected)
        counts: list[tuple[str, int]] = []
        for dataset, frame in prepared:
            count = frame.count()
            overwrite_delta(frame, silver_dir / dataset)
            counts.append((dataset, count))
            _LOGGER.info(
                "silver written",
                extra={"run_id": run_id, "dataset": dataset, "row_count": count},
            )
    finally:
        if spark is None:
            session.stop()
    finished_at = datetime.now(UTC)
    written = tuple(counts)
    write_snapshot_manifest(
        silver_dir / "manifest.json",
        run_id=run_id,
        source_key="bronze_path",
        source_path=bronze_dir,
        counts=written,
        started_at=started_at,
        finished_at=finished_at,
    )
    telemetry_count = next(count for name, count in written if name == "telemetry_events")
    _LOGGER.info(
        "silver finished",
        extra={"run_id": run_id, "row_count": telemetry_count, "landing_path": str(silver_dir)},
    )
    return SilverResult(
        landing_run_id=run_id,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        row_counts=written,
    )


def _prepare(
    spark: SparkSession,
    bronze_dir: Path,
    run_id: str,
    expected: dict[str, int],
) -> tuple[tuple[str, DataFrame], ...]:
    frames = {
        dataset: _collapse(_read_checked(spark, bronze_dir, dataset, expected[dataset]), dataset)
        for dataset in DATASETS
    }
    frames["telemetry_events"] = _with_site(frames["telemetry_events"], frames["vehicles"])
    prepared: list[tuple[str, DataFrame]] = []
    for dataset in DATASETS:
        frame = frames[dataset]
        if frame.filter(F.col(LANDING_RUN_COLUMN) != F.lit(run_id)).limit(1).count():
            raise ValueError(f"{dataset} contains a row from another landing run")
        prepared.append((dataset, frame))
    return tuple(prepared)


def _read_checked(
    spark: SparkSession,
    bronze_dir: Path,
    dataset: str,
    expected_rows: int,
) -> DataFrame:
    try:
        frame = spark.read.format("delta").load((bronze_dir / dataset).as_posix())
        projected = frame.select(
            *[spec.name for spec in TABLES[dataset]],
            LANDING_RUN_COLUMN,
        )
        row_count = projected.count()
    except PySparkException as exc:
        raise ValueError(f"{dataset} does not match the bronze schema") from exc
    if row_count != expected_rows:
        raise ValueError(
            f"{dataset} row count {row_count} does not match the bronze manifest {expected_rows}"
        )
    return projected


def _collapse(frame: DataFrame, dataset: str) -> DataFrame:
    key = BUSINESS_KEYS[dataset]
    distinct = frame.dropDuplicates(list(frame.columns))
    try:
        conflict = (
            distinct.groupBy(key)
            .agg(F.count(F.lit(1)).alias("_n"))
            .filter(F.col("_n") > 1)
            .limit(1)
            .collect()
        )
    except PySparkException as exc:
        raise ValueError(f"{dataset} does not match the bronze schema") from exc
    if conflict:
        raise ValueError(f"{dataset}.{key} has conflicting rows")
    return distinct


def _with_site(telemetry: DataFrame, vehicles: DataFrame) -> DataFrame:
    lookup = vehicles.select(
        F.col("vehicle_id"),
        F.col("site_id").alias("_resolved_site_id"),
    )
    joined = telemetry.join(lookup, "vehicle_id", "left")
    columns = [spec.name for spec in TABLES["telemetry_events"]]
    vehicle_index = columns.index("vehicle_id") + 1
    ordered = [
        *columns[:vehicle_index],
        "site_id",
        *columns[vehicle_index:],
        LANDING_RUN_COLUMN,
    ]
    return joined.withColumn("site_id", F.col("_resolved_site_id")).select(*ordered)
