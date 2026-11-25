"""Apply the fleet quality rules to one Silver snapshot."""

import json
import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from lakeforge.config import Settings
from lakeforge.ingest.landing import read_manifest
from lakeforge.lakehouse.catalog import DATASETS, LANDING_RUN_COLUMN
from lakeforge.lakehouse.paths import resolve_delta_run
from lakeforge.lakehouse.schema import silver_telemetry_schema
from lakeforge.lakehouse.session import build_session, default_warehouse_dir
from lakeforge.lakehouse.snapshot import dataset_counts, landing_run_id, overwrite_delta

_LOGGER = logging.getLogger("lakeforge.quality")
_MAX_SPEED_KPH = 80.0
_LATE_SECONDS = 60 * 60
_ENGINE_STATES = ("off", "idle", "working")
_PASSED_TABLE = "telemetry_passed"
_VIOLATION_TABLE = "telemetry_violation"
_REFERENCE_TABLE = "reference_violation"
TELEMETRY_RULES = (
    "vehicle_id_present",
    "vehicle_known",
    "event_time_present",
    "position_present",
    "position_range",
    "speed_present",
    "speed_range",
    "engine_state_known",
    "event_time_late",
    "duplicate_reading",
    "received_at_out_of_order",
)
REFERENCE_RULES = (
    "alarm_vehicle_known",
    "work_order_vehicle_known",
    "work_order_site_known",
    "work_order_alarm_known",
)
RULES = TELEMETRY_RULES + REFERENCE_RULES


@dataclass(frozen=True, slots=True)
class QualityResult:
    """Outcome of one quality build."""

    landing_run_id: str
    silver_dir: Path
    quality_dir: Path
    passed_rows: int
    violation_rows: int
    rule_counts: tuple[tuple[str, int], ...]


def run_quality(
    settings: Settings,
    silver_run: Path,
    *,
    spark: SparkSession | None = None,
) -> QualityResult:
    """Overwrite the quality tables for one Silver run."""
    silver_dir = resolve_delta_run(settings.silver_dir, silver_run, DATASETS, layer="silver")
    manifest = read_manifest(silver_dir / "manifest.json")
    run_id = landing_run_id(manifest, silver_dir)
    expected = dataset_counts(manifest, DATASETS, layer="silver")
    quality_dir = settings.quality_dir.resolve() / silver_dir.name
    quality_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    _LOGGER.info("quality started", extra={"run_id": run_id, "landing_path": str(silver_dir)})
    session = spark if spark is not None else build_session(warehouse_dir=default_warehouse_dir())
    try:
        outcome = _write_tables(session, silver_dir, quality_dir, run_id, expected)
    finally:
        if spark is None:
            session.stop()
    finished_at = datetime.now(UTC)
    _write_manifest(
        quality_dir / "manifest.json",
        run_id=run_id,
        silver_dir=silver_dir,
        passed_rows=outcome.passed_rows,
        violation_rows=outcome.violation_rows,
        rule_counts=outcome.rule_counts,
        started_at=started_at,
        finished_at=finished_at,
    )
    _LOGGER.info(
        "quality finished",
        extra={
            "run_id": run_id,
            "row_count": outcome.passed_rows,
            "landing_path": str(quality_dir),
        },
    )
    return QualityResult(
        landing_run_id=run_id,
        silver_dir=silver_dir,
        quality_dir=quality_dir,
        passed_rows=outcome.passed_rows,
        violation_rows=outcome.violation_rows,
        rule_counts=outcome.rule_counts,
    )


def _write_tables(
    spark: SparkSession,
    silver_dir: Path,
    quality_dir: Path,
    run_id: str,
    expected: dict[str, int],
) -> QualityResult:
    telemetry = _read_checked(spark, silver_dir, "telemetry_events", expected["telemetry_events"])
    vehicles = _read_checked(spark, silver_dir, "vehicles", expected["vehicles"])
    sites = _read_checked(spark, silver_dir, "sites", expected["sites"])
    alarms = _read_checked(spark, silver_dir, "alarms", expected["alarms"])
    work_orders = _read_checked(spark, silver_dir, "work_orders", expected["work_orders"])
    violations = _telemetry_violations(telemetry, vehicles)
    if violations.filter(F.col(LANDING_RUN_COLUMN) != F.lit(run_id)).limit(1).count():
        raise ValueError("telemetry_events contains a row from another landing run")
    violation_ids = violations.select("event_id").dropDuplicates(["event_id"])
    passed = telemetry.join(violation_ids, "event_id", "left_anti")
    passed_rows = passed.count()
    violation_rows = violations.count()
    references = _reference_violations(alarms, work_orders, vehicles, sites)
    rule_counts = _rule_counts(violations, references)
    overwrite_delta(passed, quality_dir / _PASSED_TABLE)
    overwrite_delta(violations, quality_dir / _VIOLATION_TABLE)
    _LOGGER.info(
        "quality written",
        extra={"run_id": run_id, "dataset": _PASSED_TABLE, "row_count": passed_rows},
    )
    _LOGGER.info(
        "quality written",
        extra={"run_id": run_id, "dataset": _VIOLATION_TABLE, "row_count": violation_rows},
    )
    reference_rows = sum(count for rule, count in rule_counts if rule in REFERENCE_RULES)
    reference_dir = quality_dir / _REFERENCE_TABLE
    if reference_rows:
        overwrite_delta(references, reference_dir)
    elif reference_dir.exists():
        shutil.rmtree(reference_dir)
    return QualityResult(
        landing_run_id=run_id,
        silver_dir=silver_dir,
        quality_dir=quality_dir,
        passed_rows=passed_rows,
        violation_rows=violation_rows,
        rule_counts=rule_counts,
    )


def _telemetry_violations(telemetry: DataFrame, vehicles: DataFrame) -> DataFrame:
    columns = [field.name for field in silver_telemetry_schema().fields]
    known = vehicles.select("vehicle_id").dropDuplicates(["vehicle_id"])
    late = telemetry.filter(_is_late())
    unknown_vehicle = telemetry.filter(F.col("vehicle_id").isNotNull()).join(
        known,
        "vehicle_id",
        "left_anti",
    )
    parts = (
        _mark(telemetry.filter(F.col("vehicle_id").isNull()), "vehicle_id_present", columns),
        _mark(unknown_vehicle, "vehicle_known", columns),
        _mark(telemetry.filter(F.col("event_time").isNull()), "event_time_present", columns),
        _mark(
            telemetry.filter(F.col("latitude").isNull() | F.col("longitude").isNull()),
            "position_present",
            columns,
        ),
        _mark(telemetry.filter(_position_out_of_range()), "position_range", columns),
        _mark(telemetry.filter(F.col("speed_kph").isNull()), "speed_present", columns),
        _mark(telemetry.filter(_speed_out_of_range()), "speed_range", columns),
        _mark(telemetry.filter(_engine_state_invalid()), "engine_state_known", columns),
        _mark(late, "event_time_late", columns),
        _mark(_duplicate_rows(telemetry), "duplicate_reading", columns),
        _mark(_out_of_order_rows(telemetry, late), "received_at_out_of_order", columns),
    )
    combined = parts[0]
    for part in parts[1:]:
        combined = combined.unionByName(part)
    return combined.dropDuplicates(["event_id", "rule_id"])


def _duplicate_rows(telemetry: DataFrame) -> DataFrame:
    eligible = telemetry.filter(F.col("vehicle_id").isNotNull() & F.col("event_time").isNotNull())
    window = Window.partitionBy("vehicle_id", "event_time").orderBy("event_id")
    ranked = eligible.withColumn("_copy", F.row_number().over(window))
    return ranked.filter(F.col("_copy") > 1).drop("_copy")


def _out_of_order_rows(telemetry: DataFrame, late: DataFrame) -> DataFrame:
    sequenced = telemetry.join(late.select("event_id"), "event_id", "left_anti").filter(
        F.col("vehicle_id").isNotNull() & F.col("event_time").isNotNull()
    )
    window = Window.partitionBy("vehicle_id").orderBy("event_time", "event_id")
    sequenced = sequenced.withColumn("_prev_received_at", F.lag("received_at").over(window))
    sequenced = sequenced.withColumn("_prev_event_id", F.lag("event_id").over(window))
    breaks = sequenced.filter(
        F.col("_prev_received_at").isNotNull() & (F.col("received_at") < F.col("_prev_received_at"))
    )
    ids = breaks.select("event_id").union(breaks.select(F.col("_prev_event_id").alias("event_id")))
    return telemetry.join(ids.dropDuplicates(["event_id"]), "event_id", "inner")


def _reference_violations(
    alarms: DataFrame,
    work_orders: DataFrame,
    vehicles: DataFrame,
    sites: DataFrame,
) -> DataFrame:
    vehicle_ids = vehicles.select("vehicle_id")
    site_ids = sites.select("site_id")
    alarm_ids = alarms.select("alarm_id")
    parts = (
        _missing_reference(
            alarms,
            id_column="alarm_id",
            key_column="vehicle_id",
            lookup=vehicle_ids,
            rule_id="alarm_vehicle_known",
            source_table="alarms",
        ),
        _missing_reference(
            work_orders,
            id_column="work_order_id",
            key_column="vehicle_id",
            lookup=vehicle_ids,
            rule_id="work_order_vehicle_known",
            source_table="work_orders",
        ),
        _missing_reference(
            work_orders,
            id_column="work_order_id",
            key_column="site_id",
            lookup=site_ids,
            rule_id="work_order_site_known",
            source_table="work_orders",
        ),
        _missing_reference(
            work_orders,
            id_column="work_order_id",
            key_column="alarm_id",
            lookup=alarm_ids,
            rule_id="work_order_alarm_known",
            source_table="work_orders",
        ),
    )
    combined = parts[0]
    for part in parts[1:]:
        combined = combined.unionByName(part)
    return combined


def _missing_reference(
    frame: DataFrame,
    *,
    id_column: str,
    key_column: str,
    lookup: DataFrame,
    rule_id: str,
    source_table: str,
) -> DataFrame:
    return (
        frame.filter(F.col(key_column).isNotNull())
        .join(lookup.select(F.col(key_column)), key_column, "left_anti")
        .select(
            F.lit(rule_id).alias("rule_id"),
            F.lit(source_table).alias("source_table"),
            F.col(id_column).alias("record_id"),
        )
    )


def _rule_counts(violations: DataFrame, references: DataFrame) -> tuple[tuple[str, int], ...]:
    found = _collected_counts(violations)
    found.update(_collected_counts(references))
    return tuple((rule, found.get(rule, 0)) for rule in RULES)


def _collected_counts(frame: DataFrame) -> dict[str, int]:
    rows = frame.groupBy("rule_id").count().collect()
    return {str(row["rule_id"]): int(row["count"]) for row in rows}


def _mark(frame: DataFrame, rule_id: str, columns: list[str]) -> DataFrame:
    return frame.select(*columns).withColumn("rule_id", F.lit(rule_id))


def _is_late() -> Column:
    elapsed = F.unix_timestamp("received_at") - F.unix_timestamp("event_time")
    return (
        F.col("event_time").isNotNull()
        & F.col("received_at").isNotNull()
        & (elapsed > F.lit(_LATE_SECONDS))
    )


def _position_out_of_range() -> Column:
    latitude = F.col("latitude").isNotNull() & (
        (F.col("latitude") < F.lit(-90.0)) | (F.col("latitude") > F.lit(90.0))
    )
    longitude = F.col("longitude").isNotNull() & (
        (F.col("longitude") < F.lit(-180.0)) | (F.col("longitude") > F.lit(180.0))
    )
    return latitude | longitude


def _speed_out_of_range() -> Column:
    return F.col("speed_kph").isNotNull() & (
        (F.col("speed_kph") < F.lit(0.0)) | (F.col("speed_kph") > F.lit(_MAX_SPEED_KPH))
    )


def _engine_state_invalid() -> Column:
    return F.col("engine_state").isNull() | ~F.col("engine_state").isin(*_ENGINE_STATES)


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


def _write_manifest(
    path: Path,
    *,
    run_id: str,
    silver_dir: Path,
    passed_rows: int,
    violation_rows: int,
    rule_counts: tuple[tuple[str, int], ...],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    payload = {
        "landing_run_id": run_id,
        "silver_path": str(silver_dir),
        "format": "delta",
        "started_at": started_at.astimezone(UTC).isoformat(timespec="seconds"),
        "finished_at": finished_at.astimezone(UTC).isoformat(timespec="seconds"),
        "datasets": {
            _PASSED_TABLE: {"rows": passed_rows},
            _VIOLATION_TABLE: {"rows": violation_rows},
        },
        "rules": {rule: count for rule, count in rule_counts},
        "status": "succeeded",
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
