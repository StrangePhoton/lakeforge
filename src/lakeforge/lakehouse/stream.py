"""Publish landing telemetry to Kafka and load it with Structured Streaming."""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakeforge.config import Settings
from lakeforge.lakehouse.bronze import _parse
from lakeforge.lakehouse.catalog import TABLES
from lakeforge.lakehouse.contract import (
    ContractViolation,
    check_dataset,
    load_catalog,
    run_contract,
)
from lakeforge.lakehouse.paths import resolve_landing_run, resolve_stream_dir
from lakeforge.lakehouse.schema import landing_string_schema
from lakeforge.lakehouse.session import build_session, default_warehouse_dir

_LOGGER = logging.getLogger("lakeforge.stream")
TELEMETRY_TOPIC = "lakeforge.telemetry.events"
_MAX_OFFSETS_PER_TRIGGER = 40


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Outcome of one telemetry publish."""

    landing_run_id: str
    topic: str
    row_count: int


@dataclass(frozen=True, slots=True)
class StreamResult:
    """Outcome of one streaming catch-up."""

    topic: str
    stream_dir: Path
    checkpoint_dir: Path
    table_dir: Path
    row_count: int


def telemetry_kafka_record(line: str) -> tuple[str, str]:
    """Return the Kafka key and JSON value for one landing telemetry line."""
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError("telemetry record must be a JSON object")
    event_id = payload.get("event_id")
    if not isinstance(event_id, str):
        raise ValueError("telemetry record is missing event_id")
    return event_id, line.strip()


def run_publish(
    settings: Settings,
    landing_run: Path,
    *,
    spark: SparkSession | None = None,
) -> PublishResult:
    """Publish one landing telemetry file to Kafka after the contract check."""
    report = run_contract(settings, landing_run)
    failed = [name for name, count in report.violation_counts if count > 0]
    if failed:
        raise ValueError(f"{', '.join(failed)} has schema violations")
    bootstrap = resolve_bootstrap(settings.kafka_bootstrap)
    landing_dir = resolve_landing_run(settings.landing_dir, report.landing_dir)
    source = landing_dir / "telemetry_events.jsonl"
    _LOGGER.info(
        "publish started",
        extra={"run_id": report.landing_run_id, "landing_path": str(source)},
    )
    session = spark if spark is not None else build_session(warehouse_dir=default_warehouse_dir())
    try:
        frame = _kafka_frame(session, source)
        row_count = frame.count()
        (
            frame.write.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("kafka.acks", "all")
            .option("topic", TELEMETRY_TOPIC)
            .save()
        )
    finally:
        if spark is None:
            session.stop()
    _LOGGER.info(
        "publish finished",
        extra={
            "run_id": report.landing_run_id,
            "row_count": row_count,
            "landing_path": str(source),
        },
    )
    return PublishResult(
        landing_run_id=report.landing_run_id,
        topic=TELEMETRY_TOPIC,
        row_count=row_count,
    )


def run_stream(settings: Settings, *, spark: SparkSession | None = None) -> StreamResult:
    """Read the telemetry topic up to now and merge it into the stream table."""
    stream_dir = resolve_stream_dir(settings.stream_dir)
    bootstrap = resolve_bootstrap(settings.kafka_bootstrap)
    table_dir = stream_dir / "telemetry_events"
    checkpoint_dir = stream_dir / "checkpoint" / "telemetry_events"
    violation_path = stream_dir / "schema_violation.jsonl"
    stream_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.parent.mkdir(parents=True, exist_ok=True)
    _LOGGER.info(
        "stream started",
        extra={"run_id": TELEMETRY_TOPIC, "landing_path": str(checkpoint_dir)},
    )
    session = spark if spark is not None else build_session(warehouse_dir=default_warehouse_dir())
    try:
        source = (
            session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", TELEMETRY_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", str(_MAX_OFFSETS_PER_TRIGGER))
            .load()
        )

        def _consume(batch: DataFrame, _epoch: int) -> None:
            _apply_batch(batch, table_dir, violation_path)

        query = (
            source.writeStream.foreachBatch(_consume)
            .option("checkpointLocation", checkpoint_dir.as_posix())
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        row_count = _table_count(session, table_dir)
    finally:
        if spark is None:
            session.stop()
    _write_manifest(
        stream_dir / "manifest.json",
        checkpoint_dir=checkpoint_dir,
        table_dir=table_dir,
        row_count=row_count,
    )
    _LOGGER.info(
        "stream finished",
        extra={"run_id": TELEMETRY_TOPIC, "row_count": row_count, "landing_path": str(table_dir)},
    )
    return StreamResult(
        topic=TELEMETRY_TOPIC,
        stream_dir=stream_dir,
        checkpoint_dir=checkpoint_dir,
        table_dir=table_dir,
        row_count=row_count,
    )


def resolve_bootstrap(bootstrap: str) -> str:
    """Return a host:port bootstrap that is not a filesystem path."""
    if ".." in bootstrap or "/" in bootstrap or "\\" in bootstrap:
        raise ValueError("kafka bootstrap must stay a host and port")
    host, separator, port = bootstrap.rpartition(":")
    if separator != ":" or host == "" or not port.isdigit():
        raise ValueError("kafka bootstrap must stay a host and port")
    return bootstrap


def _kafka_frame(spark: SparkSession, source: Path) -> DataFrame:
    text = spark.read.text(source.as_posix())
    return text.filter(F.length(F.trim(F.col("value"))) > 0).select(
        F.get_json_object("value", "$.event_id").alias("key"),
        F.col("value").alias("value"),
    )


def _apply_batch(batch: DataFrame, table_dir: Path, violation_path: Path) -> None:
    rows = batch.select(F.col("value").cast("string").alias("value"), F.col("offset")).collect()
    version = load_catalog()["telemetry_events"].latest()
    accepted: list[str] = []
    violations: list[ContractViolation] = []
    for row in rows:
        value = row["value"]
        offset = int(row["offset"])
        if not isinstance(value, str) or not value.strip():
            violations.append(
                ContractViolation("telemetry_events", offset, "invalid_json", "", None)
            )
            continue
        _, found = check_dataset("telemetry_events", value.strip() + "\n", version)
        if found:
            violations.extend(
                ContractViolation(item.dataset, offset, item.rule_id, item.field, item.record_id)
                for item in found
            )
            continue
        accepted.append(value.strip())
    _append_violations(violation_path, violations)
    if accepted:
        _merge_telemetry(batch.sparkSession, accepted, table_dir)


def _merge_telemetry(spark: SparkSession, lines: list[str], table_dir: Path) -> None:
    raw = spark.read.schema(landing_string_schema("telemetry_events")).json(
        spark.sparkContext.parallelize(lines)
    )
    specs = TABLES["telemetry_events"]
    parsed = raw.select(*(_parse(F.col(spec.name), spec).alias(spec.name) for spec in specs))
    destination = table_dir.as_posix()
    if DeltaTable.isDeltaTable(spark, destination):
        stored = DeltaTable.forPath(spark, destination).alias("stored")
        (
            stored.merge(parsed.alias("incoming"), "stored.event_id = incoming.event_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return
    (
        parsed.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(destination)
    )


def _append_violations(path: Path, violations: list[ContractViolation]) -> None:
    if not violations:
        return
    lines = [
        json.dumps(
            {
                "dataset": item.dataset,
                "line": item.line,
                "rule_id": item.rule_id,
                "field": item.field,
                "record_id": item.record_id,
            },
            separators=(",", ":"),
        )
        for item in violations
    ]
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    path.write_text(existing + "\n".join(lines) + "\n", encoding="utf-8")


def _table_count(spark: SparkSession, table_dir: Path) -> int:
    if not (table_dir / "_delta_log").is_dir():
        return 0
    return int(spark.read.format("delta").load(table_dir.as_posix()).count())


def _write_manifest(
    path: Path,
    *,
    checkpoint_dir: Path,
    table_dir: Path,
    row_count: int,
) -> None:
    payload = {
        "topic": TELEMETRY_TOPIC,
        "checkpoint_path": str(checkpoint_dir),
        "table_path": str(table_dir),
        "rows": row_count,
        "status": "succeeded",
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
