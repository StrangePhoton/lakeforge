"""Load one JSONL landing run into local Bronze Delta tables."""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from lakeforge.config import Settings
from lakeforge.ingest.landing import read_manifest
from lakeforge.lakehouse.catalog import DATASETS, LANDING_RUN_COLUMN, TABLES, ColumnSpec
from lakeforge.lakehouse.paths import resolve_landing_run
from lakeforge.lakehouse.schema import landing_string_schema
from lakeforge.lakehouse.session import build_session, default_warehouse_dir

_LOGGER = logging.getLogger("lakeforge.bronze")
_TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX"
_DATE_PATTERN = "yyyy-MM-dd"


@dataclass(frozen=True, slots=True)
class BronzeResult:
    """Outcome of one Bronze load."""

    landing_run_id: str
    landing_dir: Path
    bronze_dir: Path
    row_counts: tuple[tuple[str, int], ...]


def run_bronze(
    settings: Settings,
    landing_run: Path,
    *,
    spark: SparkSession | None = None,
) -> BronzeResult:
    """Overwrite the Bronze tables for one landing run."""
    landing_dir = resolve_landing_run(settings.landing_dir, landing_run)
    manifest = read_manifest(landing_dir / "manifest.json")
    run_id = _manifest_run_id(manifest, landing_dir)
    expected = _manifest_counts(manifest)
    bronze_dir = settings.bronze_dir.resolve() / landing_dir.name
    bronze_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    _LOGGER.info("bronze started", extra={"run_id": run_id, "landing_path": str(landing_dir)})
    session = spark if spark is not None else build_session(warehouse_dir=default_warehouse_dir())
    try:
        counts: list[tuple[str, int]] = []
        for dataset in DATASETS:
            count = _write_dataset(
                session,
                dataset=dataset,
                source=landing_dir / f"{dataset}.jsonl",
                target=bronze_dir / dataset,
                run_id=run_id,
                expected_rows=expected[dataset],
            )
            counts.append((dataset, count))
            _LOGGER.info(
                "bronze written",
                extra={"run_id": run_id, "dataset": dataset, "row_count": count},
            )
    finally:
        if spark is None:
            session.stop()
    finished_at = datetime.now(UTC)
    _write_manifest(
        bronze_dir / "manifest.json",
        run_id=run_id,
        landing_dir=landing_dir,
        counts=tuple(counts),
        started_at=started_at,
        finished_at=finished_at,
    )
    telemetry_count = next(count for name, count in counts if name == "telemetry_events")
    _LOGGER.info(
        "bronze finished",
        extra={"run_id": run_id, "row_count": telemetry_count, "landing_path": str(bronze_dir)},
    )
    return BronzeResult(
        landing_run_id=run_id,
        landing_dir=landing_dir,
        bronze_dir=bronze_dir,
        row_counts=tuple(counts),
    )


def _write_dataset(
    spark: SparkSession,
    *,
    dataset: str,
    source: Path,
    target: Path,
    run_id: str,
    expected_rows: int,
) -> int:
    frame = _read_and_parse(spark, dataset, source, run_id)
    try:
        row_count = frame.count()
    except PySparkException as exc:
        raise ValueError(f"{dataset} does not match the bronze schema") from exc
    if row_count != expected_rows:
        raise ValueError(
            f"{dataset} row count {row_count} does not match the landing manifest {expected_rows}"
        )
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target.as_posix())
    )
    return row_count


def _read_and_parse(spark: SparkSession, dataset: str, source: Path, run_id: str) -> DataFrame:
    specs = TABLES[dataset]
    string_schema = landing_string_schema(dataset)
    if source.stat().st_size == 0:
        raw = spark.createDataFrame([], string_schema)
    else:
        raw = spark.read.schema(string_schema).json(source.as_posix())
    _reject_unparsed_values(raw, dataset, specs)
    return raw.select(
        *(_parse(F.col(spec.name), spec).alias(spec.name) for spec in specs),
        F.lit(run_id).alias(LANDING_RUN_COLUMN),
    )


def _parse(column: Column, spec: ColumnSpec) -> Column:
    if spec.kind == "string":
        return column
    if spec.kind == "double":
        return column.cast("double")
    if spec.kind == "date":
        return F.to_date(column, _DATE_PATTERN)
    return F.to_timestamp(column, _TIMESTAMP_PATTERN)


def _reject_unparsed_values(
    raw: DataFrame,
    dataset: str,
    specs: tuple[ColumnSpec, ...],
) -> None:
    checked = raw.select(
        *[F.col(spec.name).alias(f"raw_{spec.name}") for spec in specs],
        *[_parse(F.col(spec.name), spec).alias(spec.name) for spec in specs],
    )
    problems = [F.when(_invalid(spec), F.lit(spec.name)) for spec in specs]
    try:
        found = (
            checked.select(F.coalesce(*problems).alias("problem"))
            .filter(F.col("problem").isNotNull())
            .limit(1)
            .collect()
        )
    except PySparkException as exc:
        raise ValueError(f"{dataset} does not match the bronze schema") from exc
    if found:
        raise ValueError(f"{dataset}.{found[0]['problem']} does not match the bronze schema")


def _invalid(spec: ColumnSpec) -> Column:
    parsed_is_null = F.col(spec.name).isNull()
    raw_present = F.col(f"raw_{spec.name}").isNotNull()
    if spec.nullable:
        return raw_present & parsed_is_null
    return parsed_is_null


def _manifest_run_id(manifest: dict[str, object], landing_dir: Path) -> str:
    run_id = manifest.get("run_id")
    if not isinstance(run_id, str) or landing_dir.name != f"run_id={run_id}":
        raise ValueError("manifest run_id does not match the landing directory")
    return run_id


def _manifest_counts(manifest: dict[str, object]) -> dict[str, int]:
    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError("landing manifest is missing dataset counts")
    counts: dict[str, int] = {}
    for name in DATASETS:
        entry = datasets.get(name)
        if not isinstance(entry, dict) or not isinstance(entry.get("rows"), int):
            raise ValueError(f"landing manifest is missing a row count for {name}")
        counts[name] = entry["rows"]
    return counts


def _write_manifest(
    path: Path,
    *,
    run_id: str,
    landing_dir: Path,
    counts: tuple[tuple[str, int], ...],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    payload = {
        "landing_run_id": run_id,
        "landing_path": str(landing_dir),
        "format": "delta",
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "datasets": {name: {"rows": count} for name, count in counts},
        "status": "succeeded",
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")
