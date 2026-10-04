"""Batch snapshot from the simulated source into the local landing zone."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.source.postgres import (
    apply_schema,
    connect_with_retry,
    fetch_telemetry,
    fetch_vehicles,
    replace_dataset,
)
from lakeforge.synthetic.generator import GeneratedDataset, generate_dataset

_SOURCE_LOGGER = logging.getLogger("lakeforge.source")
_INGEST_LOGGER = logging.getLogger("lakeforge.ingest")
_PIPELINE_LOGGER = logging.getLogger("lakeforge.pipeline")


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """Outcome of one snapshot extract."""

    run_id: str
    landing_dir: Path
    seed: int | None
    vehicle_count: int
    telemetry_count: int


def run_generate(settings: Settings, *, run_id: str | None = None) -> GeneratedDataset:
    """Replace the PostgreSQL source with one deterministic snapshot."""
    dataset = generate_dataset(
        seed=settings.seed,
        vehicle_count=settings.vehicle_count,
        hours=settings.hours,
        interval_minutes=settings.interval_minutes,
    )
    with connect_with_retry(settings) as connection:
        apply_schema(connection)
        replace_dataset(connection, dataset)
    _log_rows(
        _SOURCE_LOGGER,
        "source loaded",
        run_id=run_id,
        dataset="vehicles",
        row_count=len(dataset.vehicles),
        seed=settings.seed,
    )
    _log_rows(
        _SOURCE_LOGGER,
        "source loaded",
        run_id=run_id,
        dataset="telemetry_events",
        row_count=len(dataset.telemetry_events),
        seed=settings.seed,
    )
    return dataset


def run_ingest(
    settings: Settings,
    *,
    run_id: str | None = None,
    seed: int | None = None,
) -> PipelineResult:
    """Extract the current source snapshot into a new landing run."""
    resolved_run_id = run_id or str(uuid4())
    started_at = datetime.now(UTC)
    _INGEST_LOGGER.info("ingest started", extra={"run_id": resolved_run_id})
    with connect_with_retry(settings) as connection:
        apply_schema(connection)
        vehicles = fetch_vehicles(connection)
        telemetry_events = fetch_telemetry(connection)
    finished_at = datetime.now(UTC)
    landing_dir = write_landing_run(
        landing_dir=settings.landing_dir,
        run_id=resolved_run_id,
        vehicles=vehicles,
        telemetry_events=telemetry_events,
        seed=seed,
        started_at=started_at,
        finished_at=finished_at,
    )
    _log_rows(
        _INGEST_LOGGER,
        "landing written",
        run_id=resolved_run_id,
        dataset="vehicles",
        row_count=len(vehicles),
        landing_path=str(landing_dir),
    )
    _log_rows(
        _INGEST_LOGGER,
        "landing written",
        run_id=resolved_run_id,
        dataset="telemetry_events",
        row_count=len(telemetry_events),
        landing_path=str(landing_dir),
    )
    return PipelineResult(
        run_id=resolved_run_id,
        landing_dir=landing_dir,
        seed=seed,
        vehicle_count=len(vehicles),
        telemetry_count=len(telemetry_events),
    )


def run_pipeline(settings: Settings, *, run_id: str | None = None) -> PipelineResult:
    """Generate the source snapshot and extract it in one run."""
    resolved_run_id = run_id or str(uuid4())
    _PIPELINE_LOGGER.info(
        "pipeline started",
        extra={"run_id": resolved_run_id, "seed": settings.seed},
    )
    run_generate(settings, run_id=resolved_run_id)
    result = run_ingest(settings, run_id=resolved_run_id, seed=settings.seed)
    _PIPELINE_LOGGER.info(
        "pipeline finished",
        extra={
            "run_id": resolved_run_id,
            "row_count": result.telemetry_count,
            "landing_path": str(result.landing_dir),
        },
    )
    return result


def _log_rows(
    logger: logging.Logger,
    message: str,
    *,
    run_id: str | None,
    dataset: str,
    row_count: int,
    seed: int | None = None,
    landing_path: str | None = None,
) -> None:
    extra: dict[str, object] = {"dataset": dataset, "row_count": row_count}
    if run_id is not None:
        extra["run_id"] = run_id
    if seed is not None:
        extra["seed"] = seed
    if landing_path is not None:
        extra["landing_path"] = landing_path
    logger.info(message, extra=extra)
