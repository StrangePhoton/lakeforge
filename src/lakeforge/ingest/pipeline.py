"""Batch snapshot from the simulated source into the local landing zone."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.source.postgres import (
    connect_with_retry,
    fetch_alarms,
    fetch_sites,
    fetch_telemetry,
    fetch_vehicles,
    fetch_work_orders,
    recreate_schema,
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
    site_count: int
    vehicle_count: int
    telemetry_count: int
    alarm_count: int
    work_order_count: int


def run_generate(settings: Settings, *, run_id: str | None = None) -> GeneratedDataset:
    """Replace the PostgreSQL source with one deterministic snapshot."""
    dataset = generate_dataset(
        seed=settings.seed,
        profile=settings.profile,
        vehicle_count=settings.vehicle_count,
        hours=settings.hours,
        interval_minutes=settings.interval_minutes,
        anomalies=settings.anomalies,
    )
    with connect_with_retry(settings) as connection:
        recreate_schema(connection)
        replace_dataset(connection, dataset)
    for name, row_count in _dataset_counts(dataset):
        _log_rows(
            _SOURCE_LOGGER,
            "source loaded",
            run_id=run_id,
            dataset=name,
            row_count=row_count,
            seed=settings.seed,
        )
    return dataset


def run_ingest(
    settings: Settings,
    *,
    run_id: str | None = None,
    seed: int | None = None,
    generation: GeneratedDataset | None = None,
) -> PipelineResult:
    """Extract the current source snapshot into a new landing run."""
    resolved_run_id = run_id or str(uuid4())
    started_at = datetime.now(UTC)
    _INGEST_LOGGER.info("ingest started", extra={"run_id": resolved_run_id})
    with connect_with_retry(settings) as connection:
        sites = fetch_sites(connection)
        vehicles = fetch_vehicles(connection)
        telemetry_events = fetch_telemetry(connection)
        alarms = fetch_alarms(connection)
        work_orders = fetch_work_orders(connection)
    finished_at = datetime.now(UTC)
    landing_dir = write_landing_run(
        landing_dir=settings.landing_dir,
        run_id=resolved_run_id,
        sites=sites,
        vehicles=vehicles,
        telemetry_events=telemetry_events,
        alarms=alarms,
        work_orders=work_orders,
        anomalies=None if generation is None else generation.anomalies,
        seed=seed,
        profile=None if generation is None else generation.profile,
        anomalies_applied=None if generation is None else generation.anomalies_applied,
        started_at=started_at,
        finished_at=finished_at,
    )
    counts = (
        ("sites", len(sites)),
        ("vehicles", len(vehicles)),
        ("telemetry_events", len(telemetry_events)),
        ("alarms", len(alarms)),
        ("work_orders", len(work_orders)),
    )
    for name, row_count in counts:
        _log_rows(
            _INGEST_LOGGER,
            "landing written",
            run_id=resolved_run_id,
            dataset=name,
            row_count=row_count,
            landing_path=str(landing_dir),
        )
    return PipelineResult(
        run_id=resolved_run_id,
        landing_dir=landing_dir,
        seed=seed,
        site_count=len(sites),
        vehicle_count=len(vehicles),
        telemetry_count=len(telemetry_events),
        alarm_count=len(alarms),
        work_order_count=len(work_orders),
    )


def run_pipeline(settings: Settings, *, run_id: str | None = None) -> PipelineResult:
    """Generate the source snapshot and extract it in one run."""
    resolved_run_id = run_id or str(uuid4())
    _PIPELINE_LOGGER.info(
        "pipeline started",
        extra={"run_id": resolved_run_id, "seed": settings.seed},
    )
    dataset = run_generate(settings, run_id=resolved_run_id)
    result = run_ingest(
        settings,
        run_id=resolved_run_id,
        seed=settings.seed,
        generation=dataset,
    )
    _PIPELINE_LOGGER.info(
        "pipeline finished",
        extra={
            "run_id": resolved_run_id,
            "row_count": result.telemetry_count,
            "landing_path": str(result.landing_dir),
        },
    )
    return result


def _dataset_counts(dataset: GeneratedDataset) -> tuple[tuple[str, int], ...]:
    return (
        ("sites", len(dataset.sites)),
        ("vehicles", len(dataset.vehicles)),
        ("telemetry_events", len(dataset.telemetry_events)),
        ("alarms", len(dataset.alarms)),
        ("work_orders", len(dataset.work_orders)),
    )


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
