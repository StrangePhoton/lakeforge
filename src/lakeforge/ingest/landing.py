"""Write a Milestone 0 landing run as JSONL plus a manifest."""

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from lakeforge.domain.models import TelemetryEvent, Vehicle

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_PIPELINE_NAME = "m0_source_snapshot"


def write_landing_run(
    *,
    landing_dir: Path,
    run_id: str,
    vehicles: Sequence[Vehicle],
    telemetry_events: Sequence[TelemetryEvent],
    seed: int | None,
    started_at: datetime,
    finished_at: datetime,
) -> Path:
    """Write one immutable landing directory for a snapshot extract."""
    if _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("run_id must contain only letters, numbers, hyphens, and underscores")
    run_dir = landing_dir / f"run_id={run_id}"
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(exist_ok=False)
    _write_jsonl(run_dir / "vehicles.jsonl", [_vehicle_record(vehicle) for vehicle in vehicles])
    _write_jsonl(
        run_dir / "telemetry_events.jsonl",
        [_telemetry_record(event) for event in telemetry_events],
    )
    manifest = {
        "run_id": run_id,
        "pipeline": _PIPELINE_NAME,
        "seed": seed,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "source": {"system": "fleet_oltp", "mode": "full_snapshot"},
        "datasets": {
            "vehicles": {"rows": len(vehicles)},
            "telemetry_events": {"rows": len(telemetry_events)},
        },
        "status": "succeeded",
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return run_dir


def read_vehicles(path: Path) -> tuple[Vehicle, ...]:
    """Read a vehicles JSONL file back into domain records."""
    return tuple(_vehicle_from_record(record) for record in _read_jsonl(path))


def read_telemetry(path: Path) -> tuple[TelemetryEvent, ...]:
    """Read a telemetry JSONL file back into domain records."""
    return tuple(_telemetry_from_record(record) for record in _read_jsonl(path))


def read_manifest(path: Path) -> dict[str, Any]:
    """Read a landing manifest."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest must be a JSON object")
    return payload


def _write_jsonl(path: Path, records: Sequence[Mapping[str, object]]) -> None:
    lines = [json.dumps(record, separators=(",", ":")) for record in records]
    text = "\n".join(lines)
    if lines:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _read_jsonl(path: Path) -> tuple[dict[str, Any], ...]:
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"{path.name} contains a non-object JSON line")
        records.append(payload)
    return tuple(records)


def _vehicle_record(vehicle: Vehicle) -> dict[str, object]:
    return {
        "vehicle_id": vehicle.vehicle_id,
        "asset_type": vehicle.asset_type,
        "model_code": vehicle.model_code,
        "site_code": vehicle.site_code,
        "status": vehicle.status,
        "commissioned_on": vehicle.commissioned_on.isoformat(),
    }


def _telemetry_record(event: TelemetryEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "vehicle_id": event.vehicle_id,
        "event_time": _iso(event.event_time),
        "latitude": event.latitude,
        "longitude": event.longitude,
        "speed_kph": event.speed_kph,
        "engine_state": event.engine_state,
        "fuel_rate_lph": event.fuel_rate_lph,
        "engine_hours": event.engine_hours,
    }


def _vehicle_from_record(record: Mapping[str, Any]) -> Vehicle:
    return Vehicle(
        vehicle_id=str(record["vehicle_id"]),
        asset_type=str(record["asset_type"]),
        model_code=str(record["model_code"]),
        site_code=str(record["site_code"]),
        status=str(record["status"]),
        commissioned_on=date.fromisoformat(str(record["commissioned_on"])),
    )


def _telemetry_from_record(record: Mapping[str, Any]) -> TelemetryEvent:
    return TelemetryEvent(
        event_id=str(record["event_id"]),
        vehicle_id=str(record["vehicle_id"]),
        event_time=datetime.fromisoformat(str(record["event_time"])),
        latitude=float(record["latitude"]),
        longitude=float(record["longitude"]),
        speed_kph=float(record["speed_kph"]),
        engine_state=str(record["engine_state"]),
        fuel_rate_lph=float(record["fuel_rate_lph"]),
        engine_hours=float(record["engine_hours"]),
    )


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds")
