"""Write a landing run as JSONL plus a manifest."""

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from lakeforge.domain.models import Alarm, AnomalyRecord, Site, TelemetryEvent, Vehicle, WorkOrder

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_PIPELINE_NAME = "source_snapshot"


def write_landing_run(
    *,
    landing_dir: Path,
    run_id: str,
    sites: Sequence[Site],
    vehicles: Sequence[Vehicle],
    telemetry_events: Sequence[TelemetryEvent],
    alarms: Sequence[Alarm],
    work_orders: Sequence[WorkOrder],
    anomalies: Sequence[AnomalyRecord] | None,
    seed: int | None,
    profile: str | None,
    anomalies_applied: bool | None,
    started_at: datetime,
    finished_at: datetime,
) -> Path:
    """Write one immutable landing directory for a snapshot extract.

    ``anomalies`` is simulation metadata. Pass None on an ingest that does not
    know the answer key. Operational rows never carry an anomaly label.
    """
    if _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("run_id must contain only letters, numbers, hyphens, and underscores")
    run_dir = landing_dir / f"run_id={run_id}"
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(exist_ok=False)
    _write_jsonl(run_dir / "sites.jsonl", [_site_record(site) for site in sites])
    _write_jsonl(run_dir / "vehicles.jsonl", [_vehicle_record(vehicle) for vehicle in vehicles])
    _write_jsonl(
        run_dir / "telemetry_events.jsonl",
        [_telemetry_record(event) for event in telemetry_events],
    )
    _write_jsonl(run_dir / "alarms.jsonl", [_alarm_record(alarm) for alarm in alarms])
    _write_jsonl(
        run_dir / "work_orders.jsonl", [_work_order_record(order) for order in work_orders]
    )
    if anomalies is not None:
        payload = [
            {
                "kind": item.kind,
                "table": item.table,
                "record_id": item.record_id,
                "field": item.field,
            }
            for item in anomalies
        ]
        (run_dir / "anomalies.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
    manifest: dict[str, object] = {
        "run_id": run_id,
        "pipeline": _PIPELINE_NAME,
        "seed": seed,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "source": {"system": "fleet_oltp", "mode": "full_snapshot"},
        "datasets": {
            "sites": {"rows": len(sites)},
            "vehicles": {"rows": len(vehicles)},
            "telemetry_events": {"rows": len(telemetry_events)},
            "alarms": {"rows": len(alarms)},
            "work_orders": {"rows": len(work_orders)},
        },
        "status": "succeeded",
    }
    if profile is not None:
        manifest["generation"] = {
            "profile": profile,
            "anomalies_applied": anomalies_applied,
            "anomaly_count": 0 if anomalies is None else len(anomalies),
        }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return run_dir


def read_sites(path: Path) -> tuple[Site, ...]:
    """Read a sites JSONL file back into domain records."""
    return tuple(_site_from_record(record) for record in _read_jsonl(path))


def read_vehicles(path: Path) -> tuple[Vehicle, ...]:
    """Read a vehicles JSONL file back into domain records."""
    return tuple(_vehicle_from_record(record) for record in _read_jsonl(path))


def read_telemetry(path: Path) -> tuple[TelemetryEvent, ...]:
    """Read a telemetry JSONL file back into domain records."""
    return tuple(_telemetry_from_record(record) for record in _read_jsonl(path))


def read_alarms(path: Path) -> tuple[Alarm, ...]:
    """Read an alarms JSONL file back into domain records."""
    return tuple(_alarm_from_record(record) for record in _read_jsonl(path))


def read_work_orders(path: Path) -> tuple[WorkOrder, ...]:
    """Read a work-order JSONL file back into domain records."""
    return tuple(_work_order_from_record(record) for record in _read_jsonl(path))


def read_anomalies(path: Path) -> tuple[AnomalyRecord, ...]:
    """Read the simulation answer-key file."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("anomalies.json must be a JSON array")
    records: list[AnomalyRecord] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("anomaly catalog entries must be JSON objects")
        records.append(
            AnomalyRecord(
                kind=str(item["kind"]),
                table=str(item["table"]),
                record_id=str(item["record_id"]),
                field=str(item["field"]),
            )
        )
    return tuple(records)


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


def _site_record(site: Site) -> dict[str, object]:
    return {
        "site_id": site.site_id,
        "site_name": site.site_name,
        "latitude": site.latitude,
        "longitude": site.longitude,
        "timezone_name": site.timezone_name,
    }


def _vehicle_record(vehicle: Vehicle) -> dict[str, object]:
    return {
        "vehicle_id": vehicle.vehicle_id,
        "asset_type": vehicle.asset_type,
        "model_code": vehicle.model_code,
        "site_id": vehicle.site_id,
        "status": vehicle.status,
        "commissioned_on": vehicle.commissioned_on.isoformat(),
    }


def _telemetry_record(event: TelemetryEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "vehicle_id": event.vehicle_id,
        "event_time": None if event.event_time is None else _iso(event.event_time),
        "received_at": _iso(event.received_at),
        "latitude": event.latitude,
        "longitude": event.longitude,
        "speed_kph": event.speed_kph,
        "engine_state": event.engine_state,
        "fuel_rate_lph": event.fuel_rate_lph,
        "engine_hours": event.engine_hours,
    }


def _alarm_record(alarm: Alarm) -> dict[str, object]:
    return {
        "alarm_id": alarm.alarm_id,
        "vehicle_id": alarm.vehicle_id,
        "event_id": alarm.event_id,
        "alarm_code": alarm.alarm_code,
        "severity": alarm.severity,
        "raised_at": _iso(alarm.raised_at),
    }


def _work_order_record(order: WorkOrder) -> dict[str, object]:
    return {
        "work_order_id": order.work_order_id,
        "vehicle_id": order.vehicle_id,
        "site_id": order.site_id,
        "alarm_id": order.alarm_id,
        "opened_at": _iso(order.opened_at),
        "closed_at": None if order.closed_at is None else _iso(order.closed_at),
        "work_type": order.work_type,
        "cause_code": order.cause_code,
        "status": order.status,
    }


def _site_from_record(record: Mapping[str, Any]) -> Site:
    return Site(
        site_id=str(record["site_id"]),
        site_name=str(record["site_name"]),
        latitude=float(record["latitude"]),
        longitude=float(record["longitude"]),
        timezone_name=str(record["timezone_name"]),
    )


def _vehicle_from_record(record: Mapping[str, Any]) -> Vehicle:
    return Vehicle(
        vehicle_id=str(record["vehicle_id"]),
        asset_type=str(record["asset_type"]),
        model_code=str(record["model_code"]),
        site_id=str(record["site_id"]),
        status=str(record["status"]),
        commissioned_on=date.fromisoformat(str(record["commissioned_on"])),
    )


def _telemetry_from_record(record: Mapping[str, Any]) -> TelemetryEvent:
    return TelemetryEvent(
        event_id=str(record["event_id"]),
        vehicle_id=None if record["vehicle_id"] is None else str(record["vehicle_id"]),
        event_time=_optional_time(record["event_time"]),
        received_at=_required_time(record["received_at"]),
        latitude=_optional_float(record["latitude"]),
        longitude=_optional_float(record["longitude"]),
        speed_kph=_optional_float(record["speed_kph"]),
        engine_state=None if record["engine_state"] is None else str(record["engine_state"]),
        fuel_rate_lph=_optional_float(record["fuel_rate_lph"]),
        engine_hours=_optional_float(record["engine_hours"]),
    )


def _alarm_from_record(record: Mapping[str, Any]) -> Alarm:
    return Alarm(
        alarm_id=str(record["alarm_id"]),
        vehicle_id=str(record["vehicle_id"]),
        event_id=None if record["event_id"] is None else str(record["event_id"]),
        alarm_code=str(record["alarm_code"]),
        severity=str(record["severity"]),
        raised_at=_required_time(record["raised_at"]),
    )


def _work_order_from_record(record: Mapping[str, Any]) -> WorkOrder:
    return WorkOrder(
        work_order_id=str(record["work_order_id"]),
        vehicle_id=str(record["vehicle_id"]),
        site_id=str(record["site_id"]),
        alarm_id=None if record["alarm_id"] is None else str(record["alarm_id"]),
        opened_at=_required_time(record["opened_at"]),
        closed_at=_optional_time(record["closed_at"]),
        work_type=str(record["work_type"]),
        cause_code=str(record["cause_code"]),
        status=str(record["status"]),
    )


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    raise ValueError("expected a number")


def _optional_time(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value))


def _required_time(value: object) -> datetime:
    return datetime.fromisoformat(str(value))


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds")
