"""Pure synthetic fleet records. This module performs no I/O."""

import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from lakeforge.domain.models import TelemetryEvent, Vehicle

WINDOW_START = datetime(2026, 1, 12, 6, 0, tzinfo=UTC)
SITE_CODE = "SITE-ALPHA"
ORIGIN_LATITUDE = 45.0
ORIGIN_LONGITUDE = -90.0
MAX_SPEED_KPH = 40.0
MAX_FUEL_RATE_LPH = 40.0

ASSET_CATALOG: tuple[tuple[str, str], ...] = (
    ("haul_truck", "HT-240"),
    ("excavator", "EX-90"),
    ("wheel_loader", "WL-160"),
    ("dozer", "DZ-120"),
)

_WORKING_SPEED_KPH = {
    "haul_truck": 28.0,
    "excavator": 0.0,
    "wheel_loader": 12.0,
    "dozer": 8.0,
}
_FUEL_RATE_LPH = {
    "off": 0.0,
    "idle": 3.5,
    "working": 22.0,
}
_FUEL_JITTER_LPH = {
    "off": 0.0,
    "idle": 0.4,
    "working": 1.5,
}


@dataclass(frozen=True, slots=True)
class GeneratedDataset:
    """One seeded snapshot of the operational source."""

    seed: int
    vehicles: tuple[Vehicle, ...]
    telemetry_events: tuple[TelemetryEvent, ...]


def generate_dataset(
    *,
    seed: int,
    vehicle_count: int,
    hours: int,
    interval_minutes: int,
) -> GeneratedDataset:
    """Build a deterministic vehicle and telemetry snapshot.

    Identity fields are structural. Measurements use ``random.Random(seed)``
    and a fixed clock, so the same arguments always return the same records.
    """
    steps = _validated_steps(
        vehicle_count=vehicle_count,
        hours=hours,
        interval_minutes=interval_minutes,
    )
    rng = random.Random(seed)
    vehicles = tuple(_vehicle(index) for index in range(1, vehicle_count + 1))
    events: list[TelemetryEvent] = []
    for vehicle in vehicles:
        events.extend(_events_for_vehicle(rng, vehicle, steps, interval_minutes))
    ordered = tuple(sorted(events, key=lambda event: (event.event_time, event.event_id)))
    return GeneratedDataset(seed=seed, vehicles=vehicles, telemetry_events=ordered)


def _validated_steps(*, vehicle_count: int, hours: int, interval_minutes: int) -> int:
    if vehicle_count < 1:
        raise ValueError("vehicle_count must be at least 1")
    if hours < 1:
        raise ValueError("hours must be at least 1")
    if interval_minutes < 1:
        raise ValueError("interval_minutes must be at least 1")
    steps = (hours * 60) // interval_minutes
    if steps < 1:
        raise ValueError("interval_minutes must fit at least once inside the generation window")
    return steps


def _vehicle(index: int) -> Vehicle:
    asset_type, model_code = ASSET_CATALOG[(index - 1) % len(ASSET_CATALOG)]
    return Vehicle(
        vehicle_id=f"VEH-{index:04d}",
        asset_type=asset_type,
        model_code=model_code,
        site_code=SITE_CODE,
        status="active",
        commissioned_on=date(2020, 1, 15) + timedelta(days=30 * (index - 1)),
    )


def _events_for_vehicle(
    rng: random.Random,
    vehicle: Vehicle,
    steps: int,
    interval_minutes: int,
) -> list[TelemetryEvent]:
    vehicle_index = int(vehicle.vehicle_id.removeprefix("VEH-"))
    latitude = round(ORIGIN_LATITUDE + (vehicle_index - 1) * 0.01, 6)
    longitude = round(ORIGIN_LONGITUDE + (vehicle_index - 1) * 0.01, 6)
    engine_hours = round(1000.0 + (vehicle_index - 1) * 125.0, 3)
    events: list[TelemetryEvent] = []
    for step in range(steps):
        event_time = WINDOW_START + timedelta(minutes=interval_minutes * step)
        engine_state = _engine_state(event_time)
        latitude = _clamp(round(latitude + rng.uniform(-0.0008, 0.0008), 6), -90.0, 90.0)
        longitude = _clamp(round(longitude + rng.uniform(-0.0008, 0.0008), 6), -180.0, 180.0)
        if engine_state != "off":
            engine_hours = round(engine_hours + (interval_minutes / 60.0), 3)
        events.append(
            TelemetryEvent(
                event_id=f"EVT-{vehicle_index:04d}-{step + 1:06d}",
                vehicle_id=vehicle.vehicle_id,
                event_time=event_time,
                latitude=latitude,
                longitude=longitude,
                speed_kph=_speed(rng, vehicle.asset_type, engine_state),
                engine_state=engine_state,
                fuel_rate_lph=_fuel(rng, engine_state),
                engine_hours=engine_hours,
            )
        )
    return events


def _engine_state(event_time: datetime) -> str:
    hour = event_time.astimezone(UTC).hour
    if hour < 7:
        return "off"
    if hour < 8:
        return "idle"
    if hour < 11:
        return "working"
    return "idle"


def _speed(rng: random.Random, asset_type: str, engine_state: str) -> float:
    if engine_state != "working":
        return 0.0
    base = _WORKING_SPEED_KPH[asset_type]
    if base == 0.0:
        return 0.0
    return _clamp(round(base + rng.uniform(-1.5, 1.5), 3), 0.0, MAX_SPEED_KPH)


def _fuel(rng: random.Random, engine_state: str) -> float:
    spread = _FUEL_JITTER_LPH[engine_state]
    if spread == 0.0:
        return 0.0
    return _clamp(
        round(_FUEL_RATE_LPH[engine_state] + rng.uniform(-spread, spread), 3),
        0.0,
        MAX_FUEL_RATE_LPH,
    )


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)
