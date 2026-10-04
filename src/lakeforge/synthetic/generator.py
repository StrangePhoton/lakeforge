"""Deterministic operational data for the industrial source simulator."""

import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from lakeforge.domain.models import Alarm, AnomalyRecord, Site, TelemetryEvent, Vehicle, WorkOrder
from lakeforge.synthetic.anomalies import apply_anomaly_plan
from lakeforge.synthetic.profiles import (
    ASSET_CATALOG,
    SITES,
    GenerationProfile,
    resolve_profile,
)

SERVICE_INTERVAL_HOURS = 250.0
IDLE_MINUTES = 60
WORK_ORDER_DURATION = timedelta(hours=4)
SITE_RADIUS_DEGREES = 0.02

_WORKING_SPEED_KPH = {
    "haul_truck": 28.0,
    "excavator": 0.0,
    "wheel_loader": 12.0,
    "dozer": 8.0,
}
_WORKING_FUEL_LPH = {
    "haul_truck": 32.0,
    "excavator": 18.0,
    "wheel_loader": 22.0,
    "dozer": 16.0,
}
_IDLE_FUEL_LPH = 3.5
_MAX_CLEAN_SPEED_KPH = 40.0
_MAX_CLEAN_FUEL_LPH = 80.0


@dataclass(frozen=True, slots=True)
class GeneratedDataset:
    """One seeded snapshot of the operational source."""

    seed: int
    profile: str
    anomalies_applied: bool
    sites: tuple[Site, ...]
    vehicles: tuple[Vehicle, ...]
    telemetry_events: tuple[TelemetryEvent, ...]
    alarms: tuple[Alarm, ...]
    work_orders: tuple[WorkOrder, ...]
    anomalies: tuple[AnomalyRecord, ...]


def generate_dataset(
    *,
    seed: int,
    profile: str = "dev",
    vehicle_count: int | None = None,
    hours: int | None = None,
    interval_minutes: int | None = None,
    anomalies: bool = True,
) -> GeneratedDataset:
    """Build sites, vehicles, telemetry, alarms, and work orders.

    The seed affects measurement jitter only. Identifiers, schedules, alarms,
    work orders, and defect placement are fixed for a profile and its overrides.
    """
    selected = resolve_profile(
        profile,
        vehicle_count=vehicle_count,
        hours=hours,
        interval_minutes=interval_minutes,
    )
    sites = SITES
    site_by_id = {site.site_id: site for site in sites}
    vehicles = tuple(_vehicle(index, sites) for index in range(1, selected.vehicle_count + 1))
    rng = random.Random(seed)
    clean_events: list[TelemetryEvent] = []
    for vehicle in vehicles:
        clean_events.extend(
            _events_for_vehicle(
                rng,
                vehicle,
                site_by_id[vehicle.site_id],
                selected,
            )
        )
    clean = tuple(sorted(clean_events, key=lambda event: (event.event_time, event.event_id)))
    alarms = _derive_alarms(vehicles, clean, selected.interval_minutes)
    work_orders = _derive_work_orders(vehicles, alarms, selected)
    anomaly_catalog: tuple[AnomalyRecord, ...] = ()
    telemetry = clean
    if anomalies:
        telemetry, anomaly_catalog = apply_anomaly_plan(clean)
    return GeneratedDataset(
        seed=seed,
        profile=selected.name,
        anomalies_applied=anomalies,
        sites=sites,
        vehicles=vehicles,
        telemetry_events=telemetry,
        alarms=alarms,
        work_orders=work_orders,
        anomalies=anomaly_catalog,
    )


def _vehicle(index: int, sites: tuple[Site, ...]) -> Vehicle:
    asset_type, model_code = ASSET_CATALOG[(index - 1) % len(ASSET_CATALOG)]
    site = sites[(index - 1) % len(sites)]
    return Vehicle(
        vehicle_id=f"VEH-{index:04d}",
        asset_type=asset_type,
        model_code=model_code,
        site_id=site.site_id,
        status="active",
        commissioned_on=date(2020, 1, 15) + timedelta(days=30 * (index - 1)),
    )


def _baseline_engine_hours(index: int) -> float:
    if index == 1:
        return 249.0
    return round(1000.0 + (index - 1) * 125.0, 3)


def _events_for_vehicle(
    rng: random.Random,
    vehicle: Vehicle,
    site: Site,
    profile: GenerationProfile,
) -> list[TelemetryEvent]:
    vehicle_index = int(vehicle.vehicle_id.removeprefix("VEH-"))
    latitude = site.latitude
    longitude = site.longitude
    engine_hours = _baseline_engine_hours(vehicle_index)
    events: list[TelemetryEvent] = []
    for step in range(profile.steps):
        event_time = profile.window_start + timedelta(minutes=profile.interval_minutes * step)
        engine_state = _engine_state(event_time)
        if vehicle.asset_type != "excavator" and engine_state != "off":
            latitude = _near_site(
                round(latitude + rng.uniform(-0.0008, 0.0008), 6),
                site.latitude,
            )
            longitude = _near_site(
                round(longitude + rng.uniform(-0.0008, 0.0008), 6),
                site.longitude,
            )
        if engine_state != "off":
            engine_hours = round(engine_hours + (profile.interval_minutes / 60.0), 3)
        events.append(
            TelemetryEvent(
                event_id=f"EVT-{vehicle_index:04d}-{step + 1:06d}",
                vehicle_id=vehicle.vehicle_id,
                event_time=event_time,
                received_at=event_time,
                latitude=latitude,
                longitude=longitude,
                speed_kph=_speed(rng, vehicle.asset_type, engine_state),
                engine_state=engine_state,
                fuel_rate_lph=_fuel(rng, vehicle.asset_type, engine_state),
                engine_hours=engine_hours,
            )
        )
    return events


def _engine_state(event_time: datetime) -> str:
    hour = event_time.astimezone(UTC).hour
    if hour <= 5:
        return "off"
    if hour == 6:
        return "idle"
    if hour <= 16:
        return "working"
    if hour <= 19:
        return "idle"
    return "off"


def _speed(rng: random.Random, asset_type: str, engine_state: str) -> float:
    if engine_state != "working":
        return 0.0
    base = _WORKING_SPEED_KPH[asset_type]
    if base == 0.0:
        return 0.0
    return _clamp(round(base + rng.uniform(-1.5, 1.5), 3), 0.0, _MAX_CLEAN_SPEED_KPH)


def _fuel(rng: random.Random, asset_type: str, engine_state: str) -> float:
    if engine_state == "off":
        return 0.0
    if engine_state == "idle":
        base = _IDLE_FUEL_LPH
        spread = 0.4
    else:
        base = _WORKING_FUEL_LPH[asset_type]
        spread = 1.5
    return _clamp(round(base + rng.uniform(-spread, spread), 3), 0.0, _MAX_CLEAN_FUEL_LPH)


def _derive_alarms(
    vehicles: tuple[Vehicle, ...],
    events: tuple[TelemetryEvent, ...],
    interval_minutes: int,
) -> tuple[Alarm, ...]:
    by_vehicle: dict[str, list[TelemetryEvent]] = {vehicle.vehicle_id: [] for vehicle in vehicles}
    for event in events:
        if event.vehicle_id in by_vehicle:
            by_vehicle[event.vehicle_id].append(event)
    found: list[tuple[datetime, str, str, str, str]] = []
    for vehicle in vehicles:
        series = by_vehicle[vehicle.vehicle_id]
        found.extend(_idle_alarms(vehicle.vehicle_id, series, interval_minutes))
        found.extend(_service_alarms(vehicle, series))
    found.sort(key=lambda item: (item[0], item[1], item[2]))
    return tuple(
        Alarm(
            alarm_id=f"ALM-{index:06d}",
            vehicle_id=vehicle_id,
            event_id=event_id,
            alarm_code=code,
            severity=severity,
            raised_at=raised_at,
        )
        for index, (raised_at, vehicle_id, code, severity, event_id) in enumerate(found, start=1)
    )


def _idle_alarms(
    vehicle_id: str,
    series: list[TelemetryEvent],
    interval_minutes: int,
) -> list[tuple[datetime, str, str, str, str]]:
    streak = 0
    raised = False
    alarms: list[tuple[datetime, str, str, str, str]] = []
    for event in series:
        if event.engine_state == "idle" and event.event_time is not None:
            streak += 1
            if not raised and streak * interval_minutes >= IDLE_MINUTES:
                alarms.append(
                    (event.event_time, vehicle_id, "IDLE_EXCESS", "warning", event.event_id)
                )
                raised = True
        else:
            streak = 0
            raised = False
    return alarms


def _service_alarms(
    vehicle: Vehicle,
    series: list[TelemetryEvent],
) -> list[tuple[datetime, str, str, str, str]]:
    previous = _baseline_engine_hours(int(vehicle.vehicle_id.removeprefix("VEH-")))
    alarms: list[tuple[datetime, str, str, str, str]] = []
    for event in series:
        current = event.engine_hours
        if (
            event.event_time is not None
            and current is not None
            and int(previous // SERVICE_INTERVAL_HOURS) < int(current // SERVICE_INTERVAL_HOURS)
        ):
            alarms.append(
                (event.event_time, vehicle.vehicle_id, "SERVICE_DUE", "high", event.event_id)
            )
        if current is not None:
            previous = current
    return alarms


def _derive_work_orders(
    vehicles: tuple[Vehicle, ...],
    alarms: tuple[Alarm, ...],
    profile: GenerationProfile,
) -> tuple[WorkOrder, ...]:
    site_by_vehicle = {vehicle.vehicle_id: vehicle.site_id for vehicle in vehicles}
    window_end = profile.window_start + timedelta(hours=profile.hours)
    service_alarms = [alarm for alarm in alarms if alarm.alarm_code == "SERVICE_DUE"]
    orders: list[WorkOrder] = []
    for index, alarm in enumerate(service_alarms, start=1):
        close_at = alarm.raised_at + WORK_ORDER_DURATION
        completed = close_at <= window_end
        orders.append(
            WorkOrder(
                work_order_id=f"WO-{index:06d}",
                vehicle_id=alarm.vehicle_id,
                site_id=site_by_vehicle[alarm.vehicle_id],
                alarm_id=alarm.alarm_id,
                opened_at=alarm.raised_at,
                closed_at=close_at if completed else None,
                work_type="service",
                cause_code="interval_service",
                status="completed" if completed else "open",
            )
        )
    return tuple(orders)


def _near_site(value: float, origin: float) -> float:
    bounded = _clamp(value, origin - SITE_RADIUS_DEGREES, origin + SITE_RADIUS_DEGREES)
    return _clamp(bounded, -180.0, 180.0)


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)
