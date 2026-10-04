"""Operational records for the simulated industrial source."""

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class Site:
    """A fictional operating location owned by the haulage organization."""

    site_id: str
    site_name: str
    latitude: float
    longitude: float
    timezone_name: str


@dataclass(frozen=True, slots=True)
class Vehicle:
    """One industrial asset assigned to a site."""

    vehicle_id: str
    asset_type: str
    model_code: str
    site_id: str
    status: str
    commissioned_on: date


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    """One reading from the external machine event feed.

    ``vehicle_id`` and ``event_time`` may be missing. ``received_at`` is the
    time the source stored the reading and is always present.
    """

    event_id: str
    vehicle_id: str | None
    event_time: datetime | None
    received_at: datetime
    latitude: float | None
    longitude: float | None
    speed_kph: float | None
    engine_state: str | None
    fuel_rate_lph: float | None
    engine_hours: float | None


@dataclass(frozen=True, slots=True)
class Alarm:
    """An alarm raised from clean telemetry rules."""

    alarm_id: str
    vehicle_id: str
    event_id: str | None
    alarm_code: str
    severity: str
    raised_at: datetime


@dataclass(frozen=True, slots=True)
class WorkOrder:
    """Maintenance opened from a service alarm."""

    work_order_id: str
    vehicle_id: str
    site_id: str
    alarm_id: str | None
    opened_at: datetime
    closed_at: datetime | None
    work_type: str
    cause_code: str
    status: str


@dataclass(frozen=True, slots=True)
class AnomalyRecord:
    """Simulation answer key for one injected telemetry defect.

    This is not stored on the operational row.
    """

    kind: str
    table: str
    record_id: str
    field: str
