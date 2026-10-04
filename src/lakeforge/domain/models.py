"""Source-system records for the Milestone 0 fleet simulation."""

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class Vehicle:
    """One industrial asset in the simulated operational source."""

    vehicle_id: str
    asset_type: str
    model_code: str
    site_code: str
    status: str
    commissioned_on: date


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    """One raw telemetry reading as it would be stored by the source system."""

    event_id: str
    vehicle_id: str
    event_time: datetime
    latitude: float
    longitude: float
    speed_kph: float
    engine_state: str
    fuel_rate_lph: float
    engine_hours: float
