"""Bronze column definitions shared by the loader and its tests."""

from dataclasses import dataclass
from typing import Literal

ColumnKind = Literal["string", "double", "timestamp", "date"]
LANDING_RUN_COLUMN = "_landing_run_id"
DATASETS = ("sites", "vehicles", "telemetry_events", "alarms", "work_orders")
GOLD_TABLES = ("vehicle_activity", "site_activity")
BUSINESS_KEYS: dict[str, str] = {
    "sites": "site_id",
    "vehicles": "vehicle_id",
    "telemetry_events": "event_id",
    "alarms": "alarm_id",
    "work_orders": "work_order_id",
}


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One landing field stored in a Bronze table."""

    name: str
    kind: ColumnKind
    nullable: bool


TABLES: dict[str, tuple[ColumnSpec, ...]] = {
    "sites": (
        ColumnSpec("site_id", "string", False),
        ColumnSpec("site_name", "string", False),
        ColumnSpec("latitude", "double", False),
        ColumnSpec("longitude", "double", False),
        ColumnSpec("timezone_name", "string", False),
    ),
    "vehicles": (
        ColumnSpec("vehicle_id", "string", False),
        ColumnSpec("asset_type", "string", False),
        ColumnSpec("model_code", "string", False),
        ColumnSpec("site_id", "string", False),
        ColumnSpec("status", "string", False),
        ColumnSpec("commissioned_on", "date", False),
    ),
    "telemetry_events": (
        ColumnSpec("event_id", "string", False),
        ColumnSpec("vehicle_id", "string", True),
        ColumnSpec("event_time", "timestamp", True),
        ColumnSpec("received_at", "timestamp", False),
        ColumnSpec("latitude", "double", True),
        ColumnSpec("longitude", "double", True),
        ColumnSpec("speed_kph", "double", True),
        ColumnSpec("engine_state", "string", True),
        ColumnSpec("fuel_rate_lph", "double", True),
        ColumnSpec("engine_hours", "double", True),
    ),
    "alarms": (
        ColumnSpec("alarm_id", "string", False),
        ColumnSpec("vehicle_id", "string", False),
        ColumnSpec("event_id", "string", True),
        ColumnSpec("alarm_code", "string", False),
        ColumnSpec("severity", "string", False),
        ColumnSpec("raised_at", "timestamp", False),
    ),
    "work_orders": (
        ColumnSpec("work_order_id", "string", False),
        ColumnSpec("vehicle_id", "string", False),
        ColumnSpec("site_id", "string", False),
        ColumnSpec("alarm_id", "string", True),
        ColumnSpec("opened_at", "timestamp", False),
        ColumnSpec("closed_at", "timestamp", True),
        ColumnSpec("work_type", "string", False),
        ColumnSpec("cause_code", "string", False),
        ColumnSpec("status", "string", False),
    ),
}
