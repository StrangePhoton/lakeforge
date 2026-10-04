"""Load and read the simulated operational source in PostgreSQL."""

import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection, OperationalError
from psycopg.rows import class_row

from lakeforge.config import Settings
from lakeforge.domain.models import Alarm, Site, TelemetryEvent, Vehicle, WorkOrder
from lakeforge.synthetic.generator import GeneratedDataset

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

SITE_COLUMNS = (
    "site_id",
    "site_name",
    "latitude",
    "longitude",
    "timezone_name",
)
VEHICLE_COLUMNS = (
    "vehicle_id",
    "asset_type",
    "model_code",
    "site_id",
    "status",
    "commissioned_on",
)
TELEMETRY_COLUMNS = (
    "event_id",
    "vehicle_id",
    "event_time",
    "received_at",
    "latitude",
    "longitude",
    "speed_kph",
    "engine_state",
    "fuel_rate_lph",
    "engine_hours",
)
ALARM_COLUMNS = (
    "alarm_id",
    "vehicle_id",
    "event_id",
    "alarm_code",
    "severity",
    "raised_at",
)
WORK_ORDER_COLUMNS = (
    "work_order_id",
    "vehicle_id",
    "site_id",
    "alarm_id",
    "opened_at",
    "closed_at",
    "work_type",
    "cause_code",
    "status",
)

_INSERT_SITES = """
INSERT INTO source.sites (
    site_id, site_name, latitude, longitude, timezone_name
) VALUES (%s, %s, %s, %s, %s)
"""
_INSERT_VEHICLES = """
INSERT INTO source.vehicles (
    vehicle_id, asset_type, model_code, site_id, status, commissioned_on
) VALUES (%s, %s, %s, %s, %s, %s)
"""
_INSERT_TELEMETRY = """
INSERT INTO source.telemetry_events (
    event_id, vehicle_id, event_time, received_at, latitude, longitude,
    speed_kph, engine_state, fuel_rate_lph, engine_hours
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
_INSERT_ALARMS = """
INSERT INTO source.alarms (
    alarm_id, vehicle_id, event_id, alarm_code, severity, raised_at
) VALUES (%s, %s, %s, %s, %s, %s)
"""
_INSERT_WORK_ORDERS = """
INSERT INTO source.work_orders (
    work_order_id, vehicle_id, site_id, alarm_id, opened_at, closed_at,
    work_type, cause_code, status
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
_SELECT_SITES = """
SELECT site_id, site_name, latitude, longitude, timezone_name
FROM source.sites
ORDER BY site_id
"""
_SELECT_VEHICLES = """
SELECT vehicle_id, asset_type, model_code, site_id, status, commissioned_on
FROM source.vehicles
ORDER BY vehicle_id
"""
_SELECT_TELEMETRY = """
SELECT
    event_id, vehicle_id, event_time, received_at, latitude, longitude,
    speed_kph, engine_state, fuel_rate_lph, engine_hours
FROM source.telemetry_events
ORDER BY event_time NULLS LAST, event_id
"""
_SELECT_ALARMS = """
SELECT alarm_id, vehicle_id, event_id, alarm_code, severity, raised_at
FROM source.alarms
ORDER BY alarm_id
"""
_SELECT_WORK_ORDERS = """
SELECT
    work_order_id, vehicle_id, site_id, alarm_id, opened_at, closed_at,
    work_type, cause_code, status
FROM source.work_orders
ORDER BY work_order_id
"""
_TRUNCATE = """
TRUNCATE TABLE
    source.work_orders,
    source.alarms,
    source.telemetry_events,
    source.vehicles,
    source.sites
"""


def connect(settings: Settings) -> Connection[tuple[Any, ...]]:
    """Open one session to the simulated source database."""
    connection = psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=5,
        application_name="lakeforge",
    )
    connection.execute("SET TIME ZONE 'UTC'")
    connection.commit()
    return connection


def connect_with_retry(
    settings: Settings,
    *,
    attempts: int = 10,
    delay_seconds: float = 1.0,
) -> Connection[tuple[Any, ...]]:
    """Connect, retrying only while PostgreSQL is still starting."""
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_error: OperationalError | None = None
    for attempt in range(1, attempts + 1):
        try:
            return connect(settings)
        except OperationalError as exc:
            last_error = exc
            if attempt == attempts or not _is_unreachable(exc):
                break
            time.sleep(delay_seconds)
    if last_error is None:
        raise RuntimeError("PostgreSQL connection was not attempted")
    raise last_error


def apply_schema(connection: Connection[tuple[Any, ...]]) -> None:
    """Execute the authoritative schema.sql script."""
    script = SCHEMA_PATH.read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        for statement in split_sql_script(script):
            cursor.execute(statement)
    connection.commit()


def recreate_schema(connection: Connection[tuple[Any, ...]]) -> None:
    """Drop and recreate the source schema from schema.sql.

    Generation replaces the simulator. It does not migrate an older shape.
    """
    connection.execute("DROP SCHEMA IF EXISTS source CASCADE")
    connection.commit()
    apply_schema(connection)


def truncate_source(connection: Connection[tuple[Any, ...]]) -> None:
    """Remove simulated source rows without dropping the schema."""
    connection.execute(_TRUNCATE)
    connection.commit()


def replace_dataset(
    connection: Connection[tuple[Any, ...]],
    dataset: GeneratedDataset,
) -> None:
    """Replace the source tables with one generated snapshot."""
    with connection.cursor() as cursor:
        cursor.execute(_TRUNCATE)
        if dataset.sites:
            cursor.executemany(_INSERT_SITES, [_site_params(row) for row in dataset.sites])
        if dataset.vehicles:
            cursor.executemany(_INSERT_VEHICLES, [_vehicle_params(row) for row in dataset.vehicles])
        if dataset.telemetry_events:
            cursor.executemany(
                _INSERT_TELEMETRY,
                [_telemetry_params(row) for row in dataset.telemetry_events],
            )
        if dataset.alarms:
            cursor.executemany(_INSERT_ALARMS, [_alarm_params(row) for row in dataset.alarms])
        if dataset.work_orders:
            cursor.executemany(
                _INSERT_WORK_ORDERS,
                [_work_order_params(row) for row in dataset.work_orders],
            )
    connection.commit()


def fetch_sites(connection: Connection[tuple[Any, ...]]) -> tuple[Site, ...]:
    """Read sites in primary-key order."""
    with connection.cursor(row_factory=class_row(Site)) as cursor:
        cursor.execute(_SELECT_SITES)
        return tuple(cursor.fetchall())


def fetch_vehicles(connection: Connection[tuple[Any, ...]]) -> tuple[Vehicle, ...]:
    """Read vehicles in primary-key order."""
    with connection.cursor(row_factory=class_row(Vehicle)) as cursor:
        cursor.execute(_SELECT_VEHICLES)
        return tuple(cursor.fetchall())


def fetch_telemetry(connection: Connection[tuple[Any, ...]]) -> tuple[TelemetryEvent, ...]:
    """Read telemetry ordered by event time, with null times last, then event id."""
    with connection.cursor(row_factory=class_row(TelemetryEvent)) as cursor:
        cursor.execute(_SELECT_TELEMETRY)
        return tuple(cursor.fetchall())


def fetch_alarms(connection: Connection[tuple[Any, ...]]) -> tuple[Alarm, ...]:
    """Read alarms in primary-key order."""
    with connection.cursor(row_factory=class_row(Alarm)) as cursor:
        cursor.execute(_SELECT_ALARMS)
        return tuple(cursor.fetchall())


def fetch_work_orders(connection: Connection[tuple[Any, ...]]) -> tuple[WorkOrder, ...]:
    """Read work orders in primary-key order."""
    with connection.cursor(row_factory=class_row(WorkOrder)) as cursor:
        cursor.execute(_SELECT_WORK_ORDERS)
        return tuple(cursor.fetchall())


def split_sql_script(script: str) -> list[str]:
    """Split a simple SQL script on semicolons, dropping comment-only chunks."""
    statements: list[str] = []
    for chunk in script.split(";"):
        lines = [
            line
            for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


def _site_params(site: Site) -> tuple[str, str, float, float, str]:
    return (site.site_id, site.site_name, site.latitude, site.longitude, site.timezone_name)


def _vehicle_params(vehicle: Vehicle) -> tuple[str, str, str, str, str, date]:
    return (
        vehicle.vehicle_id,
        vehicle.asset_type,
        vehicle.model_code,
        vehicle.site_id,
        vehicle.status,
        vehicle.commissioned_on,
    )


def _telemetry_params(
    event: TelemetryEvent,
) -> tuple[
    str,
    str | None,
    datetime | None,
    datetime,
    float | None,
    float | None,
    float | None,
    str | None,
    float | None,
    float | None,
]:
    return (
        event.event_id,
        event.vehicle_id,
        event.event_time,
        event.received_at,
        event.latitude,
        event.longitude,
        event.speed_kph,
        event.engine_state,
        event.fuel_rate_lph,
        event.engine_hours,
    )


def _alarm_params(alarm: Alarm) -> tuple[str, str, str | None, str, str, datetime]:
    return (
        alarm.alarm_id,
        alarm.vehicle_id,
        alarm.event_id,
        alarm.alarm_code,
        alarm.severity,
        alarm.raised_at,
    )


def _work_order_params(
    order: WorkOrder,
) -> tuple[str, str, str, str | None, datetime, datetime | None, str, str, str]:
    return (
        order.work_order_id,
        order.vehicle_id,
        order.site_id,
        order.alarm_id,
        order.opened_at,
        order.closed_at,
        order.work_type,
        order.cause_code,
        order.status,
    )


def _is_unreachable(exc: OperationalError) -> bool:
    sqlstate = exc.sqlstate
    return sqlstate is None or sqlstate.startswith("08")
