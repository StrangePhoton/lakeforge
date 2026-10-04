"""Load and read the simulated operational source in PostgreSQL."""

import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection, OperationalError
from psycopg.rows import class_row

from lakeforge.config import Settings
from lakeforge.domain.models import TelemetryEvent, Vehicle
from lakeforge.synthetic.generator import GeneratedDataset

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

VEHICLE_COLUMNS = (
    "vehicle_id",
    "asset_type",
    "model_code",
    "site_code",
    "status",
    "commissioned_on",
)
TELEMETRY_COLUMNS = (
    "event_id",
    "vehicle_id",
    "event_time",
    "latitude",
    "longitude",
    "speed_kph",
    "engine_state",
    "fuel_rate_lph",
    "engine_hours",
)

_INSERT_VEHICLES = """
INSERT INTO source.vehicles (
    vehicle_id,
    asset_type,
    model_code,
    site_code,
    status,
    commissioned_on
) VALUES (%s, %s, %s, %s, %s, %s)
"""
_INSERT_TELEMETRY = """
INSERT INTO source.telemetry_events (
    event_id,
    vehicle_id,
    event_time,
    latitude,
    longitude,
    speed_kph,
    engine_state,
    fuel_rate_lph,
    engine_hours
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
_SELECT_VEHICLES = """
SELECT vehicle_id, asset_type, model_code, site_code, status, commissioned_on
FROM source.vehicles
ORDER BY vehicle_id
"""
_SELECT_TELEMETRY = """
SELECT
    event_id,
    vehicle_id,
    event_time,
    latitude,
    longitude,
    speed_kph,
    engine_state,
    fuel_rate_lph,
    engine_hours
FROM source.telemetry_events
ORDER BY event_time, event_id
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


def truncate_source(connection: Connection[tuple[Any, ...]]) -> None:
    """Remove simulated source rows without dropping the schema."""
    connection.execute("TRUNCATE TABLE source.telemetry_events, source.vehicles")
    connection.commit()


def replace_dataset(
    connection: Connection[tuple[Any, ...]],
    dataset: GeneratedDataset,
) -> None:
    """Replace the source tables with one generated snapshot."""
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE source.telemetry_events, source.vehicles")
        if dataset.vehicles:
            cursor.executemany(_INSERT_VEHICLES, [_vehicle_params(row) for row in dataset.vehicles])
        if dataset.telemetry_events:
            cursor.executemany(
                _INSERT_TELEMETRY,
                [_telemetry_params(row) for row in dataset.telemetry_events],
            )
    connection.commit()


def fetch_vehicles(connection: Connection[tuple[Any, ...]]) -> tuple[Vehicle, ...]:
    """Read vehicles in primary-key order."""
    with connection.cursor(row_factory=class_row(Vehicle)) as cursor:
        cursor.execute(_SELECT_VEHICLES)
        return tuple(cursor.fetchall())


def fetch_telemetry(connection: Connection[tuple[Any, ...]]) -> tuple[TelemetryEvent, ...]:
    """Read telemetry ordered by event time, then event id."""
    with connection.cursor(row_factory=class_row(TelemetryEvent)) as cursor:
        cursor.execute(_SELECT_TELEMETRY)
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


def _vehicle_params(vehicle: Vehicle) -> tuple[str, str, str, str, str, date]:
    return (
        vehicle.vehicle_id,
        vehicle.asset_type,
        vehicle.model_code,
        vehicle.site_code,
        vehicle.status,
        vehicle.commissioned_on,
    )


def _telemetry_params(
    event: TelemetryEvent,
) -> tuple[str, str, datetime, float, float, float, str, float, float]:
    return (
        event.event_id,
        event.vehicle_id,
        event.event_time,
        event.latitude,
        event.longitude,
        event.speed_kph,
        event.engine_state,
        event.fuel_rate_lph,
        event.engine_hours,
    )


def _is_unreachable(exc: OperationalError) -> bool:
    sqlstate = exc.sqlstate
    return sqlstate is None or sqlstate.startswith("08")
