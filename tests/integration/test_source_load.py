from datetime import UTC, datetime

import pytest
from psycopg import Connection
from psycopg.errors import ForeignKeyViolation

from lakeforge.source.postgres import (
    fetch_alarms,
    fetch_sites,
    fetch_telemetry,
    fetch_vehicles,
    fetch_work_orders,
    replace_dataset,
)
from lakeforge.synthetic.generator import generate_dataset

pytestmark = pytest.mark.integration

_CLEAN = {
    "seed": 42,
    "profile": "tiny",
    "anomalies": False,
}


def test_snapshot_matches_generated_rows(source_connection: Connection[tuple[object, ...]]) -> None:
    dataset = generate_dataset(**_CLEAN)
    replace_dataset(source_connection, dataset)

    assert fetch_sites(source_connection) == dataset.sites
    assert fetch_vehicles(source_connection) == dataset.vehicles
    assert fetch_telemetry(source_connection) == dataset.telemetry_events
    assert fetch_alarms(source_connection) == dataset.alarms
    assert fetch_work_orders(source_connection) == dataset.work_orders


def test_reload_with_the_same_seed_is_idempotent(
    source_connection: Connection[tuple[object, ...]],
) -> None:
    dataset = generate_dataset(**_CLEAN)
    replace_dataset(source_connection, dataset)
    replace_dataset(source_connection, generate_dataset(**_CLEAN))

    assert fetch_sites(source_connection) == dataset.sites
    assert fetch_vehicles(source_connection) == dataset.vehicles
    assert fetch_telemetry(source_connection) == dataset.telemetry_events
    assert fetch_alarms(source_connection) == dataset.alarms
    assert fetch_work_orders(source_connection) == dataset.work_orders


def test_telemetry_feed_accepts_incomplete_events_and_master_data_stays_constrained(
    source_connection: Connection[tuple[object, ...]],
) -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    replace_dataset(source_connection, dataset)
    telemetry = fetch_telemetry(source_connection)

    assert any(event.vehicle_id is None for event in telemetry)
    assert any(event.vehicle_id == "VEH-9999" for event in telemetry)
    assert any(event.event_time is None for event in telemetry)
    assert all(event.received_at is not None for event in telemetry)

    raised_at = datetime(2026, 1, 12, 7, 0, tzinfo=UTC)
    with pytest.raises(ForeignKeyViolation):
        source_connection.execute(
            """
            INSERT INTO source.vehicles (
                vehicle_id, asset_type, model_code, site_id, status, commissioned_on
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("VEH-BAD", "haul_truck", "HT-240", "SITE-MISSING", "active", raised_at.date()),
        )
    source_connection.rollback()
    with pytest.raises(ForeignKeyViolation):
        source_connection.execute(
            """
            INSERT INTO source.alarms (
                alarm_id, vehicle_id, event_id, alarm_code, severity, raised_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("ALM-BAD", "VEH-NOPE", None, "IDLE_EXCESS", "warning", raised_at),
        )
    source_connection.rollback()
    with pytest.raises(ForeignKeyViolation):
        source_connection.execute(
            """
            INSERT INTO source.work_orders (
                work_order_id, vehicle_id, site_id, alarm_id, opened_at, closed_at,
                work_type, cause_code, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "WO-BAD",
                "VEH-0001",
                "SITE-ALPHA",
                "ALM-MISSING",
                raised_at,
                None,
                "service",
                "interval_service",
                "open",
            ),
        )
    source_connection.rollback()
