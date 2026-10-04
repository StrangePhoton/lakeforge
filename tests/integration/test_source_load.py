import pytest

from lakeforge.source.postgres import fetch_telemetry, fetch_vehicles, replace_dataset
from lakeforge.synthetic.generator import generate_dataset

pytestmark = pytest.mark.integration

_PROFILE = {
    "seed": 42,
    "vehicle_count": 4,
    "hours": 6,
    "interval_minutes": 15,
}


def test_snapshot_matches_generated_rows(source_connection: object) -> None:
    dataset = generate_dataset(**_PROFILE)
    replace_dataset(source_connection, dataset)

    assert fetch_vehicles(source_connection) == dataset.vehicles
    assert fetch_telemetry(source_connection) == dataset.telemetry_events


def test_reload_with_the_same_seed_is_idempotent(source_connection: object) -> None:
    dataset = generate_dataset(**_PROFILE)
    replace_dataset(source_connection, dataset)
    replace_dataset(source_connection, generate_dataset(**_PROFILE))

    assert fetch_vehicles(source_connection) == dataset.vehicles
    assert fetch_telemetry(source_connection) == dataset.telemetry_events
