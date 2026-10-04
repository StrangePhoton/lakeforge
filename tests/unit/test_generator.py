from datetime import UTC, timedelta

import pytest

from lakeforge.synthetic.generator import (
    MAX_FUEL_RATE_LPH,
    MAX_SPEED_KPH,
    WINDOW_START,
    generate_dataset,
)

_PROFILE = {
    "seed": 42,
    "vehicle_count": 4,
    "hours": 6,
    "interval_minutes": 15,
}


def test_default_profile_size_and_fixed_clock() -> None:
    dataset = generate_dataset(**_PROFILE)
    window_end = WINDOW_START + timedelta(hours=6)

    assert len(dataset.vehicles) == 4
    assert len(dataset.telemetry_events) == 96
    assert dataset.telemetry_events[0].event_time == WINDOW_START
    assert all(WINDOW_START <= event.event_time < window_end for event in dataset.telemetry_events)
    assert all(event.event_time.tzinfo == UTC for event in dataset.telemetry_events)


def test_same_seed_is_identical() -> None:
    assert generate_dataset(**_PROFILE) == generate_dataset(**_PROFILE)


def test_different_seed_changes_telemetry() -> None:
    first = generate_dataset(**_PROFILE)
    second = generate_dataset(**{**_PROFILE, "seed": 43})

    assert first.vehicles == second.vehicles
    assert first.telemetry_events != second.telemetry_events


def test_events_reference_known_vehicles() -> None:
    dataset = generate_dataset(**_PROFILE)
    vehicle_ids = {vehicle.vehicle_id for vehicle in dataset.vehicles}

    assert vehicle_ids == {"VEH-0001", "VEH-0002", "VEH-0003", "VEH-0004"}
    assert {vehicle.asset_type for vehicle in dataset.vehicles} == {
        "haul_truck",
        "excavator",
        "wheel_loader",
        "dozer",
    }
    assert all(event.vehicle_id in vehicle_ids for event in dataset.telemetry_events)


def test_measurements_stay_inside_physical_bounds() -> None:
    dataset = generate_dataset(**_PROFILE)
    events_by_vehicle = {vehicle.vehicle_id: vehicle for vehicle in dataset.vehicles}

    for event in dataset.telemetry_events:
        assert -90.0 <= event.latitude <= 90.0
        assert -180.0 <= event.longitude <= 180.0
        assert 0.0 <= event.speed_kph <= MAX_SPEED_KPH
        assert 0.0 <= event.fuel_rate_lph <= MAX_FUEL_RATE_LPH
        assert event.engine_state in {"off", "idle", "working"}
        asset_type = events_by_vehicle[event.vehicle_id].asset_type
        if event.engine_state in {"off", "idle"} or asset_type == "excavator":
            assert event.speed_kph == 0.0
        if event.engine_state == "off":
            assert event.fuel_rate_lph == 0.0


def test_events_are_ordered_by_time_then_id() -> None:
    dataset = generate_dataset(**_PROFILE)
    ordering = [(event.event_time, event.event_id) for event in dataset.telemetry_events]

    assert ordering == sorted(ordering)


def test_invalid_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="vehicle_count"):
        generate_dataset(seed=1, vehicle_count=0, hours=1, interval_minutes=15)
    with pytest.raises(ValueError, match="interval_minutes"):
        generate_dataset(seed=1, vehicle_count=1, hours=1, interval_minutes=120)
