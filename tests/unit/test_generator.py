from datetime import UTC, timedelta

import pytest

from lakeforge.synthetic.generator import generate_dataset
from lakeforge.synthetic.profiles import SITES, resolve_profile

_TINY = {"seed": 42, "profile": "tiny", "anomalies": False}


def test_profile_sizes() -> None:
    tiny = generate_dataset(**_TINY)
    dev = generate_dataset(seed=42, profile="dev", anomalies=False)
    large = generate_dataset(seed=1, profile="large", anomalies=False)

    assert len(tiny.sites) == 2
    assert len(tiny.vehicles) == 4
    assert len(tiny.telemetry_events) == 96
    assert len(dev.vehicles) == 8
    assert len(dev.telemetry_events) == 768
    assert len(large.vehicles) == 20
    assert len(large.telemetry_events) == 13440
    assert (
        tiny.telemetry_events[0].event_time
        == resolve_profile(
            "tiny", vehicle_count=None, hours=None, interval_minutes=None
        ).window_start
    )


def test_same_seed_is_identical() -> None:
    assert generate_dataset(**_TINY) == generate_dataset(**_TINY)


def test_different_seed_changes_measurements_only() -> None:
    first = generate_dataset(seed=1, profile="tiny", anomalies=True)
    second = generate_dataset(seed=2, profile="tiny", anomalies=True)

    assert first.vehicles == second.vehicles
    assert first.anomalies == second.anomalies
    assert [event.event_id for event in first.telemetry_events] == [
        event.event_id for event in second.telemetry_events
    ]
    assert first.telemetry_events != second.telemetry_events


def test_vehicles_belong_to_known_sites() -> None:
    dataset = generate_dataset(**_TINY)
    site_ids = {site.site_id for site in dataset.sites}

    assert site_ids == {"SITE-ALPHA", "SITE-BRAVO"}
    assert {vehicle.site_id for vehicle in dataset.vehicles} <= site_ids
    assert {vehicle.asset_type for vehicle in dataset.vehicles} == {
        "haul_truck",
        "excavator",
        "wheel_loader",
        "dozer",
    }
    assert all(
        event.vehicle_id in {vehicle.vehicle_id for vehicle in dataset.vehicles}
        for event in dataset.telemetry_events
    )


def test_clean_behavior_follows_asset_and_engine_state() -> None:
    dataset = generate_dataset(**_TINY)
    sites = {site.site_id: site for site in SITES}
    vehicles = {vehicle.vehicle_id: vehicle for vehicle in dataset.vehicles}
    haul_fuel: list[float] = []
    dozer_fuel: list[float] = []

    for event in dataset.telemetry_events:
        assert event.vehicle_id is not None
        assert event.event_time is not None
        assert event.received_at == event.event_time
        assert event.engine_state in {"off", "idle", "working"}
        vehicle = vehicles[event.vehicle_id]
        site = sites[vehicle.site_id]
        assert event.latitude is not None and event.longitude is not None
        assert abs(event.latitude - site.latitude) <= 0.02
        assert abs(event.longitude - site.longitude) <= 0.02
        if vehicle.asset_type == "excavator":
            assert event.speed_kph == 0.0
            assert event.latitude == site.latitude
            assert event.longitude == site.longitude
        if event.engine_state in {"off", "idle"}:
            assert event.speed_kph == 0.0
        if event.engine_state == "off":
            assert event.fuel_rate_lph == 0.0
        if event.engine_state == "working" and event.fuel_rate_lph is not None:
            if vehicle.asset_type == "haul_truck":
                haul_fuel.append(event.fuel_rate_lph)
            if vehicle.asset_type == "dozer":
                dozer_fuel.append(event.fuel_rate_lph)

    assert haul_fuel
    assert dozer_fuel
    assert min(haul_fuel) > max(dozer_fuel)


def test_off_hours_do_not_burn_fuel() -> None:
    dataset = generate_dataset(seed=42, profile="dev", anomalies=False)
    off = [event for event in dataset.telemetry_events if event.engine_state == "off"]

    assert off
    assert all(event.fuel_rate_lph == 0.0 and event.speed_kph == 0.0 for event in off)


def test_service_and_idle_rules_on_tiny() -> None:
    dataset = generate_dataset(**_TINY)
    service = [alarm for alarm in dataset.alarms if alarm.alarm_code == "SERVICE_DUE"]
    idle = [alarm for alarm in dataset.alarms if alarm.alarm_code == "IDLE_EXCESS"]

    assert len(service) == 1
    assert service[0].vehicle_id == "VEH-0001"
    assert service[0].raised_at.astimezone(UTC).hour == 6
    assert service[0].raised_at.astimezone(UTC).minute == 45
    assert len(idle) == 4
    assert len(dataset.work_orders) == 1
    order = dataset.work_orders[0]
    assert order.alarm_id == service[0].alarm_id
    assert order.cause_code == "interval_service"
    assert order.status == "completed"
    assert order.closed_at == order.opened_at + timedelta(hours=4)
    assert all(
        alarm.alarm_code != "IDLE_EXCESS" or alarm.alarm_id != order.alarm_id for alarm in idle
    )


def test_dev_idle_cycle_raises_two_alarms_per_vehicle() -> None:
    dataset = generate_dataset(seed=3, profile="dev", anomalies=False)
    idle = [alarm for alarm in dataset.alarms if alarm.alarm_code == "IDLE_EXCESS"]
    service = [alarm for alarm in dataset.alarms if alarm.alarm_code == "SERVICE_DUE"]

    assert len(idle) == 16
    assert len(service) == 1
    assert len(dataset.work_orders) == 1


def test_invalid_size_is_rejected() -> None:
    with pytest.raises(ValueError, match="vehicle_count"):
        generate_dataset(seed=1, profile="tiny", vehicle_count=0, anomalies=False)
    with pytest.raises(ValueError, match="interval_minutes"):
        generate_dataset(seed=1, profile="tiny", hours=1, interval_minutes=120, anomalies=False)
    with pytest.raises(ValueError, match="unknown profile"):
        generate_dataset(seed=1, profile="bench", anomalies=False)
