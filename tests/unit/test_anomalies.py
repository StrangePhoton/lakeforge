from datetime import timedelta

from lakeforge.synthetic.anomalies import ANOMALY_KINDS
from lakeforge.synthetic.generator import generate_dataset


def test_tiny_anomaly_catalog_is_fixed() -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    clean = generate_dataset(seed=42, profile="tiny", anomalies=False)
    mutated = {event.event_id: event for event in dataset.telemetry_events}
    original = {event.event_id: event for event in clean.telemetry_events}
    late_original = original["EVT-0003-000012"]

    assert [record.kind for record in dataset.anomalies] == list(ANOMALY_KINDS)
    assert all(record.table == "telemetry_events" for record in dataset.anomalies)
    assert len(dataset.telemetry_events) == 97
    assert mutated["EVT-DUP-0001"].event_time == mutated["EVT-0001-000010"].event_time
    assert mutated["EVT-DUP-0001"].vehicle_id == "VEH-0001"
    assert mutated["EVT-0002-000003"].latitude is None
    assert mutated["EVT-0002-000003"].longitude is None
    assert mutated["EVT-0003-000004"].speed_kph is None
    assert mutated["EVT-0004-000005"].event_time is None
    assert mutated["EVT-0004-000005"].received_at == original["EVT-0004-000005"].received_at
    assert mutated["EVT-0001-000006"].vehicle_id is None
    assert mutated["EVT-0002-000007"].vehicle_id == "VEH-9999"
    assert mutated["EVT-0001-000008"].speed_kph == 240.0
    assert mutated["EVT-0003-000009"].latitude == 95.0
    assert mutated["EVT-0004-000010"].longitude == 200.0
    assert mutated["EVT-0002-000011"].engine_state == "unknown"
    assert late_original.event_time is not None
    assert mutated["EVT-0003-000012"].event_time == late_original.event_time - timedelta(hours=12)
    assert mutated["EVT-0003-000012"].received_at == late_original.received_at
    assert mutated["EVT-0001-000013"].received_at == original["EVT-0001-000014"].received_at
    assert mutated["EVT-0001-000014"].received_at == original["EVT-0001-000013"].received_at


def test_clean_profile_has_no_catalog() -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=False)

    assert dataset.anomalies == ()
    assert len(dataset.telemetry_events) == 96
    assert all(event.vehicle_id != "VEH-9999" for event in dataset.telemetry_events)


def test_large_profile_keeps_the_same_defect_count() -> None:
    dataset = generate_dataset(seed=1, profile="large", anomalies=True)

    assert len(dataset.telemetry_events) == 13441
    assert [record.kind for record in dataset.anomalies] == list(ANOMALY_KINDS)
