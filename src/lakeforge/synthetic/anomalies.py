"""Fixed telemetry defects applied after the operational story is built."""

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from lakeforge.domain.models import AnomalyRecord, TelemetryEvent

ANOMALY_KINDS: tuple[str, ...] = (
    "duplicate",
    "null_position",
    "null_speed",
    "null_event_time",
    "null_vehicle",
    "missing_reference",
    "impossible_speed",
    "impossible_latitude",
    "impossible_longitude",
    "invalid_engine_state",
    "late_event",
    "out_of_order",
)

_TABLE = "telemetry_events"
_DUPLICATE_ID = "EVT-DUP-0001"


def apply_anomaly_plan(
    events: tuple[TelemetryEvent, ...],
) -> tuple[tuple[TelemetryEvent, ...], tuple[AnomalyRecord, ...]]:
    """Return telemetry with the fixed defect plan applied.

    Targets that are not in the series are skipped. Placement does not use
    the random seed.
    """
    indexed = {event.event_id: event for event in events}
    catalog: list[AnomalyRecord] = []
    extras: list[TelemetryEvent] = []

    source = indexed.get("EVT-0001-000010")
    if source is not None:
        extras.append(replace(source, event_id=_DUPLICATE_ID))
        catalog.append(_record("duplicate", _DUPLICATE_ID, "event_time"))

    _edit(
        indexed,
        catalog,
        "EVT-0002-000003",
        "null_position",
        "latitude",
        lambda event: replace(event, latitude=None, longitude=None),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0003-000004",
        "null_speed",
        "speed_kph",
        lambda event: replace(event, speed_kph=None),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0004-000005",
        "null_event_time",
        "event_time",
        lambda event: replace(event, event_time=None),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0001-000006",
        "null_vehicle",
        "vehicle_id",
        lambda event: replace(event, vehicle_id=None),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0002-000007",
        "missing_reference",
        "vehicle_id",
        lambda event: replace(event, vehicle_id="VEH-9999"),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0001-000008",
        "impossible_speed",
        "speed_kph",
        lambda event: replace(event, speed_kph=240.0),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0003-000009",
        "impossible_latitude",
        "latitude",
        lambda event: replace(event, latitude=95.0),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0004-000010",
        "impossible_longitude",
        "longitude",
        lambda event: replace(event, longitude=200.0),
    )
    _edit(
        indexed,
        catalog,
        "EVT-0002-000011",
        "invalid_engine_state",
        "engine_state",
        lambda event: replace(event, engine_state="unknown"),
    )

    late = indexed.get("EVT-0003-000012")
    if late is not None and late.event_time is not None:
        indexed[late.event_id] = replace(late, event_time=late.event_time - timedelta(hours=12))
        catalog.append(_record("late_event", late.event_id, "event_time"))

    earlier = indexed.get("EVT-0001-000013")
    later = indexed.get("EVT-0001-000014")
    if earlier is not None and later is not None:
        indexed[earlier.event_id] = replace(earlier, received_at=later.received_at)
        indexed[later.event_id] = replace(later, received_at=earlier.received_at)
        catalog.append(_record("out_of_order", earlier.event_id, "received_at"))

    combined = tuple(sorted((*indexed.values(), *extras), key=_telemetry_order))
    return combined, tuple(catalog)


def _edit(
    indexed: dict[str, TelemetryEvent],
    catalog: list[AnomalyRecord],
    event_id: str,
    kind: str,
    field: str,
    edit: Callable[[TelemetryEvent], TelemetryEvent],
) -> None:
    current = indexed.get(event_id)
    if current is None:
        return
    indexed[event_id] = edit(current)
    catalog.append(_record(kind, event_id, field))


def _record(kind: str, record_id: str, field: str) -> AnomalyRecord:
    return AnomalyRecord(kind=kind, table=_TABLE, record_id=record_id, field=field)


def _telemetry_order(event: TelemetryEvent) -> tuple[bool, datetime, str]:
    if event.event_time is None:
        return (True, datetime.max.replace(tzinfo=UTC), event.event_id)
    return (False, event.event_time, event.event_id)
