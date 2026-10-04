import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakeforge.domain.models import TelemetryEvent, Vehicle
from lakeforge.ingest.landing import (
    read_manifest,
    read_telemetry,
    read_vehicles,
    write_landing_run,
)
from lakeforge.synthetic.generator import generate_dataset

_STARTED = datetime(2026, 1, 12, 12, 0, tzinfo=UTC)
_FINISHED = datetime(2026, 1, 12, 12, 0, 2, tzinfo=UTC)


def test_jsonl_round_trip_preserves_records(tmp_path: Path) -> None:
    dataset = generate_dataset(seed=42, vehicle_count=4, hours=6, interval_minutes=15)
    run_dir = write_landing_run(
        landing_dir=tmp_path,
        run_id="round-trip",
        vehicles=dataset.vehicles,
        telemetry_events=dataset.telemetry_events,
        seed=42,
        started_at=_STARTED,
        finished_at=_FINISHED,
    )

    assert read_vehicles(run_dir / "vehicles.jsonl") == dataset.vehicles
    assert read_telemetry(run_dir / "telemetry_events.jsonl") == dataset.telemetry_events
    vehicle_line = (run_dir / "vehicles.jsonl").read_text(encoding="utf-8").splitlines()[0]
    assert list(json.loads(vehicle_line)) == [
        "vehicle_id",
        "asset_type",
        "model_code",
        "site_code",
        "status",
        "commissioned_on",
    ]
    event_line = (run_dir / "telemetry_events.jsonl").read_text(encoding="utf-8").splitlines()[0]
    assert json.loads(event_line)["event_time"] == "2026-01-12T06:00:00+00:00"


def test_manifest_records_snapshot_counts(tmp_path: Path) -> None:
    vehicle = Vehicle(
        vehicle_id="VEH-0001",
        asset_type="haul_truck",
        model_code="HT-240",
        site_code="SITE-ALPHA",
        status="active",
        commissioned_on=datetime(2020, 1, 15, tzinfo=UTC).date(),
    )
    run_dir = write_landing_run(
        landing_dir=tmp_path,
        run_id="manifest-run",
        vehicles=(vehicle,),
        telemetry_events=(),
        seed=None,
        started_at=_STARTED,
        finished_at=_FINISHED,
    )

    manifest = read_manifest(run_dir / "manifest.json")
    assert manifest["run_id"] == "manifest-run"
    assert manifest["pipeline"] == "m0_source_snapshot"
    assert manifest["seed"] is None
    assert manifest["source"] == {"system": "fleet_oltp", "mode": "full_snapshot"}
    assert manifest["datasets"] == {
        "vehicles": {"rows": 1},
        "telemetry_events": {"rows": 0},
    }
    assert manifest["status"] == "succeeded"
    assert manifest["started_at"] == "2026-01-12T12:00:00+00:00"


def test_run_id_cannot_escape_the_landing_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="run_id"):
        write_landing_run(
            landing_dir=tmp_path,
            run_id="../escape",
            vehicles=(),
            telemetry_events=(),
            seed=1,
            started_at=_STARTED,
            finished_at=_FINISHED,
        )


def test_naive_timestamps_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        write_landing_run(
            landing_dir=tmp_path,
            run_id="naive",
            vehicles=(),
            telemetry_events=(
                TelemetryEvent(
                    event_id="EVT-0001-000001",
                    vehicle_id="VEH-0001",
                    event_time=datetime(2026, 1, 12, 6, 0),
                    latitude=45.0,
                    longitude=-90.0,
                    speed_kph=0.0,
                    engine_state="off",
                    fuel_rate_lph=0.0,
                    engine_hours=1000.0,
                ),
            ),
            seed=1,
            started_at=_STARTED,
            finished_at=_FINISHED,
        )
