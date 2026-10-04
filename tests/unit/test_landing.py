import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakeforge.domain.models import TelemetryEvent, Vehicle
from lakeforge.ingest.landing import (
    read_alarms,
    read_anomalies,
    read_manifest,
    read_sites,
    read_telemetry,
    read_vehicles,
    read_work_orders,
    write_landing_run,
)
from lakeforge.synthetic.generator import generate_dataset

_STARTED = datetime(2026, 1, 12, 12, 0, tzinfo=UTC)
_FINISHED = datetime(2026, 1, 12, 12, 0, 2, tzinfo=UTC)


def test_jsonl_round_trip_preserves_records(tmp_path: Path) -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    run_dir = write_landing_run(
        landing_dir=tmp_path,
        run_id="round-trip",
        sites=dataset.sites,
        vehicles=dataset.vehicles,
        telemetry_events=dataset.telemetry_events,
        alarms=dataset.alarms,
        work_orders=dataset.work_orders,
        anomalies=dataset.anomalies,
        seed=42,
        profile="tiny",
        anomalies_applied=True,
        started_at=_STARTED,
        finished_at=_FINISHED,
    )

    assert read_sites(run_dir / "sites.jsonl") == dataset.sites
    assert read_vehicles(run_dir / "vehicles.jsonl") == dataset.vehicles
    assert read_telemetry(run_dir / "telemetry_events.jsonl") == dataset.telemetry_events
    assert read_alarms(run_dir / "alarms.jsonl") == dataset.alarms
    assert read_work_orders(run_dir / "work_orders.jsonl") == dataset.work_orders
    assert read_anomalies(run_dir / "anomalies.json") == dataset.anomalies
    vehicle_line = (run_dir / "vehicles.jsonl").read_text(encoding="utf-8").splitlines()[0]
    assert list(json.loads(vehicle_line)) == [
        "vehicle_id",
        "asset_type",
        "model_code",
        "site_id",
        "status",
        "commissioned_on",
    ]
    telemetry_text = (run_dir / "telemetry_events.jsonl").read_text(encoding="utf-8")
    assert "is_anomaly" not in telemetry_text
    rows = [json.loads(line) for line in telemetry_text.splitlines()]
    opening = next(row for row in rows if row["event_id"] == "EVT-0001-000001")
    assert opening["event_time"] == "2026-01-12T06:00:00+00:00"


def test_manifest_records_snapshot_counts(tmp_path: Path) -> None:
    vehicle = Vehicle(
        vehicle_id="VEH-0001",
        asset_type="haul_truck",
        model_code="HT-240",
        site_id="SITE-ALPHA",
        status="active",
        commissioned_on=datetime(2020, 1, 15, tzinfo=UTC).date(),
    )
    run_dir = write_landing_run(
        landing_dir=tmp_path,
        run_id="manifest-run",
        sites=(),
        vehicles=(vehicle,),
        telemetry_events=(),
        alarms=(),
        work_orders=(),
        anomalies=(),
        seed=None,
        profile="tiny",
        anomalies_applied=False,
        started_at=_STARTED,
        finished_at=_FINISHED,
    )

    manifest = read_manifest(run_dir / "manifest.json")
    assert manifest["run_id"] == "manifest-run"
    assert manifest["pipeline"] == "source_snapshot"
    assert manifest["seed"] is None
    assert manifest["source"] == {"system": "fleet_oltp", "mode": "full_snapshot"}
    assert manifest["datasets"] == {
        "sites": {"rows": 0},
        "vehicles": {"rows": 1},
        "telemetry_events": {"rows": 0},
        "alarms": {"rows": 0},
        "work_orders": {"rows": 0},
    }
    assert manifest["generation"] == {
        "profile": "tiny",
        "anomalies_applied": False,
        "anomaly_count": 0,
    }
    assert manifest["status"] == "succeeded"
    assert manifest["started_at"] == "2026-01-12T12:00:00+00:00"
    assert json.loads((run_dir / "anomalies.json").read_text(encoding="utf-8")) == []


def test_ingest_without_a_catalog_omits_the_answer_key(tmp_path: Path) -> None:
    run_dir = write_landing_run(
        landing_dir=tmp_path,
        run_id="ingest-only",
        sites=(),
        vehicles=(),
        telemetry_events=(),
        alarms=(),
        work_orders=(),
        anomalies=None,
        seed=None,
        profile=None,
        anomalies_applied=None,
        started_at=_STARTED,
        finished_at=_FINISHED,
    )

    assert not (run_dir / "anomalies.json").exists()
    assert "generation" not in read_manifest(run_dir / "manifest.json")


def test_run_id_cannot_escape_the_landing_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="run_id"):
        write_landing_run(
            landing_dir=tmp_path,
            run_id="../escape",
            sites=(),
            vehicles=(),
            telemetry_events=(),
            alarms=(),
            work_orders=(),
            anomalies=None,
            seed=1,
            profile=None,
            anomalies_applied=None,
            started_at=_STARTED,
            finished_at=_FINISHED,
        )


def test_naive_timestamps_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        write_landing_run(
            landing_dir=tmp_path,
            run_id="naive",
            sites=(),
            vehicles=(),
            telemetry_events=(
                TelemetryEvent(
                    event_id="EVT-0001-000001",
                    vehicle_id="VEH-0001",
                    event_time=datetime(2026, 1, 12, 6, 0, tzinfo=UTC),
                    received_at=datetime(2026, 1, 12, 6, 0),
                    latitude=45.0,
                    longitude=-90.0,
                    speed_kph=0.0,
                    engine_state="idle",
                    fuel_rate_lph=3.5,
                    engine_hours=249.25,
                ),
            ),
            alarms=(),
            work_orders=(),
            anomalies=None,
            seed=1,
            profile=None,
            anomalies_applied=None,
            started_at=_STARTED,
            finished_at=_FINISHED,
        )
