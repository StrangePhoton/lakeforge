import json
from pathlib import Path

import pytest

from lakeforge.cli import main
from lakeforge.config import Settings
from lakeforge.lakehouse.catalog import DATASETS
from lakeforge.lakehouse.paths import resolve_stream_dir
from lakeforge.lakehouse.stream import (
    resolve_bootstrap,
    run_publish,
    telemetry_kafka_record,
)


def test_telemetry_record_uses_the_event_id_as_the_kafka_key() -> None:
    line = json.dumps(_telemetry("EVT-1"))

    key, value = telemetry_kafka_record(line)

    assert key == "EVT-1"
    assert json.loads(value) == _telemetry("EVT-1")


def test_publish_rejects_a_contract_violation_before_kafka(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    landing = _landing(tmp_path / "landing", "drift", extra={"note": "later"})

    def _refuse_spark(**_kwargs: object) -> None:
        raise AssertionError("spark started")

    monkeypatch.setattr("lakeforge.lakehouse.stream.build_session", _refuse_spark)
    settings = Settings(
        _env_file=None,
        landing_dir=landing.parent,
        contracts_dir=tmp_path / "contracts",
        kafka_bootstrap="localhost:19092",
    )

    with pytest.raises(ValueError, match="schema violations"):
        run_publish(settings, landing)

    report = tmp_path / "contracts" / "run_id=drift" / "schema_violation.jsonl"
    assert "unknown_field" in report.read_text(encoding="utf-8")


def test_bootstrap_and_stream_dir_reject_a_path_escape(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="host and port"):
        resolve_bootstrap("../broker")
    with pytest.raises(ValueError, match="stream directory"):
        resolve_stream_dir(tmp_path / ".." / "escaped")


def test_cli_rejects_a_stream_path_and_bootstrap_outside_the_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream_dir = tmp_path / "stream"
    stream_dir.mkdir()
    monkeypatch.setenv("LAKEFORGE_STREAM_DIR", str(stream_dir))
    outside = tmp_path / "other" / ".." / "escaped"

    assert main(["stream", "--stream-dir", str(outside)]) == 1
    assert main(["stream", "--kafka-bootstrap", "../broker"]) == 1


def _telemetry(event_id: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "vehicle_id": "VEH-0001",
        "event_time": "2026-01-12T06:00:00+00:00",
        "received_at": "2026-01-12T06:00:00+00:00",
        "latitude": 45.0,
        "longitude": -90.0,
        "speed_kph": 0,
        "engine_state": "idle",
        "fuel_rate_lph": 3.5,
        "engine_hours": 249.25,
    }


def _landing(root: Path, run_id: str, *, extra: dict[str, object] | None) -> Path:
    run_dir = root / f"run_id={run_id}"
    run_dir.mkdir(parents=True)
    for name in DATASETS:
        (run_dir / f"{name}.jsonl").write_text("", encoding="utf-8")
    (run_dir / "sites.jsonl").write_text(
        json.dumps(
            {
                "site_id": "SITE-ALPHA",
                "site_name": "Site Alpha",
                "latitude": 45.0,
                "longitude": -90.0,
                "timezone_name": "UTC",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "vehicles.jsonl").write_text(
        json.dumps(
            {
                "vehicle_id": "VEH-0001",
                "asset_type": "haul_truck",
                "model_code": "HT-240",
                "site_id": "SITE-ALPHA",
                "status": "active",
                "commissioned_on": "2020-01-15",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    row = _telemetry("EVT-1")
    if extra is not None:
        row.update(extra)
    (run_dir / "telemetry_events.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    counts = {name: 0 for name in DATASETS}
    counts["sites"] = 1
    counts["vehicles"] = 1
    counts["telemetry_events"] = 1
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "datasets": {name: {"rows": count} for name, count in counts.items()},
            }
        ),
        encoding="utf-8",
    )
    return run_dir
