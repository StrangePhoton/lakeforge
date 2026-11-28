import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakeforge.cli import main
from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.lakehouse.bronze import run_bronze
from lakeforge.lakehouse.catalog import DATASETS, TABLES
from lakeforge.lakehouse.contract import check_dataset, load_catalog, run_contract
from lakeforge.lakehouse.paths import resolve_landing_run
from lakeforge.synthetic.generator import generate_dataset

_STARTED = datetime(2026, 1, 12, 12, 0, tzinfo=UTC)
_FINISHED = datetime(2026, 1, 12, 12, 0, 2, tzinfo=UTC)


def test_published_contracts_match_the_bronze_columns() -> None:
    catalog = load_catalog()

    for dataset, specs in TABLES.items():
        latest = catalog[dataset].latest()
        assert [(field.name, field.kind, field.required) for field in latest.fields] == [
            (spec.name, spec.kind, not spec.nullable) for spec in specs
        ]
    version_one = catalog["telemetry_events"].at(1)
    assert [field.name for field in version_one.fields] == [
        spec.name for spec in TABLES["telemetry_events"] if spec.name != "payload_kg"
    ]
    assert catalog["telemetry_events"].latest().version == 2


def test_telemetry_contract_versions_accept_backward_compatible_payload() -> None:
    telemetry = load_catalog()["telemetry_events"]
    current = _telemetry_line()
    evolved = _telemetry_line(payload_kg=12.5)

    assert check_dataset("telemetry_events", current, telemetry.at(2))[1] == ()
    assert check_dataset("telemetry_events", evolved, telemetry.at(2))[1] == ()
    version_one = check_dataset("telemetry_events", evolved, telemetry.at(1))[1]
    assert [(item.rule_id, item.field) for item in version_one] == [("unknown_field", "payload_kg")]

    heavy = _telemetry_line(payload_kg="heavy")
    mismatched = check_dataset("telemetry_events", heavy, telemetry.at(2))[1]
    assert [(item.rule_id, item.field) for item in mismatched] == [("type_mismatch", "payload_kg")]

    missing = json.loads(current)
    del missing["received_at"]
    missing_text = json.dumps(missing) + "\n"
    missing_field = check_dataset("telemetry_events", missing_text, telemetry.at(2))[1]
    assert [(item.rule_id, item.field) for item in missing_field] == [
        ("missing_field", "received_at")
    ]

    unknown = json.loads(current)
    unknown["note"] = "later"
    unknown_text = json.dumps(unknown) + "\n"
    unknown_field = check_dataset("telemetry_events", unknown_text, telemetry.at(2))[1]
    assert [(item.rule_id, item.field) for item in unknown_field] == [("unknown_field", "note")]


def test_tiny_landing_passes_the_latest_contracts(tmp_path: Path) -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    landing_root = tmp_path / "landing"
    run_dir = write_landing_run(
        landing_dir=landing_root,
        run_id="contract-tiny",
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
    settings = Settings(
        _env_file=None,
        landing_dir=landing_root,
        contracts_dir=tmp_path / "contracts",
    )

    result = run_contract(settings, run_dir)

    assert result.violation_counts == tuple((name, 0) for name in DATASETS)
    assert dict(result.contract_versions)["telemetry_events"] == 2
    assert not (result.report_dir / "schema_violation.jsonl").exists()
    assert dict(result.row_counts)["telemetry_events"] == 97


def test_bronze_rejects_an_unknown_field_before_spark(tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    run_dir = _landing(landing_root, "drift", extra={"note": "later"})
    settings = Settings(
        _env_file=None,
        landing_dir=landing_root,
        bronze_dir=tmp_path / "bronze",
        contracts_dir=tmp_path / "contracts",
    )

    with pytest.raises(ValueError, match="schema violations"):
        run_bronze(settings, run_dir)

    report = tmp_path / "contracts" / "run_id=drift" / "schema_violation.jsonl"
    assert "unknown_field" in report.read_text(encoding="utf-8")
    assert not (tmp_path / "bronze" / "run_id=drift" / "telemetry_events").exists()


def test_a_clean_rerun_removes_a_stale_violation_file(tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    run_dir = _landing(landing_root, "rerun", extra={"note": "later"})
    settings = Settings(
        _env_file=None,
        landing_dir=landing_root,
        contracts_dir=tmp_path / "contracts",
    )
    first = run_contract(settings, run_dir)
    assert (first.report_dir / "schema_violation.jsonl").is_file()

    _write_telemetry(run_dir, extra=None)
    second = run_contract(settings, run_dir)

    assert dict(second.violation_counts)["telemetry_events"] == 0
    assert not (second.report_dir / "schema_violation.jsonl").exists()


def test_cli_rejects_a_contract_run_outside_the_landing_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)
    monkeypatch.setenv("LAKEFORGE_LANDING_DIR", str(landing))

    with pytest.raises(ValueError, match="landing directory"):
        resolve_landing_run(landing, outside)
    assert main(["contract", "--landing-run", str(outside)]) == 1


def _telemetry_line(**overrides: object) -> str:
    row: dict[str, object] = {
        "event_id": "EVT-1",
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
    row.update(overrides)
    return json.dumps(row) + "\n"


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
    _write_telemetry(run_dir, extra=extra)
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


def _write_telemetry(run_dir: Path, *, extra: dict[str, object] | None) -> None:
    row = json.loads(_telemetry_line())
    if extra is not None:
        row.update(extra)
    (run_dir / "telemetry_events.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
