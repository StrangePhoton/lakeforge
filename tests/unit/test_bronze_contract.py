from pathlib import Path

import pytest

from lakeforge.cli import main
from lakeforge.lakehouse.catalog import LANDING_RUN_COLUMN, TABLES
from lakeforge.lakehouse.paths import resolve_landing_run
from lakeforge.lakehouse.schema import bronze_schema


def test_bronze_schema_keeps_source_nullability() -> None:
    telemetry = {field.name: field.nullable for field in bronze_schema("telemetry_events").fields}

    assert telemetry["event_time"] is True
    assert telemetry["vehicle_id"] is True
    assert telemetry["received_at"] is False
    assert telemetry[LANDING_RUN_COLUMN] is False
    assert "is_anomaly" not in telemetry
    assert [field.name for field in bronze_schema("sites").fields] == [
        *[spec.name for spec in TABLES["sites"]],
        LANDING_RUN_COLUMN,
    ]


def test_landing_run_must_stay_inside_the_landing_directory(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)

    with pytest.raises(ValueError, match="landing directory"):
        resolve_landing_run(landing, outside)
    with pytest.raises(ValueError, match="landing directory"):
        resolve_landing_run(landing, landing / ".." / "other" / "run_id=abc")


def test_landing_run_requires_the_operational_files(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    run_dir = landing / "run_id=round-trip"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="sites.jsonl"):
        resolve_landing_run(landing, run_dir)

    for name in TABLES:
        (run_dir / f"{name}.jsonl").write_text("", encoding="utf-8")

    assert resolve_landing_run(landing, Path("run_id=round-trip")) == run_dir.resolve()


def test_cli_rejects_a_bronze_run_outside_the_landing_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)
    monkeypatch.setenv("LAKEFORGE_LANDING_DIR", str(landing))

    assert main(["bronze", "--landing-run", str(outside)]) == 1
