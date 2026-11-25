from pathlib import Path

import pytest

from lakeforge.cli import main
from lakeforge.lakehouse.catalog import DATASETS, LANDING_RUN_COLUMN
from lakeforge.lakehouse.paths import resolve_delta_run
from lakeforge.lakehouse.schema import silver_telemetry_schema


def test_silver_telemetry_schema_adds_a_nullable_site_id() -> None:
    fields = {field.name: field.nullable for field in silver_telemetry_schema().fields}

    assert fields["site_id"] is True
    assert fields["event_id"] is False
    assert fields["vehicle_id"] is True
    assert fields[LANDING_RUN_COLUMN] is False
    assert "is_anomaly" not in fields
    names = [field.name for field in silver_telemetry_schema().fields]
    assert names.index("site_id") == names.index("vehicle_id") + 1


def test_delta_run_must_stay_inside_its_directory(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)

    with pytest.raises(ValueError, match="bronze directory"):
        resolve_delta_run(bronze, outside, DATASETS, layer="bronze")
    with pytest.raises(ValueError, match="bronze directory"):
        resolve_delta_run(bronze, bronze / ".." / "other" / "run_id=abc", DATASETS, layer="bronze")


def test_delta_run_requires_delta_tables(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    run_dir = bronze / "run_id=round-trip"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="sites"):
        resolve_delta_run(bronze, run_dir, DATASETS, layer="bronze")

    for name in DATASETS:
        (run_dir / name / "_delta_log").mkdir(parents=True)

    assert resolve_delta_run(bronze, Path("run_id=round-trip"), DATASETS, layer="bronze") == (
        run_dir.resolve()
    )


def test_cli_rejects_a_silver_run_outside_the_bronze_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)
    monkeypatch.setenv("LAKEFORGE_BRONZE_DIR", str(bronze))

    assert main(["silver", "--bronze-run", str(outside)]) == 1


def test_cli_rejects_a_gold_run_outside_the_silver_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    silver = tmp_path / "silver"
    silver.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)
    monkeypatch.setenv("LAKEFORGE_SILVER_DIR", str(silver))

    assert main(["gold", "--silver-run", str(outside)]) == 1
