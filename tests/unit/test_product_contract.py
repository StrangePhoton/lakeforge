import json
from pathlib import Path

import pytest

from lakeforge.cli import main
from lakeforge.config import Settings
from lakeforge.lakehouse.catalog import DATASETS, LANDING_RUN_COLUMN
from lakeforge.lakehouse.product import (
    ALARM_COLUMNS,
    FLEET_COLUMNS,
    SITE_COLUMNS,
    load_descriptors,
    run_product,
)
from lakeforge.lakehouse.quality import REFERENCE_RULES, TELEMETRY_RULES


def test_descriptors_match_the_product_builders() -> None:
    loaded = load_descriptors()

    fleet = loaded["fleet.vehicle_activity"]
    operations = loaded["operations.site_activity"]
    maintenance = loaded["maintenance.alarm_events"]
    assert fleet["columns"] == list(FLEET_COLUMNS)
    assert operations["columns"] == list(SITE_COLUMNS)
    assert maintenance["columns"] == list(ALARM_COLUMNS)
    assert fleet["quality_requirements"] == list(TELEMETRY_RULES)
    assert operations["quality_requirements"] == list(TELEMETRY_RULES)
    assert maintenance["quality_requirements"] == list(REFERENCE_RULES)
    assert fleet["owner"] == "fleet"
    assert operations["owner"] == "operations"
    assert maintenance["owner"] == "maintenance"
    assert fleet["version"] == 1
    assert LANDING_RUN_COLUMN in FLEET_COLUMNS


def test_reference_failures_block_promotion_before_spark(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    silver = _run_dir(tmp_path / "silver", "blocked", DATASETS)
    quality = _run_dir(tmp_path / "quality", "blocked", ("telemetry_passed",))
    _quality_manifest(quality, reference_count=1)

    def _refuse_spark(**_kwargs: object) -> None:
        raise AssertionError("spark started")

    monkeypatch.setattr("lakeforge.lakehouse.product.build_session", _refuse_spark)
    settings = Settings(
        _env_file=None,
        silver_dir=silver.parent,
        quality_dir=quality.parent,
        products_dir=tmp_path / "products",
    )

    with pytest.raises(ValueError, match="reference violations"):
        run_product(settings, silver, quality)

    assert not (tmp_path / "products" / "run_id=blocked").exists()


def test_cli_rejects_an_escaped_product_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    silver = tmp_path / "silver"
    quality = tmp_path / "quality"
    products = tmp_path / "products"
    silver.mkdir()
    quality.mkdir()
    products.mkdir()
    monkeypatch.setenv("LAKEFORGE_SILVER_DIR", str(silver))
    monkeypatch.setenv("LAKEFORGE_QUALITY_DIR", str(quality))
    monkeypatch.setenv("LAKEFORGE_PRODUCTS_DIR", str(products))
    outside = tmp_path / "other" / ".." / "escaped"
    silver_run = silver / "run_id=abc"
    quality_run = quality / "run_id=abc"

    assert main(["product", "--silver-run", str(outside), "--quality-run", str(quality_run)]) == 1
    assert main(["product", "--silver-run", str(silver_run), "--quality-run", str(outside)]) == 1
    assert (
        main(
            [
                "product",
                "--silver-run",
                str(silver_run),
                "--quality-run",
                str(quality_run),
                "--products-dir",
                str(tmp_path / ".." / "escaped"),
            ]
        )
        == 1
    )


def _run_dir(root: Path, run_id: str, tables: tuple[str, ...]) -> Path:
    run_dir = root / f"run_id={run_id}"
    run_dir.mkdir(parents=True)
    for name in tables:
        (run_dir / name / "_delta_log").mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps({"landing_run_id": run_id, "datasets": {}}),
        encoding="utf-8",
    )
    return run_dir


def _quality_manifest(run_dir: Path, *, reference_count: int) -> None:
    rules = {name: 0 for name in TELEMETRY_RULES}
    rules.update({name: 0 for name in REFERENCE_RULES})
    rules["alarm_vehicle_known"] = reference_count
    payload = {
        "landing_run_id": run_dir.name.removeprefix("run_id="),
        "datasets": {"telemetry_passed": {"rows": 0}},
        "rules": rules,
    }
    (run_dir / "manifest.json").write_text(json.dumps(payload), encoding="utf-8")
