from pathlib import Path

import pytest

from lakeforge.cli import main
from lakeforge.lakehouse.catalog import DATASETS
from lakeforge.lakehouse.paths import resolve_delta_run
from lakeforge.lakehouse.quality import REFERENCE_RULES, RULES, TELEMETRY_RULES


def test_quality_rules_are_the_fleet_checks() -> None:
    assert TELEMETRY_RULES == (
        "vehicle_id_present",
        "vehicle_known",
        "event_time_present",
        "position_present",
        "position_range",
        "speed_present",
        "speed_range",
        "engine_state_known",
        "event_time_late",
        "duplicate_reading",
        "received_at_out_of_order",
    )
    assert REFERENCE_RULES == (
        "alarm_vehicle_known",
        "work_order_vehicle_known",
        "work_order_site_known",
        "work_order_alarm_known",
    )
    assert RULES == TELEMETRY_RULES + REFERENCE_RULES


def test_quality_run_must_stay_inside_the_silver_directory(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    silver.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)

    with pytest.raises(ValueError, match="silver directory"):
        resolve_delta_run(silver, outside, DATASETS, layer="silver")


def test_cli_rejects_a_quality_run_outside_the_silver_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    silver = tmp_path / "silver"
    silver.mkdir()
    outside = tmp_path / "other" / "run_id=abc"
    outside.mkdir(parents=True)
    monkeypatch.setenv("LAKEFORGE_SILVER_DIR", str(silver))

    assert main(["quality", "--silver-run", str(outside)]) == 1
