import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from lakeforge.config import Settings
from lakeforge.lakehouse.bronze import run_bronze
from lakeforge.lakehouse.catalog import DATASETS
from lakeforge.lakehouse.session import build_session
from lakeforge.lakehouse.silver import run_silver

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_session(warehouse_dir=warehouse)
    yield session
    session.stop()


def test_optional_payload_round_trips_and_stays_null_when_omitted(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    landing_root = tmp_path / "landing"
    run_dir = landing_root / "run_id=payload"
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
    present = _telemetry("EVT-PRESENT")
    present["payload_kg"] = 12.5
    omitted = _telemetry("EVT-OMITTED")
    (run_dir / "telemetry_events.jsonl").write_text(
        json.dumps(present) + "\n" + json.dumps(omitted) + "\n",
        encoding="utf-8",
    )
    counts = {name: 0 for name in DATASETS}
    counts["sites"] = 1
    counts["vehicles"] = 1
    counts["telemetry_events"] = 2
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "payload",
                "datasets": {name: {"rows": count} for name, count in counts.items()},
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        _env_file=None,
        landing_dir=landing_root,
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        contracts_dir=tmp_path / "contracts",
    )

    bronze = run_bronze(settings, run_dir, spark=spark_session)
    silver = run_silver(settings, bronze.bronze_dir, spark=spark_session)
    bronze_rows = {
        row["event_id"]: row["payload_kg"]
        for row in spark_session.read.format("delta")
        .load((bronze.bronze_dir / "telemetry_events").as_posix())
        .collect()
    }
    silver_rows = {
        row["event_id"]: row["payload_kg"]
        for row in spark_session.read.format("delta")
        .load((silver.silver_dir / "telemetry_events").as_posix())
        .collect()
    }

    assert bronze_rows["EVT-PRESENT"] == pytest.approx(12.5)
    assert bronze_rows["EVT-OMITTED"] is None
    assert silver_rows == bronze_rows


def _telemetry(event_id: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "vehicle_id": "VEH-0001",
        "event_time": "2026-01-12T06:00:00+00:00",
        "received_at": "2026-01-12T06:00:00+00:00",
        "latitude": 45.0,
        "longitude": -90.0,
        "speed_kph": 0.0,
        "engine_state": "idle",
        "fuel_rate_lph": 3.5,
        "engine_hours": 249.25,
    }
