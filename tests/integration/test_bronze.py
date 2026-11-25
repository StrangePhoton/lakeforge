import json
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.lakehouse.bronze import run_bronze
from lakeforge.lakehouse.catalog import DATASETS, LANDING_RUN_COLUMN
from lakeforge.lakehouse.session import build_session
from lakeforge.synthetic.generator import generate_dataset

pytestmark = pytest.mark.spark

_STARTED = datetime(2026, 1, 12, 12, 0, tzinfo=UTC)
_FINISHED = datetime(2026, 1, 12, 12, 0, 2, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_session(warehouse_dir=warehouse)
    yield session
    session.stop()


def test_tiny_landing_round_trips_into_delta(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    landing_root = tmp_path / "landing"
    bronze_root = tmp_path / "bronze"
    run_dir = write_landing_run(
        landing_dir=landing_root,
        run_id="bronze-tiny",
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
        bronze_dir=bronze_root,
    )

    first = run_bronze(settings, run_dir, spark=spark_session)
    before = _sorted_rows(
        spark_session.read.format("delta").load((first.bronze_dir / "telemetry_events").as_posix())
    )
    second = run_bronze(settings, run_dir, spark=spark_session)
    after = _sorted_rows(
        spark_session.read.format("delta").load((second.bronze_dir / "telemetry_events").as_posix())
    )

    assert first.row_counts == second.row_counts
    assert before == after
    assert first.row_counts == (
        ("sites", 2),
        ("vehicles", 4),
        ("telemetry_events", 97),
        ("alarms", 5),
        ("work_orders", 1),
    )
    bronze_dir = first.bronze_dir
    assert not (bronze_dir / "anomalies.json").exists()
    manifest = json.loads((bronze_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["landing_run_id"] == "bronze-tiny"
    assert manifest["format"] == "delta"
    assert manifest["status"] == "succeeded"
    for dataset_name in DATASETS:
        table = bronze_dir / dataset_name
        assert (table / "_delta_log").is_dir()
        frame = spark_session.read.format("delta").load(table.as_posix())
        assert LANDING_RUN_COLUMN in frame.columns
        assert "is_anomaly" not in frame.columns
        assert (
            frame.filter(frame[LANDING_RUN_COLUMN] == "bronze-tiny").count()
            == dict(first.row_counts)[dataset_name]
        )

    telemetry = spark_session.read.format("delta").load(
        (bronze_dir / "telemetry_events").as_posix()
    )
    by_id = {row["event_id"]: row for row in telemetry.collect()}
    assert by_id["EVT-0004-000005"]["event_time"] is None
    assert by_id["EVT-0004-000005"]["received_at"] is not None
    assert by_id["EVT-0001-000006"]["vehicle_id"] is None
    assert by_id["EVT-0002-000007"]["vehicle_id"] == "VEH-9999"
    assert _as_utc(by_id["EVT-0001-000001"]["event_time"]) == datetime(
        2026, 1, 12, 6, 0, tzinfo=UTC
    )
    assert by_id["EVT-0003-000009"]["latitude"] == pytest.approx(95.0)

    vehicles = spark_session.read.format("delta").load((bronze_dir / "vehicles").as_posix())
    commissioned = {row["vehicle_id"]: row["commissioned_on"] for row in vehicles.collect()}
    assert commissioned["VEH-0001"] == date(2020, 1, 15)


def test_unparseable_timestamp_fails_the_load(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    landing_root = tmp_path / "landing"
    run_dir = landing_root / "run_id=bad-time"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "bad-time",
                "datasets": {
                    name: {"rows": 1 if name in {"sites", "telemetry_events"} else 0}
                    for name in DATASETS
                },
            }
        ),
        encoding="utf-8",
    )
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
    (run_dir / "telemetry_events.jsonl").write_text(
        json.dumps(
            {
                "event_id": "EVT-0001-000001",
                "vehicle_id": "VEH-0001",
                "event_time": "not-a-timestamp",
                "received_at": "2026-01-12T06:00:00+00:00",
                "latitude": 45.0,
                "longitude": -90.0,
                "speed_kph": 0.0,
                "engine_state": "idle",
                "fuel_rate_lph": 3.5,
                "engine_hours": 249.25,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    settings = Settings(_env_file=None, landing_dir=landing_root, bronze_dir=tmp_path / "bronze")

    with pytest.raises(ValueError, match="telemetry_events"):
        run_bronze(settings, run_dir, spark=spark_session)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        # PySpark returns a naive datetime in the operating-system zone.
        return value.astimezone().astimezone(UTC)
    return value.astimezone(UTC)


def _sorted_rows(frame: DataFrame) -> list[tuple[object, ...]]:
    return sorted((tuple(row) for row in frame.collect()), key=str)
