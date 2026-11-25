import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.lakehouse.bronze import run_bronze
from lakeforge.lakehouse.catalog import DATASETS, GOLD_TABLES, LANDING_RUN_COLUMN
from lakeforge.lakehouse.gold import run_gold
from lakeforge.lakehouse.schema import bronze_schema
from lakeforge.lakehouse.session import build_session
from lakeforge.lakehouse.silver import run_silver
from lakeforge.synthetic.generator import generate_dataset

pytestmark = pytest.mark.spark

_STARTED = datetime(2026, 1, 12, 12, 0, tzinfo=UTC)
_FINISHED = datetime(2026, 1, 12, 12, 0, 2, tzinfo=UTC)
_RECEIVED = datetime(2026, 1, 12, 6, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_session(warehouse_dir=warehouse)
    yield session
    session.stop()


def test_tiny_bronze_conforms_and_aggregates(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    landing_root = tmp_path / "landing"
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    run_dir = write_landing_run(
        landing_dir=landing_root,
        run_id="medallion-tiny",
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
        silver_dir=silver_root,
        gold_dir=gold_root,
    )
    bronze = run_bronze(settings, run_dir, spark=spark_session)

    first_silver = run_silver(settings, bronze.bronze_dir, spark=spark_session)
    silver_before = _sorted_rows(
        spark_session.read.format("delta").load(
            (first_silver.silver_dir / "telemetry_events").as_posix()
        )
    )
    second_silver = run_silver(settings, bronze.bronze_dir, spark=spark_session)
    silver_after = _sorted_rows(
        spark_session.read.format("delta").load(
            (second_silver.silver_dir / "telemetry_events").as_posix()
        )
    )

    assert (
        first_silver.row_counts
        == second_silver.row_counts
        == (
            ("sites", 2),
            ("vehicles", 4),
            ("telemetry_events", 97),
            ("alarms", 5),
            ("work_orders", 1),
        )
    )
    assert silver_before == silver_after
    silver_dir = first_silver.silver_dir
    assert not (silver_dir / "anomalies.json").exists()
    telemetry = spark_session.read.format("delta").load(
        (silver_dir / "telemetry_events").as_posix()
    )
    assert "is_anomaly" not in telemetry.columns
    assert LANDING_RUN_COLUMN in telemetry.columns
    by_id = {row["event_id"]: row for row in telemetry.collect()}
    assert by_id["EVT-0004-000005"]["event_time"] is None
    assert by_id["EVT-0001-000006"]["vehicle_id"] is None
    assert by_id["EVT-0001-000006"]["site_id"] is None
    assert by_id["EVT-0002-000007"]["vehicle_id"] == "VEH-9999"
    assert by_id["EVT-0002-000007"]["site_id"] is None
    assert by_id["EVT-0001-000001"]["site_id"] == "SITE-ALPHA"
    assert by_id["EVT-DUP-0001"]["site_id"] == "SITE-ALPHA"
    assert by_id["EVT-0001-000008"]["speed_kph"] == pytest.approx(240.0)
    assert by_id["EVT-0003-000009"]["latitude"] == pytest.approx(95.0)
    assert by_id["EVT-0004-000010"]["longitude"] == pytest.approx(200.0)
    assert by_id["EVT-0002-000011"]["engine_state"] == "unknown"

    first_gold = run_gold(settings, silver_dir, spark=spark_session)
    gold_before = _sorted_rows(
        spark_session.read.format("delta").load(
            (first_gold.gold_dir / "vehicle_activity").as_posix()
        )
    )
    second_gold = run_gold(settings, silver_dir, spark=spark_session)
    gold_after = _sorted_rows(
        spark_session.read.format("delta").load(
            (second_gold.gold_dir / "vehicle_activity").as_posix()
        )
    )

    assert (
        first_gold.row_counts
        == second_gold.row_counts
        == (
            ("vehicle_activity", 4),
            ("site_activity", 2),
        )
    )
    assert gold_before == gold_after
    assert not (first_gold.gold_dir / "anomalies.json").exists()
    for name in GOLD_TABLES:
        assert (first_gold.gold_dir / name / "_delta_log").is_dir()
    vehicles = {
        row["vehicle_id"]: row
        for row in spark_session.read.format("delta")
        .load((first_gold.gold_dir / "vehicle_activity").as_posix())
        .collect()
    }
    assert _activity(vehicles["VEH-0001"]) == ("SITE-ALPHA", 24, 24, 2, 1)
    assert _activity(vehicles["VEH-0002"]) == ("SITE-BRAVO", 23, 23, 1, 0)
    assert _activity(vehicles["VEH-0003"]) == ("SITE-ALPHA", 24, 24, 1, 0)
    assert _activity(vehicles["VEH-0004"]) == ("SITE-BRAVO", 24, 23, 1, 0)
    assert _as_utc(vehicles["VEH-0003"]["first_event_time"]) == datetime(
        2026, 1, 11, 20, 45, tzinfo=UTC
    )
    assert _as_utc(vehicles["VEH-0001"]["last_event_time"]) == datetime(
        2026, 1, 12, 11, 45, tzinfo=UTC
    )
    sites = {
        row["site_id"]: row
        for row in spark_session.read.format("delta")
        .load((first_gold.gold_dir / "site_activity").as_posix())
        .collect()
    }
    assert _site(sites["SITE-ALPHA"]) == ("Site Alpha", 2, 48, 3, 1)
    assert _site(sites["SITE-BRAVO"]) == ("Site Bravo", 2, 47, 2, 0)


def test_identical_business_keys_collapse(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_dir = _write_bronze(spark_session, tmp_path, "identical-run", speed_kph=(1.0, 1.0))
    settings = Settings(
        _env_file=None,
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
    )

    result = run_silver(settings, bronze_dir, spark=spark_session)

    assert dict(result.row_counts)["telemetry_events"] == 1


def test_conflicting_business_key_fails_the_load(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_dir = _write_bronze(spark_session, tmp_path, "conflict-run", speed_kph=(1.0, 2.0))
    settings = Settings(
        _env_file=None,
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
    )

    with pytest.raises(ValueError, match="telemetry_events.event_id"):
        run_silver(settings, bronze_dir, spark=spark_session)


def _write_bronze(
    spark: SparkSession,
    root: Path,
    run_id: str,
    *,
    speed_kph: tuple[float, float],
) -> Path:
    bronze_dir = root / "bronze" / f"run_id={run_id}"
    for dataset in DATASETS:
        target = bronze_dir / dataset
        target.mkdir(parents=True, exist_ok=True)
        rows: list[tuple[object, ...]] = []
        if dataset == "telemetry_events":
            rows = [_telemetry_row(run_id, speed) for speed in speed_kph]
        frame = spark.createDataFrame(rows, bronze_schema(dataset))
        frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            target.as_posix()
        )
    counts = {name: 2 if name == "telemetry_events" else 0 for name in DATASETS}
    payload = {
        "landing_run_id": run_id,
        "format": "delta",
        "datasets": {name: {"rows": count} for name, count in counts.items()},
        "status": "succeeded",
    }
    (bronze_dir / "manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    return bronze_dir


def _telemetry_row(run_id: str, speed_kph: float) -> tuple[object, ...]:
    return (
        "EVT-1",
        "VEH-1",
        None,
        _RECEIVED,
        None,
        None,
        speed_kph,
        None,
        None,
        None,
        run_id,
    )


def _activity(row: Row) -> tuple[object, ...]:
    record = row.asDict()
    return (
        record["site_id"],
        record["event_count"],
        record["timed_event_count"],
        record["alarm_count"],
        record["work_order_count"],
    )


def _site(row: Row) -> tuple[object, ...]:
    record = row.asDict()
    return (
        record["site_name"],
        record["vehicle_count"],
        record["event_count"],
        record["alarm_count"],
        record["work_order_count"],
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.astimezone().astimezone(UTC)
    return value.astimezone(UTC)


def _sorted_rows(frame: DataFrame) -> list[tuple[object, ...]]:
    return sorted((tuple(row) for row in frame.collect()), key=str)
