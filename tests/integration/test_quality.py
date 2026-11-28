import json
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.lakehouse.bronze import run_bronze
from lakeforge.lakehouse.catalog import DATASETS
from lakeforge.lakehouse.quality import REFERENCE_RULES, RULES, run_quality
from lakeforge.lakehouse.schema import bronze_schema, silver_telemetry_schema
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


def test_tiny_silver_quarantines_the_known_defects(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    landing_root = tmp_path / "landing"
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    quality_root = tmp_path / "quality"
    run_dir = write_landing_run(
        landing_dir=landing_root,
        run_id="quality-tiny",
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
        quality_dir=quality_root,
    )
    bronze = run_bronze(settings, run_dir, spark=spark_session)
    silver = run_silver(settings, bronze.bronze_dir, spark=spark_session)

    first = run_quality(settings, silver.silver_dir, spark=spark_session)
    before = _rules_by_event(
        spark_session.read.format("delta").load(
            (first.quality_dir / "telemetry_violation").as_posix()
        )
    )
    second = run_quality(settings, silver.silver_dir, spark=spark_session)
    after = _rules_by_event(
        spark_session.read.format("delta").load(
            (second.quality_dir / "telemetry_violation").as_posix()
        )
    )

    assert first.passed_rows == second.passed_rows == 84
    assert first.violation_rows == second.violation_rows == 13
    assert before == after
    assert set(before) == {
        "EVT-DUP-0001",
        "EVT-0002-000003",
        "EVT-0003-000004",
        "EVT-0004-000005",
        "EVT-0001-000006",
        "EVT-0002-000007",
        "EVT-0001-000008",
        "EVT-0003-000009",
        "EVT-0004-000010",
        "EVT-0002-000011",
        "EVT-0003-000012",
        "EVT-0001-000013",
        "EVT-0001-000014",
    }
    assert before["EVT-0002-000003"] == {"position_present"}
    assert before["EVT-0003-000004"] == {"speed_present"}
    assert before["EVT-DUP-0001"] == {"duplicate_reading"}
    assert before["EVT-0004-000005"] == {"event_time_present"}
    assert before["EVT-0001-000006"] == {"vehicle_id_present"}
    assert before["EVT-0002-000007"] == {"vehicle_known"}
    assert before["EVT-0001-000008"] == {"speed_range"}
    assert before["EVT-0003-000009"] == {"position_range"}
    assert before["EVT-0004-000010"] == {"position_range"}
    assert before["EVT-0002-000011"] == {"engine_state_known"}
    assert before["EVT-0003-000012"] == {"event_time_late"}
    assert before["EVT-0001-000013"] == {"received_at_out_of_order"}
    assert before["EVT-0001-000014"] == {"received_at_out_of_order"}
    assert "EVT-0001-000010" not in before
    catalog_ids = {record.record_id for record in dataset.anomalies}
    assert catalog_ids <= set(before)
    counts = dict(first.rule_counts)
    assert list(counts) == list(RULES)
    assert all(counts[rule] == 0 for rule in REFERENCE_RULES)
    assert counts["vehicle_known"] == 1
    assert counts["engine_state_known"] == 1
    assert not (first.quality_dir / "reference_violation").exists()
    assert not (first.quality_dir / "anomalies.json").exists()
    passed = spark_session.read.format("delta").load(
        (first.quality_dir / "telemetry_passed").as_posix()
    )
    assert "is_anomaly" not in passed.columns
    assert "rule_id" not in passed.columns
    passed_ids = {row["event_id"] for row in passed.select("event_id").collect()}
    assert "VEH-9999" not in {row["vehicle_id"] for row in passed.collect()}
    assert "EVT-0004-000005" not in passed_ids
    assert "EVT-0001-000006" not in passed_ids
    silver_telemetry = spark_session.read.format("delta").load(
        (silver.silver_dir / "telemetry_events").as_posix()
    )
    assert silver_telemetry.count() == 97


def test_unknown_alarm_vehicle_is_recorded(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    silver_dir = _write_silver(spark_session, tmp_path, "bad-alarm")
    settings = Settings(
        _env_file=None,
        silver_dir=tmp_path / "silver",
        quality_dir=tmp_path / "quality",
    )

    result = run_quality(settings, silver_dir, spark=spark_session)

    assert result.passed_rows == 1
    assert result.violation_rows == 0
    assert dict(result.rule_counts)["alarm_vehicle_known"] == 1
    reference = spark_session.read.format("delta").load(
        (result.quality_dir / "reference_violation").as_posix()
    )
    rows = reference.collect()
    assert len(rows) == 1
    assert rows[0]["rule_id"] == "alarm_vehicle_known"
    assert rows[0]["record_id"] == "ALM-000001"
    assert rows[0]["source_table"] == "alarms"


def _write_silver(spark: SparkSession, root: Path, run_id: str) -> Path:
    silver_dir = root / "silver" / f"run_id={run_id}"
    rows: dict[str, list[tuple[object, ...]]] = {
        "sites": [("SITE-ALPHA", "Site Alpha", 45.0, -90.0, "UTC", run_id)],
        "vehicles": [
            ("VEH-0001", "haul_truck", "HT-240", "SITE-ALPHA", "active", date(2020, 1, 15), run_id)
        ],
        "telemetry_events": [
            (
                "EVT-0001-000001",
                "VEH-0001",
                "SITE-ALPHA",
                _RECEIVED,
                _RECEIVED,
                45.0,
                -90.0,
                0.0,
                "idle",
                3.5,
                249.25,
                None,
                run_id,
            )
        ],
        "alarms": [
            ("ALM-000001", "VEH-MISSING", None, "IDLE_EXCESS", "warning", _RECEIVED, run_id)
        ],
        "work_orders": [],
    }
    for dataset in DATASETS:
        schema = (
            silver_telemetry_schema() if dataset == "telemetry_events" else bronze_schema(dataset)
        )
        frame = spark.createDataFrame(rows[dataset], schema)
        frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            (silver_dir / dataset).as_posix()
        )
    payload = {
        "landing_run_id": run_id,
        "format": "delta",
        "datasets": {name: {"rows": len(rows[name])} for name in DATASETS},
        "status": "succeeded",
    }
    (silver_dir / "manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    return silver_dir


def _rules_by_event(frame: DataFrame) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for row in frame.select("event_id", "rule_id").collect():
        found.setdefault(str(row["event_id"]), set()).add(str(row["rule_id"]))
    return found
