import json
import socket
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.lakehouse.session import build_session
from lakeforge.lakehouse.stream import TELEMETRY_TOPIC, run_publish, run_stream
from lakeforge.synthetic.generator import generate_dataset

pytestmark = [pytest.mark.spark, pytest.mark.kafka]

_STARTED = datetime(2026, 1, 12, 12, 0, tzinfo=UTC)
_FINISHED = datetime(2026, 1, 12, 12, 0, 2, tzinfo=UTC)
_BOOTSTRAP = "localhost:19092"


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_session(warehouse_dir=warehouse)
    yield session
    session.stop()


def test_tiny_telemetry_streams_once_per_event_id(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    if not _broker_open():
        pytest.skip("Kafka is unavailable. Start it with `docker compose up -d --wait`.")
    _reset_topic(spark_session)
    landing_root = tmp_path / "landing"
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    run_dir = write_landing_run(
        landing_dir=landing_root,
        run_id="stream-tiny",
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
        stream_dir=tmp_path / "stream",
        kafka_bootstrap=_BOOTSTRAP,
    )

    published = run_publish(settings, run_dir, spark=spark_session)
    first = run_stream(settings, spark=spark_session)
    second = run_stream(settings, spark=spark_session)
    run_publish(settings, run_dir, spark=spark_session)
    replayed = run_stream(settings, spark=spark_session)
    _publish_bad_record(spark_session)
    with_violation = run_stream(settings, spark=spark_session)
    rows = {
        row["event_id"]: row["event_time"]
        for row in spark_session.read.format("delta").load(first.table_dir.as_posix()).collect()
    }
    manifest = json.loads((first.stream_dir / "manifest.json").read_text(encoding="utf-8"))
    violations = (first.stream_dir / "schema_violation.jsonl").read_text(encoding="utf-8")

    assert published.topic == TELEMETRY_TOPIC
    assert published.row_count == 97
    assert first.row_count == 97
    assert second.row_count == 97
    assert replayed.row_count == 97
    assert with_violation.row_count == 97
    assert len(rows) == 97
    assert "EVT-DUP-0001" in rows
    assert "EVT-0003-000012" in rows
    assert "EVT-BAD" not in rows
    assert manifest["topic"] == TELEMETRY_TOPIC
    assert manifest["rows"] == 97
    assert "checkpoint" in manifest["checkpoint_path"]
    assert "unknown_field" in violations
    assert first.checkpoint_dir.is_dir()


def _broker_open() -> bool:
    try:
        with socket.create_connection(("localhost", 19092), timeout=2):
            return True
    except OSError:
        return False


def _reset_topic(spark: SparkSession) -> None:
    jvm = spark._jvm
    assert jvm is not None
    properties = jvm.java.util.Properties()
    properties.put("bootstrap.servers", _BOOTSTRAP)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(properties)
    try:
        admin.deleteTopics([TELEMETRY_TOPIC]).all().get()
    except Exception:
        return
    finally:
        admin.close()


def _publish_bad_record(spark: SparkSession) -> None:
    payload = {
        "event_id": "EVT-BAD",
        "vehicle_id": "VEH-0001",
        "event_time": "2026-01-12T06:00:00+00:00",
        "received_at": "2026-01-12T06:00:00+00:00",
        "latitude": 45.0,
        "longitude": -90.0,
        "speed_kph": 0,
        "engine_state": "idle",
        "fuel_rate_lph": 3.5,
        "engine_hours": 249.25,
        "note": "later",
    }
    frame = spark.createDataFrame([(payload["event_id"], json.dumps(payload))], ["key", "value"])
    (
        frame.write.format("kafka")
        .option("kafka.bootstrap.servers", _BOOTSTRAP)
        .option("kafka.acks", "all")
        .option("topic", TELEMETRY_TOPIC)
        .save()
    )
