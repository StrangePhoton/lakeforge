from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from lakeforge.config import Settings
from lakeforge.ingest.landing import write_landing_run
from lakeforge.lakehouse.bronze import run_bronze
from lakeforge.lakehouse.product import run_product
from lakeforge.lakehouse.quality import run_quality
from lakeforge.lakehouse.session import build_session
from lakeforge.lakehouse.silver import run_silver
from lakeforge.synthetic.generator import generate_dataset

pytestmark = pytest.mark.spark

_STARTED = datetime(2026, 1, 12, 12, 0, tzinfo=UTC)
_FINISHED = datetime(2026, 1, 12, 12, 0, 2, tzinfo=UTC)
_LATE = datetime(2026, 1, 11, 20, 45, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_session(warehouse_dir=warehouse)
    yield session
    session.stop()


def test_tiny_products_count_passed_telemetry_only(
    spark_session: SparkSession,
    tmp_path: Path,
) -> None:
    dataset = generate_dataset(seed=42, profile="tiny", anomalies=True)
    landing_root = tmp_path / "landing"
    run_dir = write_landing_run(
        landing_dir=landing_root,
        run_id="product-tiny",
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
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        quality_dir=tmp_path / "quality",
        products_dir=tmp_path / "products",
        contracts_dir=tmp_path / "contracts",
    )
    bronze = run_bronze(settings, run_dir, spark=spark_session)
    silver = run_silver(settings, bronze.bronze_dir, spark=spark_session)
    quality = run_quality(settings, silver.silver_dir, spark=spark_session)

    published = run_product(
        settings,
        silver.silver_dir,
        quality.quality_dir,
        spark=spark_session,
    )
    vehicles = {
        row["vehicle_id"]: row
        for row in spark_session.read.format("delta")
        .load((published.products_dir / "fleet_vehicle_activity").as_posix())
        .collect()
    }
    sites = {
        row["site_id"]: row
        for row in spark_session.read.format("delta")
        .load((published.products_dir / "operations_site_activity").as_posix())
        .collect()
    }
    alarms = spark_session.read.format("delta").load(
        (published.products_dir / "maintenance_alarm_events").as_posix()
    )
    manifest = published.products_dir.joinpath("manifest.json").read_text(encoding="utf-8")

    assert {name: count for name, count in published.row_counts} == {
        "fleet.vehicle_activity": 4,
        "operations.site_activity": 2,
        "maintenance.alarm_events": 5,
    }
    assert _activity(vehicles["VEH-0001"]) == (20, 20, 2, 1)
    assert _activity(vehicles["VEH-0002"]) == (21, 21, 1, 0)
    assert _activity(vehicles["VEH-0003"]) == (21, 21, 1, 0)
    assert _activity(vehicles["VEH-0004"]) == (22, 22, 1, 0)
    assert _as_utc(vehicles["VEH-0003"]["first_event_time"]) != _LATE
    assert sites["SITE-ALPHA"]["site_name"] == "Site Alpha"
    assert _site(sites["SITE-ALPHA"]) == (2, 41, 3, 1)
    assert sites["SITE-BRAVO"]["site_name"] == "Site Bravo"
    assert _site(sites["SITE-BRAVO"]) == (2, 43, 2, 0)
    assert alarms.count() == 5
    assert '"version": 1' in manifest
    assert "fleet.vehicle_activity" in manifest
    assert not (tmp_path / "gold").exists()


def _activity(row: object) -> tuple[int, int, int, int]:
    record = row.asDict()  # type: ignore[attr-defined]
    return (
        int(record["event_count"]),
        int(record["timed_event_count"]),
        int(record["alarm_count"]),
        int(record["work_order_count"]),
    )


def _site(row: object) -> tuple[int, int, int, int]:
    record = row.asDict()  # type: ignore[attr-defined]
    return (
        int(record["vehicle_count"]),
        int(record["event_count"]),
        int(record["alarm_count"]),
        int(record["work_order_count"]),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.astimezone().astimezone(UTC)
    return value.astimezone(UTC)
