"""Publish domain products from one Silver run and its quality output."""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from lakeforge.config import Settings
from lakeforge.ingest.landing import read_manifest
from lakeforge.lakehouse.catalog import DATASETS, LANDING_RUN_COLUMN, TABLES
from lakeforge.lakehouse.gold import _read_checked, _site_activity, _vehicle_activity
from lakeforge.lakehouse.paths import resolve_delta_run, resolve_products_dir
from lakeforge.lakehouse.quality import REFERENCE_RULES, TELEMETRY_RULES
from lakeforge.lakehouse.session import build_session, default_warehouse_dir
from lakeforge.lakehouse.snapshot import dataset_counts, landing_run_id, overwrite_delta

_LOGGER = logging.getLogger("lakeforge.product")
_PASSED_TABLE = "telemetry_passed"

FLEET_COLUMNS = (
    "vehicle_id",
    "site_id",
    "asset_type",
    "model_code",
    "status",
    "event_count",
    "timed_event_count",
    "first_event_time",
    "last_event_time",
    "alarm_count",
    "work_order_count",
    LANDING_RUN_COLUMN,
)
SITE_COLUMNS = (
    "site_id",
    "site_name",
    "vehicle_count",
    "event_count",
    "alarm_count",
    "work_order_count",
    LANDING_RUN_COLUMN,
)
ALARM_COLUMNS = (
    *(spec.name for spec in TABLES["alarms"]),
    LANDING_RUN_COLUMN,
)
PRODUCT_TABLES = (
    ("fleet.vehicle_activity", "fleet_vehicle_activity", "fleet", FLEET_COLUMNS, TELEMETRY_RULES),
    (
        "operations.site_activity",
        "operations_site_activity",
        "operations",
        SITE_COLUMNS,
        TELEMETRY_RULES,
    ),
    (
        "maintenance.alarm_events",
        "maintenance_alarm_events",
        "maintenance",
        ALARM_COLUMNS,
        REFERENCE_RULES,
    ),
)


@dataclass(frozen=True, slots=True)
class ProductResult:
    """Outcome of one product publication."""

    landing_run_id: str
    silver_dir: Path
    quality_dir: Path
    products_dir: Path
    row_counts: tuple[tuple[str, int], ...]


def catalog_dir() -> Path:
    """Return the checked-in product descriptors."""
    found = Path(__file__).resolve().parents[3] / "products"
    if not found.is_dir():
        raise ValueError("product catalog is missing")
    return found


def load_descriptors(directory: Path | None = None) -> dict[str, dict[str, Any]]:
    """Load the product descriptors and check them against the builders."""
    root = catalog_dir() if directory is None else directory
    loaded: dict[str, dict[str, Any]] = {}
    for name, _table, domain, columns, rules in PRODUCT_TABLES:
        path = root / f"{name}.json"
        if not path.is_file():
            raise ValueError(f"product catalog is missing {name}.json")
        payload = json.loads(path.read_text(encoding="utf-8"))
        _require_descriptor(payload, name, domain, columns, rules)
        loaded[name] = payload
    return loaded


def require_promotable(manifest: dict[str, Any]) -> None:
    """Raise when the quality manifest cannot promote a product."""
    rules = manifest.get("rules")
    if not isinstance(rules, dict):
        raise ValueError("quality manifest is missing rules")
    for name in TELEMETRY_RULES:
        if not isinstance(rules.get(name), int):
            raise ValueError(f"quality manifest is missing {name}")
    for name in REFERENCE_RULES:
        count = rules.get(name)
        if not isinstance(count, int):
            raise ValueError(f"quality manifest is missing {name}")
        if count > 0:
            raise ValueError("quality run has reference violations")


def run_product(
    settings: Settings,
    silver_run: Path,
    quality_run: Path,
    *,
    spark: SparkSession | None = None,
) -> ProductResult:
    """Publish the domain products for one Silver run."""
    products_root = resolve_products_dir(settings.products_dir)
    silver_dir = resolve_delta_run(settings.silver_dir, silver_run, DATASETS, layer="silver")
    quality_dir = resolve_delta_run(
        settings.quality_dir,
        quality_run,
        (_PASSED_TABLE,),
        layer="quality",
    )
    silver_manifest = read_manifest(silver_dir / "manifest.json")
    quality_manifest = read_manifest(quality_dir / "manifest.json")
    run_id = landing_run_id(silver_manifest, silver_dir)
    if landing_run_id(quality_manifest, quality_dir) != run_id:
        raise ValueError("silver run and quality run do not match")
    require_promotable(quality_manifest)
    load_descriptors()
    products_dir = products_root / silver_dir.name
    products_dir.mkdir(parents=True, exist_ok=True)
    expected = dataset_counts(silver_manifest, DATASETS, layer="silver")
    passed_expected = dataset_counts(quality_manifest, (_PASSED_TABLE,), layer="quality")
    started_at = datetime.now(UTC)
    _LOGGER.info("product started", extra={"run_id": run_id, "landing_path": str(silver_dir)})
    session = spark if spark is not None else build_session(warehouse_dir=default_warehouse_dir())
    try:
        counts = _write_products(
            session,
            silver_dir=silver_dir,
            quality_dir=quality_dir,
            products_dir=products_dir,
            expected=expected,
            passed_rows=passed_expected[_PASSED_TABLE],
        )
    finally:
        if spark is None:
            session.stop()
    finished_at = datetime.now(UTC)
    _write_manifest(
        products_dir / "manifest.json",
        run_id=run_id,
        silver_dir=silver_dir,
        quality_dir=quality_dir,
        counts=counts,
        started_at=started_at,
        finished_at=finished_at,
    )
    _LOGGER.info(
        "product finished",
        extra={"run_id": run_id, "row_count": counts[0][1], "landing_path": str(products_dir)},
    )
    return ProductResult(
        landing_run_id=run_id,
        silver_dir=silver_dir,
        quality_dir=quality_dir,
        products_dir=products_dir,
        row_counts=counts,
    )


def _write_products(
    spark: SparkSession,
    *,
    silver_dir: Path,
    quality_dir: Path,
    products_dir: Path,
    expected: dict[str, int],
    passed_rows: int,
) -> tuple[tuple[str, int], ...]:
    vehicles = _read_checked(spark, silver_dir, "vehicles", expected["vehicles"])
    sites = _read_checked(spark, silver_dir, "sites", expected["sites"])
    alarms = _read_checked(spark, silver_dir, "alarms", expected["alarms"])
    work_orders = _read_checked(spark, silver_dir, "work_orders", expected["work_orders"])
    passed = spark.read.format("delta").load((quality_dir / _PASSED_TABLE).as_posix())
    if passed.count() != passed_rows:
        raise ValueError("telemetry_passed row count does not match the quality manifest")
    frames = {
        "fleet.vehicle_activity": _vehicle_activity(vehicles, passed, alarms, work_orders),
        "operations.site_activity": _site_activity(sites, vehicles, passed, alarms, work_orders),
        "maintenance.alarm_events": alarms.select(*ALARM_COLUMNS),
    }
    written: list[tuple[str, int]] = []
    for name, _table, _domain, columns, _rules in PRODUCT_TABLES:
        frame = _with_columns(frames[name], columns, name)
        count = frame.count()
        overwrite_delta(frame, products_dir / _table_name(name))
        written.append((name, count))
        _LOGGER.info(
            "product written",
            extra={"run_id": name, "dataset": name, "row_count": count},
        )
    return tuple(written)


def _with_columns(frame: DataFrame, columns: tuple[str, ...], product: str) -> DataFrame:
    if tuple(frame.columns) != columns:
        raise ValueError(f"{product} columns do not match the product descriptor")
    return frame


def _table_name(product: str) -> str:
    for name, table, _domain, _columns, _rules in PRODUCT_TABLES:
        if name == product:
            return table
    raise ValueError(f"unknown product {product}")


def _require_descriptor(
    payload: object,
    name: str,
    domain: str,
    columns: tuple[str, ...],
    rules: tuple[str, ...],
) -> None:
    if not isinstance(payload, dict):
        raise ValueError(f"{name} descriptor must be a JSON object")
    if payload.get("name") != name or payload.get("domain") != domain:
        raise ValueError(f"{name} descriptor does not match the product")
    if payload.get("owner") != domain or payload.get("version") != 1:
        raise ValueError(f"{name} descriptor does not match the product")
    if payload.get("compatibility") != "backward":
        raise ValueError(f"{name} descriptor does not match the product")
    if payload.get("classification") != "synthetic" or payload.get("freshness") != "snapshot":
        raise ValueError(f"{name} descriptor does not match the product")
    columns_match = payload.get("columns") == list(columns)
    rules_match = payload.get("quality_requirements") == list(rules)
    if not columns_match or not rules_match:
        raise ValueError(f"{name} descriptor does not match the product")
    if not isinstance(payload.get("description"), str) or not payload["description"]:
        raise ValueError(f"{name} descriptor does not match the product")


def _write_manifest(
    path: Path,
    *,
    run_id: str,
    silver_dir: Path,
    quality_dir: Path,
    counts: tuple[tuple[str, int], ...],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    payload = {
        "landing_run_id": run_id,
        "silver_path": str(silver_dir),
        "quality_path": str(quality_dir),
        "products": {
            name: {"version": 1, "rows": count, "table": _table_name(name)}
            for name, count in counts
        },
        "started_at": started_at.astimezone(UTC).isoformat(timespec="seconds"),
        "finished_at": finished_at.astimezone(UTC).isoformat(timespec="seconds"),
        "status": "succeeded",
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
