"""Manifests for one Delta snapshot directory."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame


def landing_run_id(manifest: dict[str, Any], directory: Path) -> str:
    """Return the run id when the manifest names this directory."""
    run_id = manifest.get("landing_run_id")
    if not isinstance(run_id, str) or directory.name != f"run_id={run_id}":
        raise ValueError("manifest landing_run_id does not match the run directory")
    return run_id


def dataset_counts(
    manifest: dict[str, Any],
    names: tuple[str, ...],
    *,
    layer: str,
) -> dict[str, int]:
    """Return the manifest row count for each named dataset."""
    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError(f"{layer} manifest is missing dataset counts")
    counts: dict[str, int] = {}
    for name in names:
        entry = datasets.get(name)
        if not isinstance(entry, dict) or not isinstance(entry.get("rows"), int):
            raise ValueError(f"{layer} manifest is missing a row count for {name}")
        counts[name] = entry["rows"]
    return counts


def write_snapshot_manifest(
    path: Path,
    *,
    run_id: str,
    source_key: str,
    source_path: Path,
    counts: tuple[tuple[str, int], ...],
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Write the snapshot manifest after the Delta tables are in place."""
    payload = {
        "landing_run_id": run_id,
        source_key: str(source_path),
        "format": "delta",
        "started_at": started_at.astimezone(UTC).isoformat(timespec="seconds"),
        "finished_at": finished_at.astimezone(UTC).isoformat(timespec="seconds"),
        "datasets": {name: {"rows": count} for name, count in counts},
        "status": "succeeded",
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def overwrite_delta(frame: DataFrame, target: Path) -> None:
    """Replace one Delta table with the contents of ``frame``."""
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target.as_posix())
    )
