"""Resolve one landing run without leaving the landing directory."""

import re
from pathlib import Path

from lakeforge.lakehouse.catalog import DATASETS

_RUN_DIRECTORY = re.compile(r"^run_id=([A-Za-z0-9_-]+)$")


def resolve_delta_run(
    root_dir: Path,
    run: Path,
    tables: tuple[str, ...],
    *,
    layer: str,
) -> Path:
    """Return one Delta run directory when it is a direct child of ``root_dir``."""
    if ".." in run.parts:
        raise ValueError(f"{layer} run must stay inside the {layer} directory")
    root = root_dir.resolve()
    resolved = _candidate(root, run)
    if not resolved.is_relative_to(root) or resolved.parent != root:
        raise ValueError(f"{layer} run must stay inside the {layer} directory")
    if _RUN_DIRECTORY.fullmatch(resolved.name) is None:
        raise ValueError(f"{layer} run directory must be named run_id=<id>")
    if not resolved.is_dir():
        raise ValueError(f"{layer} run directory does not exist")
    if not (resolved / "manifest.json").is_file():
        raise ValueError(f"{layer} run is missing manifest.json")
    missing = [name for name in tables if not (resolved / name / "_delta_log").is_dir()]
    if missing:
        raise ValueError(f"{layer} run is missing the {', '.join(missing)} Delta table")
    return resolved


def resolve_products_dir(products_dir: Path) -> Path:
    """Return the products directory when the path does not climb out of it."""
    if ".." in products_dir.parts:
        raise ValueError("products directory must stay inside the products directory")
    return products_dir.resolve()


def resolve_stream_dir(stream_dir: Path) -> Path:
    """Return the stream directory when the path does not climb out of it."""
    if ".." in stream_dir.parts:
        raise ValueError("stream directory must stay inside the stream directory")
    return stream_dir.resolve()


def resolve_landing_run(landing_dir: Path, landing_run: Path) -> Path:
    """Return the landing run directory when it is a direct child of ``landing_dir``."""
    if ".." in landing_run.parts:
        raise ValueError("landing run must stay inside the landing directory")
    root = landing_dir.resolve()
    resolved = _candidate(root, landing_run)
    if not resolved.is_relative_to(root) or resolved.parent != root:
        raise ValueError("landing run must stay inside the landing directory")
    match = _RUN_DIRECTORY.fullmatch(resolved.name)
    if match is None:
        raise ValueError("landing run directory must be named run_id=<id>")
    if not resolved.is_dir():
        raise ValueError("landing run directory does not exist")
    if not (resolved / "manifest.json").is_file():
        raise ValueError("landing run is missing manifest.json")
    missing = [f"{name}.jsonl" for name in DATASETS if not (resolved / f"{name}.jsonl").is_file()]
    if missing:
        raise ValueError(f"landing run is missing {', '.join(missing)}")
    return resolved


def _candidate(root: Path, landing_run: Path) -> Path:
    if landing_run.is_absolute():
        return landing_run.resolve()
    under_root = (root / landing_run).resolve()
    if under_root.parent == root:
        return under_root
    return (Path.cwd() / landing_run).resolve()
