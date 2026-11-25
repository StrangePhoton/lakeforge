"""Resolve one landing run without leaving the landing directory."""

import re
from pathlib import Path

from lakeforge.lakehouse.catalog import DATASETS

_RUN_DIRECTORY = re.compile(r"^run_id=([A-Za-z0-9_-]+)$")


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
