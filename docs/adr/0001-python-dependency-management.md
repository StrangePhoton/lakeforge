# ADR 0001: Python dependency management

## Status

Accepted

## Context

LakeForge needs a reproducible Python environment for a long-lived reference architecture. Later milestones add heavy dependencies such as PySpark. The environment has to be the same on a Windows workstation and in GitHub Actions, and a new clone has to be able to install it without undocumented steps.

The candidates considered at Milestone 0 were uv, Poetry, and pip with a virtual environment.

## Decision

Use uv with `pyproject.toml` and a committed `uv.lock`.

- Python is pinned to 3.12 in `.python-version` and `requires-python`.
- Direct dependencies use compatible ranges in `pyproject.toml`.
- `uv.lock` pins the resolved graph.
- CI installs with `uv sync --frozen`.
- Development tools (pytest, Ruff, mypy) live in the `dev` dependency group.

uv 0.12.23 was the current stable release when this decision was implemented. The lockfile, not this ADR, is the pin for Python packages.

## Alternatives

- Poetry provides a lockfile and a mature workflow. It adds a second toolchain and its own lock format beside the data-platform dependencies this repository will accumulate.
- pip plus `venv` is familiar and has the least machinery. Without an extra locking tool, two installs on different days can resolve different trees. A `pip-tools` lockfile can close that gap, with more manual steps than `uv sync`.

## Consequences

- Contributors install uv, then run `uv sync`.
- Dependency changes go through `uv lock` and the updated lockfile is committed.
- CI fails if `pyproject.toml` and `uv.lock` disagree.
- The lockfile format is uv-specific. Recreating it with another installer is a later decision, not a silent switch.

## Trade-offs

uv is maintained by Astral and its releases move quickly. That is acceptable because the project pins the resolved Python packages and records the uv release used to produce the lockfile. Poetry would be a reasonable choice for a Django application team. It is more ceremony than this repository needs. Unpinned pip is too weak for a reference architecture that must stay reproducible across milestones.
