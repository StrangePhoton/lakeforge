# LakeForge

Open-source reference architecture for an industrial fleet lakehouse. The domain is synthetic: vehicles, telemetry, and, in later milestones, maintenance and operations. No employer data belongs in this repository.

Milestone 0 is a local vertical slice. It is not a lakehouse yet.

```
synthetic generator
        |
        v
PostgreSQL source simulation
        |
        v
full-snapshot batch extract
        |
        v
JSONL landing zone + manifest
```

## Status

| Area | State |
| --- | --- |
| Python 3.12 package, uv lockfile, Ruff, mypy, pytest | Implemented |
| Environment-based configuration and JSON logging | Implemented |
| Deterministic vehicle and telemetry generator | Implemented |
| PostgreSQL 17 source simulation and Docker Compose | Implemented |
| Full-snapshot extract to a JSONL landing run | Implemented |
| GitHub Actions lint, type check, and tests | Implemented |
| Spark, PySpark, Delta Lake, medallion layers | Planned |
| Data quality, quarantine, contracts, schema evolution | Planned |
| Kafka, Event Hubs, streaming | Planned |
| Domain data products | Planned |
| Terraform, Azure, Databricks, Fabric | Planned |
| OpenTelemetry, Prometheus, Grafana | Planned |
| Kubernetes, ForgeFlow | Planned |

Architecture: [docs/architecture/overview.md](docs/architecture/overview.md). Milestone 0 detail: [docs/architecture/milestone-0.md](docs/architecture/milestone-0.md). Decisions: [docs/adr](docs/adr).

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- Docker Desktop, with the engine running
- Host port 54329 available

uv installs CPython 3.12. A system Python is optional.

## Local setup

From the repository root in PowerShell:

```powershell
uv python install 3.12
uv sync
Copy-Item .env.example .env
docker compose up -d --wait
uv run lakeforge pipeline
```

The pipeline loads 4 vehicles and 96 telemetry events for seed 42, then writes:

```
data/landing/run_id=<run_id>/
    manifest.json
    vehicles.jsonl
    telemetry_events.jsonl
```

`data/` is gitignored. Logs are one JSON object per line on stdout and include `run_id` and `row_count`.

The database user and password in Compose are the local dummy `lakeforge`. They are for this synthetic database only.

## Commands

```powershell
uv run lakeforge generate
uv run lakeforge ingest
uv run lakeforge pipeline --seed 42 --vehicles 4 --hours 6 --interval-minutes 15
```

`generate` replaces the PostgreSQL snapshot. `ingest` extracts the current snapshot and writes `seed: null` in the manifest, because the source tables do not store the seed. `pipeline` does both and records the seed.

## Checks

```powershell
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
```

Integration tests use PostgreSQL. If it is not running, they skip and tell you to start Compose. Start the database when you want the full suite:

```powershell
docker compose up -d --wait
uv run pytest
```

GitHub Actions runs the same lint, format, type check, and test commands against PostgreSQL 17.11. CI sets `LAKEFORGE_POSTGRES_PORT=5432` for the Actions service container. Local Compose publishes port 54329.

## Locked toolchain

Python packages below are pinned in `uv.lock`. uv 0.12.23 produced that lockfile. The PostgreSQL image tag is pinned in Docker Compose and in CI.

| Tool | Version |
| --- | --- |
| CPython | 3.12.15 |
| uv | 0.12.23 |
| PostgreSQL image | `postgres:17.11` |
| psycopg | 3.3.6 |
| pydantic | 2.13.5 |
| pydantic-settings | 2.15.0 |
| pytest | 9.1.1 |
| Ruff | 0.16.10 |
| mypy | 2.4.0 |

## License

MIT. See [LICENSE](LICENSE).
