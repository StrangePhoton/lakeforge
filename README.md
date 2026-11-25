# LakeForge

Open-source reference architecture for an industrial fleet lakehouse. The domain is synthetic: vehicles, telemetry, and, in later milestones, maintenance and operations. No employer data belongs in this repository.

Milestone 2 is a local operational source, a JSONL landing zone, and Bronze Delta tables. Silver and Gold are not built yet.

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
        |
        v
local Spark session
        |
        v
Bronze Delta tables
```

## Status

| Area | State |
| --- | --- |
| Python 3.12 package, uv lockfile, Ruff, mypy, pytest | Implemented |
| Environment-based configuration and JSON logging | Implemented |
| Deterministic sites, vehicles, telemetry, alarms, and work orders | Implemented |
| PostgreSQL 17 source simulation and Docker Compose | Implemented |
| Full-snapshot extract to a JSONL landing run | Implemented |
| Fixed telemetry anomaly catalog for later quality checks | Implemented |
| Local PySpark 4.2 and Bronze Delta tables | Implemented |
| GitHub Actions lint, type check, and tests | Implemented |
| Silver and Gold medallion tables | Planned |
| Data quality, quarantine, contracts, schema evolution | Planned |
| Kafka, Event Hubs, streaming | Planned |
| Domain data products | Planned |
| Terraform, Azure, Databricks, Fabric | Planned |
| OpenTelemetry, Prometheus, Grafana | Planned |
| Kubernetes, ForgeFlow | Planned |

Architecture: [docs/architecture/overview.md](docs/architecture/overview.md). Milestone 2 detail: [docs/architecture/milestone-2.md](docs/architecture/milestone-2.md). Decisions: [docs/adr](docs/adr).

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- Java 17 or 21 on `PATH` for the Bronze load
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

The default pipeline uses profile `dev` and seed 42. It loads 2 sites, 8 vehicles, 769 telemetry events (768 clean readings plus one duplicate), 17 alarms, and 1 work order, then writes:

```
data/landing/run_id=<run_id>/
    manifest.json
    sites.jsonl
    vehicles.jsonl
    telemetry_events.jsonl
    alarms.jsonl
    work_orders.jsonl
    anomalies.json
```

`data/` is gitignored. Logs are one JSON object per line on stdout and include `run_id` and `row_count`.

The database user and password in Compose are the local dummy `lakeforge`. They are for this synthetic database only.

## Commands

```powershell
uv run lakeforge generate
uv run lakeforge ingest
uv run lakeforge pipeline --profile tiny
uv run lakeforge pipeline --profile dev --no-anomalies
uv run lakeforge bronze --landing-run data/landing/run_id=<id>
```

Profiles are `tiny`, `dev`, and `large`. `large` is a bigger development dataset (20 vehicles, 7 days, 13440 clean telemetry rows), not a benchmark. `generate` drops and recreates the source schema, then replaces the snapshot. `ingest` extracts the current snapshot, writes `seed: null`, and does not write `anomalies.json`. `pipeline` does both, records the seed, and writes the anomaly catalog. `bronze` reads that landing directory and overwrites local Delta tables under `data/bronze/run_id=<id>/`. It does not copy `anomalies.json`.

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

GitHub Actions runs the same lint, format, type check, and test commands against PostgreSQL 17.11 and Temurin 17. CI sets `LAKEFORGE_POSTGRES_PORT=5432` for the Actions service container. Local Compose publishes port 54329. Pytest also starts a local Spark session for the Bronze tests.

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
| PySpark | 4.2.0 |
| delta-spark | 4.4.1 |
| Java for Bronze | 17 or 21 |

## License

MIT. See [LICENSE](LICENSE).
