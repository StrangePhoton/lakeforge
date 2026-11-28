# LakeForge

Open-source reference architecture for an industrial fleet lakehouse. The domain is synthetic: vehicles, telemetry, and, in later milestones, maintenance and operations. No employer data belongs in this repository.

Milestone 7 is a local operational source, a JSONL landing zone, published data contracts, Bronze, Silver, and Gold Delta tables, telemetry quality checks, a local Kafka stream for telemetry, and three quality-gated domain products. Gold stays the unfiltered activity snapshot.

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
published contract check
        |
        v
local Spark session
        |
        v
Bronze Delta tables
        |
        v
Silver conformed tables
        |
        v
Gold activity tables

quality checks read Silver and write violations beside the rows that pass
        |
        v
fleet, operations, and maintenance products

telemetry JSONL can also be published to local Kafka and loaded by Structured Streaming
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
| Silver conformed tables and Gold activity tables | Implemented |
| Telemetry quality checks and quarantine | Implemented |
| Contracts and schema evolution | Implemented |
| Local Kafka and Structured Streaming for telemetry | Implemented |
| Domain products for fleet, operations, and maintenance | Implemented |
| Azure Event Hubs | Planned |
| Terraform, Azure, Databricks, Fabric | Planned |
| OpenTelemetry, Prometheus, Grafana | Planned |
| Kubernetes, ForgeFlow | Planned |

Architecture: [docs/architecture/overview.md](docs/architecture/overview.md). Milestone 7 detail: [docs/architecture/milestone-7.md](docs/architecture/milestone-7.md). Decisions: [docs/adr](docs/adr).

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- Java 17 or 21 on `PATH` for local Spark
- Docker Desktop, with the engine running
- Host port 54329 available for PostgreSQL
- Host port 19092 available for Kafka

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
uv run lakeforge contract --landing-run data/landing/run_id=<id>
uv run lakeforge bronze --landing-run data/landing/run_id=<id>
uv run lakeforge silver --bronze-run data/bronze/run_id=<id>
uv run lakeforge gold --silver-run data/silver/run_id=<id>
uv run lakeforge quality --silver-run data/silver/run_id=<id>
uv run lakeforge publish --landing-run data/landing/run_id=<id>
uv run lakeforge stream
uv run lakeforge product --silver-run data/silver/run_id=<id> --quality-run data/quality/run_id=<id>
```

Profiles are `tiny`, `dev`, and `large`. `large` is a bigger development dataset (20 vehicles, 7 days, 13440 clean telemetry rows), not a benchmark. `generate` drops and recreates the source schema, then replaces the snapshot. `ingest` extracts the current snapshot, writes `seed: null`, and does not write `anomalies.json`. `pipeline` does both, records the seed, and writes the anomaly catalog. `contract` checks that landing directory against `contracts/` and writes a report under `data/contracts/run_id=<id>/`. `bronze` runs the same check, then overwrites local Delta tables under `data/bronze/run_id=<id>/`. It does not copy `anomalies.json`. Telemetry may carry an optional `payload_kg`; this generator leaves it null. `silver` conforms that Bronze run, and `gold` writes `vehicle_activity` and `site_activity`. Invalid telemetry stays in Silver. Gold counts it and does not average it. `quality` writes the failing telemetry rows and the rows that pass. It does not read `anomalies.json` and does not rewrite Silver or Gold. `publish` sends that landing run's telemetry to Kafka after the contract check. `stream` merges the topic into `data/stream/telemetry_events` by `event_id` and then stops. A second publish of the same events leaves one row per event id. `product` publishes `fleet.vehicle_activity`, `operations.site_activity`, and `maintenance.alarm_events` from that Silver run and its quality output. Event counts use `telemetry_passed`. A reference-rule failure blocks the publication. The descriptors in `products/` name the domain as owner, version 1, freshness `snapshot`, and classification `synthetic`.

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

GitHub Actions runs the same lint, format, type check, and test commands against PostgreSQL 17.11, Kafka 4.4.0, and Temurin 17. CI sets `LAKEFORGE_POSTGRES_PORT=5432` for the Actions service container. Local Compose publishes PostgreSQL on port 54329 and Kafka on port 19092. Pytest also starts a local Spark session for the Bronze, Silver, Gold, quality, contract, product, and streaming tests. The streaming tests skip when the broker is down.

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
| Spark Kafka connector | `spark-sql-kafka-0-10_2.13` 4.2.0 |
| Kafka image | `apache/kafka:4.4.0` |
| Java for Spark | 17 or 21 |

## License

MIT. See [LICENSE](LICENSE).
