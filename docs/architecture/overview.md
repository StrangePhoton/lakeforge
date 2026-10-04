# Architecture overview

LakeForge is a reference architecture for an industrial fleet data platform. All data in the repository is synthetic. This document separates the system that exists from the system that is planned.

## Implemented

Milestone 1 runs on a workstation. PostgreSQL is the simulated operational source. A Python process on the host generates a deterministic snapshot of sites, vehicles, telemetry, alarms, and work orders, loads it, and extracts it to a local JSONL landing zone.

```
synthetic generator
        |
        v
PostgreSQL 17  (schema source)
  sites, vehicles, telemetry_events, alarms, work_orders
        |
        v
full-snapshot extract
        |
        v
data/landing/run_id=<run_id>/
    sites.jsonl
    vehicles.jsonl
    telemetry_events.jsonl
    alarms.jsonl
    work_orders.jsonl
    anomalies.json
    manifest.json
```

Telemetry is an externally produced machine event feed. It does not have a foreign key to vehicles. Sites, vehicles, alarms, and work orders stay relationally constrained. `anomalies.json` is the simulation answer key and is not an operational table.

The Python package is `lakeforge`:

| Module | Role |
| --- | --- |
| `lakeforge.domain` | Site, vehicle, telemetry, alarm, and work order records |
| `lakeforge.synthetic` | Profiles, seeded generation, and the fixed anomaly plan |
| `lakeforge.source` | PostgreSQL schema, load, and snapshot read |
| `lakeforge.ingest` | Landing writer and batch orchestration |
| `lakeforge.config` | Environment-based settings |
| `lakeforge.logging` | One JSON object per log line |
| `lakeforge.cli` | `generate`, `ingest`, and `pipeline` |

There is one schema definition: `src/lakeforge/source/schema.sql`. Compose mounts it, and generation executes it after dropping schema `source`.

Details are in [milestone-1.md](milestone-1.md). The Milestone 0 slice is recorded in [milestone-0.md](milestone-0.md). Decisions are in [../adr](../adr).

## Planned

The following are not implemented. They are the direction for later milestones and are not available to run.

```
Sources
   |
   v
Batch + streaming ingestion
   |
   v
Bronze -> Silver -> Gold
   |
   v
Governed domain data products
   |
   v
Analytics / APIs / AI consumers
```

Planned technologies, each only when its milestone starts:

- PySpark, Apache Spark, and Delta Lake
- Kafka locally, Azure Event Hubs in Azure
- ADLS Gen2, Azure Databricks, Azure Key Vault, Microsoft Entra
- Terraform and GitHub Actions beyond the current lint-and-test workflow
- Microsoft Fabric and OneLake as a second implementation target
- Data quality, quarantine, contracts, and schema evolution
- Domain data products for fleet, maintenance, and operations
- OpenTelemetry, Prometheus, and Grafana
- A Kubernetes decision, which may be a decision not to use Kubernetes
- A loose ForgeFlow integration through a governed query interface

The JSONL landing zone is the pre-lakehouse extract. It is not a Bronze Delta table.
