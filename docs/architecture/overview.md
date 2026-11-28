# Architecture overview

LakeForge is a reference architecture for an industrial fleet data platform. All data in the repository is synthetic. This document separates the system that exists from the system that is planned.

## Implemented

Milestone 7 runs on a workstation. PostgreSQL is the simulated operational source. A Python process generates a deterministic snapshot, extracts it to a JSONL landing run, checks that run against published contracts, loads it into local Bronze Delta tables, conforms it to Silver, aggregates Gold activity tables, and writes telemetry quality violations beside the rows that pass. The same telemetry file can also be published to local Kafka and loaded by Structured Streaming into a separate Delta table. A quality-gated command publishes fleet, operations, and maintenance products from the Silver snapshot and the passed telemetry.

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
        |
        v
lakeforge contract
        |
        v
data/contracts/run_id=<run_id>/
    manifest.json
    schema_violation.jsonl
        |
        v
lakeforge bronze  (same check, then local Spark)
        |
        v
data/bronze/run_id=<run_id>/
    sites, vehicles, telemetry_events, alarms, work_orders
    manifest.json
        |
        v
lakeforge silver
        |
        v
data/silver/run_id=<run_id>/
        |
        +-- lakeforge gold --> data/gold/run_id=<run_id>/
        |                      vehicle_activity, site_activity
        |
        +-- lakeforge quality --> data/quality/run_id=<run_id>/
                                 telemetry_violation, telemetry_passed
                                      |
                                      v
                            lakeforge product
                                      |
                                      v
                            data/products/run_id=<run_id>/
                                fleet_vehicle_activity
                                operations_site_activity
                                maintenance_alarm_events

telemetry_events.jsonl
        |
        v
lakeforge publish --> Kafka lakeforge.telemetry.events
        |
        v
lakeforge stream --> data/stream/telemetry_events
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
| `lakeforge.lakehouse` | Contracts, local Spark session, Bronze, Silver, Gold, quality, the telemetry stream, and domain products |
| `lakeforge.cli` | `generate`, `ingest`, `pipeline`, `contract`, `bronze`, `silver`, `gold`, `quality`, `publish`, `stream`, and `product` |

There is one schema definition: `src/lakeforge/source/schema.sql`. Compose mounts it, and generation executes it after dropping schema `source`.

Details are in [milestone-7.md](milestone-7.md). The telemetry stream is recorded in [milestone-6.md](milestone-6.md), contracts in [milestone-5.md](milestone-5.md), quality in [milestone-4.md](milestone-4.md), Silver and Gold in [milestone-3.md](milestone-3.md), Bronze in [milestone-2.md](milestone-2.md), the operational source in [milestone-1.md](milestone-1.md), and the first slice in [milestone-0.md](milestone-0.md). Decisions are in [../adr](../adr). The published contracts are in [../../contracts](../../contracts). The product descriptors are in [../../products](../../products).

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
Catalog, retention, and product APIs
   |
   v
Analytics / AI consumers
```

Planned technologies, each only when its milestone starts:

- Azure Event Hubs
- ADLS Gen2, Azure Databricks, Azure Key Vault, Microsoft Entra
- Terraform and GitHub Actions beyond the current lint-and-test workflow
- Microsoft Fabric and OneLake as a second implementation target
- OpenTelemetry, Prometheus, and Grafana
- A Kubernetes decision, which may be a decision not to use Kubernetes
- A loose ForgeFlow integration through a governed query interface

The JSONL landing zone remains the operational extract. Published contracts check that extract before Bronze. Bronze, Silver, and Gold are local Delta snapshots of one run. Quality violations are written beside the Silver rows that pass. Telemetry can also flow through local Kafka into `data/stream`. `lakeforge product` publishes fleet vehicle activity, operations site activity, and maintenance alarm events when the quality reference checks are clean. Gold stays unfiltered.
