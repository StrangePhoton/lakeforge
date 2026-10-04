# Milestone 1: synthetic industrial operational source

Status: implemented.

This milestone widens the Milestone 0 source into a deterministic fleet dataset. It does not add Spark, a lakehouse table format, streaming, or a data-quality engine.

## Operational entities

| Entity | Table | Relationship |
| --- | --- | --- |
| Site | `source.sites` | Master data |
| Vehicle | `source.vehicles` | `site_id` references `sites` |
| TelemetryEvent | `source.telemetry_events` | External machine event feed. No foreign key to `vehicles` |
| Alarm | `source.alarms` | `vehicle_id` references `vehicles`. Raised from clean telemetry |
| WorkOrder | `source.work_orders` | References `vehicles`, `sites`, and `alarms`. Opened only from `SERVICE_DUE` |

`received_at` is always present on telemetry. `event_time` and `vehicle_id` may be absent after the anomaly plan runs. The reason is recorded in [ADR 0005](../adr/0005-operational-source-and-telemetry-feed.md).

## Profiles

The default profile is `dev`. `--vehicles`, `--hours`, and `--interval-minutes` override size only. The window start stays with the profile. Counts below include the default anomaly plan (one extra duplicate row, twelve catalog entries).

| Profile | Sites | Vehicles | Window | Clean telemetry | With anomalies |
| --- | --- | --- | --- | --- | --- |
| `tiny` | 2 | 4 | 2026-01-12 06:00Z, 6 hours, 15 minutes | 96 | 97 |
| `dev` | 2 | 8 | 2026-01-12 00:00Z, 24 hours, 15 minutes | 768 | 769 |
| `large` | 2 | 20 | 2026-01-05 00:00Z, 7 days, 15 minutes | 13440 | 13441 |

`large` is a larger development dataset. It is not a benchmark profile. Million-row runs belong to a later performance milestone.

For seed 42 with anomalies, `dev` also lands 17 alarms (16 `IDLE_EXCESS`, 1 `SERVICE_DUE`) and 1 completed work order. `tiny` lands 5 alarms and 1 completed work order. The seed changes measurement jitter only. Identifiers, schedules, alarms, work orders, and defect placement stay fixed.

Sites are always `SITE-ALPHA` and `SITE-BRAVO`. Vehicles rotate through haul truck, excavator, wheel loader, and dozer.

## Behavior

Engine state follows the UTC hour: 00–05 off, 06 idle, 07–16 working, 17–19 idle, 20–23 off. Off readings have zero speed and zero fuel and do not move. The excavator stays at its site origin and never reports speed. Other working assets move a short distance around the site and burn fuel in a fixed range for that asset type.

`IDLE_EXCESS` is raised once when a continuous idle streak reaches 60 minutes. `SERVICE_DUE` is raised when engine hours cross a 250-hour boundary. A service alarm opens one work order, completed when the four-hour job finishes inside the window.

## Anomalies

Defects are applied after alarms and work orders. The answer key is `anomalies.json` in the landing run. Operational rows are not labeled. See [ADR 0006](../adr/0006-anomaly-catalog.md).

## Landing layout

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

`manifest.json` uses pipeline name `source_snapshot`. `anomalies.json` is written by `pipeline` (an empty array when `--no-anomalies` is set). `ingest` alone does not write the answer key.

`lakeforge generate` drops and recreates schema `source` from `src/lakeforge/source/schema.sql`, then loads the snapshot.

## What is deliberately absent

No data-quality engine, quarantine, data contracts, schema evolution, Spark, Delta Lake, Kafka, cloud targets, or benchmark harness.
