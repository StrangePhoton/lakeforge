# ADR 0005: Operational source model and the telemetry event feed

## Status

Accepted. Supersedes the two-table source model in [ADR 0002](0002-m0-source-and-landing-format.md).

## Context

Milestone 1 replaces the two-table demonstration with a small industrial operational source: sites, vehicles, telemetry, alarms, and work orders. Those records have to stay deterministic and correlated so later quality and lakehouse work has a realistic dataset to read.

Telemetry in this domain is produced by machines and received by the operational system. A reading can be stored before the equipment identifier has been reconciled to the vehicle master, and the device clock can be missing. That is a property of this source, not a general ingestion platform.

## Decision

Model five tables in schema `source`:

- `sites`
- `vehicles`, with `site_id` referencing `sites`
- `telemetry_events`
- `alarms`, with `vehicle_id` referencing `vehicles`
- `work_orders`, with `vehicle_id` referencing `vehicles`, `site_id` referencing `sites`, and `alarm_id` referencing `alarms` when present

Treat `telemetry_events` as the stored form of an externally produced machine event feed for this fleet source.

- `vehicle_id` is not a foreign key. The source accepts a reading whose equipment identifier is absent or is not yet a row in `vehicles`.
- `event_time` is nullable. A null event time is an incomplete reading, not a deleted one.
- `received_at` is mandatory. It is the time the source stored the reading.

Master data and maintenance stay relationally constrained. A vehicle cannot name an unknown site. An alarm cannot name an unknown vehicle. A work order cannot name an unknown vehicle, site, or alarm.

`lakeforge generate` and `lakeforge pipeline` drop schema `source` and execute `src/lakeforge/source/schema.sql` again before loading rows. There is still one schema file and no migration framework. `lakeforge ingest` only reads.

The landing run is still a full JSONL snapshot. The pipeline name is `source_snapshot`. The five operational files are `sites.jsonl`, `vehicles.jsonl`, `telemetry_events.jsonl`, `alarms.jsonl`, and `work_orders.jsonl`.

## Integrity that remains enforced

- PostgreSQL rejects a vehicle whose `site_id` is not in `sites`.
- PostgreSQL rejects an alarm whose `vehicle_id` is not in `vehicles`.
- PostgreSQL rejects a work order whose vehicle, site, or alarm reference is not in the corresponding table.
- The generator builds alarms and work orders from the clean telemetry story, before any telemetry defect is applied, and only opens a work order from `SERVICE_DUE`.
- Application code that writes the landing JSONL requires `received_at` on every telemetry row.

## Trade-off

The database will not stop a telemetry row from naming equipment the vehicle master does not contain, and it will not stop a row from omitting `event_time`. Unresolved identifiers such as `VEH-9999`, a null `vehicle_id`, and a null `event_time` are realistic contents of this feed. They are not rows inserted by disabling a constraint after the fact.

The cost is that a join from telemetry to vehicles is not guaranteed by PostgreSQL. A reader can see a reading that does not match a vehicle. That gap is intentional for this table only.

Later data-quality processing is expected to identify unresolved vehicle references and incomplete event times. This milestone does not implement that processing. It only stores the conditions and, when generation injected them, an answer key outside the operational tables. See [ADR 0006](0006-anomaly-catalog.md).

This decision does not define a generic event-ingestion architecture. It applies to `source.telemetry_events` in the fleet simulator.

## Consequences

- Profiles `tiny`, `dev`, and `large` share this schema. `large` is a bigger development dataset, not a performance benchmark.
- An existing Compose volume can be replaced by `lakeforge generate` without `docker compose down -v`. A brand-new volume still runs the same `schema.sql` from `docker-entrypoint-initdb.d`.
