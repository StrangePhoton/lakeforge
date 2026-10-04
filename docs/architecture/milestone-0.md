# Milestone 0: foundation and first local vertical slice

Status: implemented, historical.

The current operational model is [Milestone 1](milestone-1.md) and [ADR 0005](../adr/0005-operational-source-and-telemetry-feed.md). This page records the Milestone 0 slice as it was built.

## What this milestone demonstrates

A deterministic generator builds a small fleet snapshot. The snapshot is loaded into PostgreSQL. A batch extract reads it back and writes a landing run.

Default profile:

| Input | Value |
| --- | --- |
| Seed | `42` |
| Vehicles | 4: haul truck, excavator, wheel loader, dozer |
| Site | `SITE-ALPHA` (fictional) |
| Window start | `2026-01-12T06:00:00+00:00` |
| Duration | 6 hours at a 15-minute interval |
| Telemetry rows | 96 |

The clock is fixed. Generation does not call `now`. The same seed and profile rebuild the same source rows. Each extract receives a new `run_id`. Tests may inject the `run_id`.

Coordinates start near latitude 45 and longitude -90 and walk a few thousandths of a degree. They are synthetic. Engine state is `off`, `idle`, or `working` from the hour of the fixed window. Speed is 0 when the engine is off or idle, and the excavator does not drive.

## Source schema

`src/lakeforge/source/schema.sql` is the only DDL.

- `source.vehicles`
- `source.telemetry_events`, with a foreign key to `source.vehicles`

Compose mounts that file at `/docker-entrypoint-initdb.d/01-schema.sql`. The loader splits and executes the same file. `CREATE ... IF NOT EXISTS` makes a second apply safe. Init scripts run only on a new data volume. After a schema change, recreate the volume:

```powershell
docker compose down -v
docker compose up -d --wait
```

There is no migration tool in this milestone.

## Landing layout

```
data/landing/run_id=<run_id>/
    manifest.json
    vehicles.jsonl
    telemetry_events.jsonl
```

JSONL objects use a stable key order. Timestamps are UTC ISO-8601 with a `+00:00` offset and second precision. `manifest.json` records the run id, pipeline name `m0_source_snapshot`, seed when the pipeline knows it, start, finish, `mode: full_snapshot`, and row counts.

`lakeforge ingest` reads whatever is currently in PostgreSQL. It does not invent a seed. Its manifest stores `seed: null`. `lakeforge pipeline` generates and then extracts, and it records the seed it used.

`data/` is gitignored.

## Logging

Standard-library logging writes one JSON object per line to stdout. Pipeline lines include `run_id` and `row_count`. The database password is a `SecretStr` and is not written to logs by the application.

## What is deliberately absent

No incremental watermark, deduplication, aggregation, data-quality quarantine, or medallion naming. The generator emits valid rows only. Anomaly injection is left for a later milestone. The generator already takes a seed and counts so that work can be added without replacing the module.
