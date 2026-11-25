# ADR 0008: Bronze Delta snapshot

## Status

Accepted

## Context

The JSONL landing run is the extract from the operational source. Milestone 2 needs a Delta copy that later Silver work can read. The copy has to stay raw enough that a bad telemetry value is still present, and deterministic enough that loading the same landing run twice matches.

## Decision

`lakeforge bronze` reads one landing directory and overwrites five Delta tables under `data/bronze/run_id=<id>/`:

- `sites`
- `vehicles`
- `telemetry_events`
- `alarms`
- `work_orders`

The landing command stays separate. `generate`, `ingest`, and `pipeline` do not start Spark.

Each table uses an explicit schema aligned with the landing fields. Timestamps and dates are parsed from the landing strings. Numbers are doubles. The only added column is `_landing_run_id`. The job clock is stored in the Bronze `manifest.json`, not on the rows, so a second load of the same files has the same table contents.

A value that cannot be parsed, or a null in a required field, fails the load. Rows are not dropped and there is no quarantine path. Null event times, null vehicle ids, and unresolved vehicle ids remain stored when the landing file contains them.

`anomalies.json` stays in the landing run. It is not a Delta table.

Spark's JSON reader produces nullable columns, so the written Delta metadata does not mark required fields non-nullable. The loader still counts required nulls and fails before the write.

## Consequences

- Bronze can be rebuilt from a landing run. It does not read PostgreSQL itself.
- A different landing run id gets its own Bronze directory. Runs are not appended into one long-lived table yet.
- Quality rules, deduplication, and business aggregates begin in later milestones. This table is the typed raw snapshot.

## Trade-offs

Parsing timestamps in Bronze makes the table usable. It also means a landing file with a malformed timestamp cannot land. That failure is a schema boundary. Quarantine of the bad row is Milestone 4.
