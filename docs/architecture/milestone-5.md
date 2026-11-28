# Milestone 5: data contracts and schema evolution

Status: implemented.

The telemetry stream is [Milestone 6](milestone-6.md). This page records the landing contracts.

This milestone checks one landing run against the published contracts before Spark starts. Rows that drift are written out and do not enter Bronze. Silver value rules and Gold are not part of it.

## Flow

```
contracts/
    sites.json
    vehicles.json
    telemetry_events.json
    alarms.json
    work_orders.json
        |
        v
data/landing/run_id=<id>/
        |
        v
lakeforge contract
        |
        v
data/contracts/run_id=<id>/
    manifest.json
    schema_violation.jsonl
```

`schema_violation.jsonl` is written only when at least one row fails. `lakeforge bronze` runs the same check first. A violation prevents the Delta write. `generate`, `ingest`, `pipeline`, `silver`, `gold`, and `quality` do not run this check.

```powershell
uv run lakeforge contract --landing-run data/landing/run_id=<id>
```

`--contracts-dir` defaults to `data/contracts`. The landing run must be a direct child of the configured landing directory.

## Compatibility

The policy name is `backward`. The runtime uses the latest version in each file. Version 1 is the column set Bronze already loaded. Telemetry version 2 adds optional `payload_kg`. The generator does not emit that field, so current landing files still pass and Bronze stores it as null.

A missing optional field is valid. An unknown field, a missing required field, a null required field, and a value of the wrong JSON type are violations. The decision and the reason the generator stays unchanged are in [ADR 0012](../adr/0012-schema-evolution.md).

## What is deliberately absent

No owners, classifications, freshness policies, contract registry, schema registry, streaming evolution, or promotion of Gold onto the rows that pass the value rules.
