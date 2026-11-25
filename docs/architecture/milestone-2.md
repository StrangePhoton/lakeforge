# Milestone 2: local Spark and Bronze Delta

Status: implemented.

Silver and Gold for this Bronze snapshot are [Milestone 3](milestone-3.md). This page records the local Spark session and the Bronze Delta load.

This milestone writes one JSONL landing run into local Delta tables. Silver, Gold, data quality, and streaming are not part of it.

## Flow

```
lakeforge pipeline
        |
        v
data/landing/run_id=<id>/*.jsonl
        |
        v
lakeforge bronze
        |
        v
data/bronze/run_id=<id>/
    manifest.json
    sites/
    vehicles/
    telemetry_events/
    alarms/
    work_orders/
```

`generate`, `ingest`, and `pipeline` are unchanged and do not start Spark. Bronze is a separate command:

```powershell
uv run lakeforge pipeline --profile tiny --landing-dir data/landing
uv run lakeforge bronze --landing-run data/landing/run_id=<id>
```

The landing run must be a direct child of the configured landing directory. `--bronze-dir` defaults to `data/bronze`. Spark scratch files stay outside that snapshot.

## Tables

Each directory is a Delta table. Loading the same landing run again overwrites those tables. The columns match the landing files, plus `_landing_run_id`. Operational rows are not labeled as anomalies. `anomalies.json` remains only in the landing run.

Tiny with anomalies is 2 sites, 4 vehicles, 97 telemetry rows, 5 alarms, and 1 work order.

Timestamps are parsed as UTC instants. A string that is not a timestamp fails the load. Spark's JSON reader still records the Delta columns as nullable; the loader rejects a null required field before writing.

## Runtime

PySpark 4.2.0 and delta-spark 4.4.1 run in a local session. GitHub Actions installs Temurin 17. Details and the Windows filesystem shims are in [ADR 0007](../adr/0007-local-spark-and-delta-versions.md). The table contract is in [ADR 0008](../adr/0008-bronze-delta-snapshot.md).

## What is deliberately absent

No Silver or Gold tables, quality checks, quarantine, contracts, schema evolution, streaming, merge, compaction, partition benchmarks, or a Spark cluster.
