# Milestone 4: data quality and quarantine

Status: implemented.

Schema checks on the landing run are [Milestone 5](milestone-5.md). This page records the Silver quality rules.

This milestone checks one Silver snapshot and writes the telemetry rows that fail beside the rows that pass. Streaming is not part of it.

## Flow

```
data/silver/run_id=<id>/
        |
        v
lakeforge quality
        |
        v
data/quality/run_id=<id>/
    telemetry_violation/
    telemetry_passed/
    manifest.json
```

Silver and Gold are not rewritten. `pipeline`, `bronze`, `silver`, and `gold` do not run these rules.

```powershell
uv run lakeforge quality --silver-run data/silver/run_id=<id>
```

`--quality-dir` defaults to `data/quality`. The Silver run must be a direct child of the configured Silver directory.

## Rules

A failed rule adds one violation row. The event is excluded from `telemetry_passed`. The manifest records a count for every rule, including zeros.

Tiny with anomalies quarantines 13 events and passes 84. The 12 catalog ids are in the violation table. The order rule also quarantines `EVT-0001-000014`, the other side of the swapped receipt. `EVT-0001-000010` stays, and `EVT-DUP-0001` is the duplicate violation.

Reference checks on alarms and work orders pass for this generator, so no `reference_violation` table is written. The rule list and the decision to leave Gold unfiltered are in [ADR 0011](../adr/0011-telemetry-quality-rules.md).

## What is deliberately absent

No general rules engine, streaming quarantine, or rebuild of Gold from the passed rows. Schema contracts are [Milestone 5](milestone-5.md).
