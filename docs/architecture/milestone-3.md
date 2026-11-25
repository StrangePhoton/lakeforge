# Milestone 3: Silver and Gold

Status: implemented.

This milestone conforms one Bronze snapshot and aggregates it. Data quality, quarantine, contracts, and streaming are not part of it.

## Flow

```
data/bronze/run_id=<id>/
        |
        v
lakeforge silver
        |
        v
data/silver/run_id=<id>/
        |
        v
lakeforge gold
        |
        v
data/gold/run_id=<id>/
    vehicle_activity/
    site_activity/
    manifest.json
```

`pipeline` and `bronze` do not write these tables.

```powershell
uv run lakeforge pipeline --profile tiny
uv run lakeforge bronze --landing-run data/landing/run_id=<id>
uv run lakeforge silver --bronze-run data/bronze/run_id=<id>
uv run lakeforge gold --silver-run data/silver/run_id=<id>
```

`--silver-dir` defaults to `data/silver`. `--gold-dir` defaults to `data/gold`. The run directory must be a direct child of the configured input directory. Spark scratch files stay outside the snapshot.

## Silver

The five operational tables keep the Bronze columns and `_landing_run_id`. Telemetry adds `site_id` from the vehicle table. Unknown and null vehicle ids leave `site_id` null.

An identical repeated business key collapses to one row. A repeated key with two different payloads fails the build. Tiny with anomalies stays at 2 sites, 4 vehicles, 97 telemetry rows, 5 alarms, and 1 work order, including `EVT-DUP-0001`.

## Gold

`vehicle_activity` has one row per vehicle. `site_activity` has one row per site. For Tiny with anomalies the vehicle event counts are 24, 23, 24, and 24. Site event counts are 48 and 47. The two telemetry rows without a known vehicle are not in those site counts.

Gold counts events, alarms, and work orders. It does not average speed, fuel, or position.

## What is deliberately absent

No quarantine, quality score, dropped measurement, contract, schema evolution, streaming, merge, compaction, or data-product catalog. The decisions are in [ADR 0009](../adr/0009-silver-conformed-snapshot.md) and [ADR 0010](../adr/0010-gold-activity-tables.md).
