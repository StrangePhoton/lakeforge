# Milestone 7: domain products

Status: implemented.

This milestone publishes three domain products from one Silver run and its quality output. A product is written only when that quality run is clean enough to trust. Gold stays the unfiltered activity snapshot.

## Flow

```
data/silver/run_id=<id>/
data/quality/run_id=<id>/
        |
        v
lakeforge product
        |
        v
data/products/run_id=<id>/
    fleet_vehicle_activity/
    operations_site_activity/
    maintenance_alarm_events/
    manifest.json
```

```powershell
uv run lakeforge product --silver-run data/silver/run_id=<id> --quality-run data/quality/run_id=<id>
```

`--products-dir` defaults to `data/products`. The descriptors live in `products/` at the repository root.

## Products

| Product | Domain | Rows | Counts from |
| --- | --- | --- | --- |
| `fleet.vehicle_activity` | fleet | one per Silver vehicle | `telemetry_passed` |
| `operations.site_activity` | operations | one per site | passed telemetry with a site |
| `maintenance.alarm_events` | maintenance | the Silver alarm rows | reference checks with zero failures |

Fleet and operations columns match the Gold activity tables. Maintenance columns are the Silver alarm columns plus `_landing_run_id`. Each descriptor records version 1, compatibility `backward`, freshness `snapshot`, classification `synthetic`, and an owner equal to the domain name. Fleet and operations require the telemetry rules. Maintenance requires the reference rules.

The promotion gate reads the quality manifest and `telemetry_passed` before Spark starts. A reference-rule count above zero raises and does not create `data/products/run_id=<id>/`.

On the tiny profile with the anomaly catalog, the products are:

| Vehicle | Events | Timed | Alarms | Work orders |
| --- | --- | --- | --- | --- |
| VEH-0001 | 20 | 20 | 2 | 1 |
| VEH-0002 | 21 | 21 | 1 | 0 |
| VEH-0003 | 21 | 21 | 1 | 0 |
| VEH-0004 | 22 | 22 | 1 | 0 |

Site Alpha has 2 vehicles, 41 events, 3 alarms, and 1 work order. Site Bravo has 2 vehicles, 43 events, 2 alarms, and 0 work orders. The alarm product has 5 rows. VEH-0003 `first_event_time` is not the quarantined late instant. Gold's unfiltered vehicle event counts stay 24, 23, 24, and 24.

The decision is in [ADR 0014](../adr/0014-domain-products.md).

## What this uses from Data Mesh

The three products are domain-oriented: fleet, operations, and maintenance. Each one is data as a product: a name, a schema, a version, a compatibility policy, and a quality gate. The promotion check is executable governance. It reads `telemetry_passed` and refuses a run with reference failures.

A federated ownership organization, a self-serve data platform, independent domain pipelines, and a global catalog service are not part of this milestone.

## What is deliberately absent

No daily grain, no fuel or speed averages, no reliability score, no predictive features, no Purview, no retention policy, no lineage service, no product API, and no ForgeFlow. Gold is not rebuilt from `telemetry_passed`. The stream table is not a product input.
