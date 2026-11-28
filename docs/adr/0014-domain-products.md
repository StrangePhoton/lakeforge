# ADR 0014: Domain products

## Status

Accepted

## Context

Silver holds the conformed snapshot, including telemetry that fails the fleet quality rules. Gold counts that unfiltered snapshot. Quality writes the telemetry rows that pass beside the rows that fail, and it records reference-check counts for alarms and work orders. A trusted activity product needs the passed rows. The alarm rows can be published when those reference checks are clean. The dataset can support vehicle counts, site counts, and alarm rows. It cannot support a daily series, an energy model, or a prediction.

## Decision

`lakeforge product` reads one Silver run and the quality run with the same `landing_run_id`. The promotion gate runs before Spark and before the product directory is created. The quality manifest must list every telemetry rule and every reference rule as an integer. `telemetry_passed` must exist. Any reference-rule count above zero raises `quality run has reference violations` and writes nothing.

Three products are published under `data/products/run_id=<id>/`:

- `fleet.vehicle_activity` is one row per Silver vehicle. Event counts come from `telemetry_passed`. The columns match Gold `vehicle_activity`, and the aggregation is the same function.
- `operations.site_activity` is one row per site. Event counts come from passed telemetry whose `site_id` is set. The columns match Gold `site_activity`.
- `maintenance.alarm_events` is the Silver alarm rows. They are published because the reference checks are clean. The command does not score or filter individual alarms.

Checked-in descriptors in `products/` name each product, its domain, version 1, columns, quality requirements, owner, freshness, classification, and compatibility. The owner is the domain name. Freshness is `snapshot`. Classification is `synthetic`. Compatibility is `backward`. The loader refuses a descriptor that drifts from those fields.

Gold is not read and is not rewritten. The stream table is not an input. Generate, ingest, pipeline, contract, bronze, silver, gold, quality, publish, and stream do not write products.

## Consequences

- A tiny run with the anomaly catalog publishes 4 vehicle rows, 2 site rows, and 5 alarm rows.
- Passed event counts are 20, 21, 21, and 22 for VEH-0001 through VEH-0004. Site Alpha has 41 passed events. Site Bravo has 43.
- Gold vehicle event counts stay 24, 23, 24, and 24 on the unfiltered path.
- A reference failure blocks every product for that run, including the vehicle and site tables.

## Trade-offs

Reusing the Gold aggregations keeps the product columns identical to the activity snapshot. The cost is that the product is a filtered count of the same grain, not a new model. Gating on the whole quality run is simple to test. The cost is that one broken alarm reference withholds the fleet products as well. Naming the owner as the domain records who the product is for without inventing a person, a team, or a catalog service.
