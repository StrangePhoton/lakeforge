# ADR 0010: Gold activity tables

## Status

Accepted

## Context

Silver is a conformed copy of one Bronze run. Milestone 3 needs a business-facing table for that same run. The telemetry feed still contains impossible measurements, so an average of speed or fuel would treat those values as facts.

## Decision

`lakeforge gold` reads one Silver directory and overwrites two Delta tables under `data/gold/run_id=<id>/`.

`vehicle_activity` has one row per Silver vehicle. It carries the vehicle attributes, the count of telemetry rows for that vehicle, the count of those rows with an event time, the first and last event time, and the alarm and work-order counts.

`site_activity` has one row per Silver site. It counts vehicles, telemetry rows whose `site_id` matched, alarms for those vehicles, and work orders for the site.

Telemetry with a null `site_id` stays in Silver and is absent from `site_activity`. There is no row for `VEH-9999`, because that identifier is not a vehicle.

Both tables copy `_landing_run_id`. The job clock is stored only in the Gold manifest. These tables are activity snapshots, not owned data products.

## Consequences

- Gold can be rebuilt from Silver.
- A second build of the same Silver run has the same table contents.
- Counts include the unresolved and invalid readings that Silver kept. Averages and quality scores wait until those readings can be separated.

## Trade-offs

Two narrow tables are enough to show the medallion step. Fleet utilization, energy, and reliability products need quality gates and explicit ownership, which are later milestones.
