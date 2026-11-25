# ADR 0009: Silver conformed snapshot

## Status

Accepted

## Context

Bronze is a typed copy of one landing run, including telemetry that names an unknown vehicle or omits a field. Milestone 3 needs a stable table grain that Gold can aggregate. Quality rules and quarantine are a later milestone, so this step cannot drop those rows.

## Decision

`lakeforge silver` reads one Bronze directory and overwrites five Delta tables under `data/silver/run_id=<id>/`.

Sites, vehicles, alarms, and work orders keep the Bronze columns. Telemetry keeps those columns and gains `site_id`, filled from a left join to vehicles. A null vehicle id, or a vehicle id that is not in the vehicle table, leaves `site_id` null. `VEH-9999` stays on the telemetry row.

The grain is the business key: `site_id`, `vehicle_id`, `event_id`, `alarm_id`, or `work_order_id`. Identical rows for one key collapse to one row. Two different payloads for one key fail the load. The injected reading `EVT-DUP-0001` has its own event id, so it remains.

`_landing_run_id` is copied from Bronze. The job clock is stored only in the Silver manifest. `anomalies.json` is not read, and no row is labeled.

## Consequences

- Silver can be rebuilt from Bronze. It does not read PostgreSQL or the JSONL landing files.
- A second build of the same Bronze run has the same table contents.
- Invalid measurements, missing event times, and unresolved vehicles are still present for a later quality milestone.

## Trade-offs

Deduplicating only exact payloads avoids hiding a conflicting correction. It also leaves the duplicate reading in place, so a Gold event count includes it until a quality rule says otherwise.
