# ADR 0011: Telemetry quality rules

## Status

Accepted

## Context

Silver keeps every conformed telemetry row, including readings the simulator damaged on purpose. Those rows have to become visible without using `anomalies.json` as an input, and without removing them from Silver. Gold still counts the full Silver snapshot.

## Decision

`lakeforge quality` reads one Silver directory and overwrites two Delta tables under `data/quality/run_id=<id>/`:

- `telemetry_violation`, one row per failed rule, with the Silver telemetry columns plus `rule_id`
- `telemetry_passed`, the events that failed no rule

The rules are fixed for this fleet feed:

- `vehicle_id` is present
- a present `vehicle_id` exists in Silver vehicles
- `event_time` is present
- latitude and longitude are present
- a present latitude is inside -90..90 and a present longitude is inside -180..180
- `speed_kph` is present and, when present, is inside 0..80
- `engine_state` is `off`, `idle`, or `working`
- `event_time` is no more than one hour before `received_at`
- one row per `vehicle_id` and `event_time`; the lexicographically smallest `event_id` stays
- within a vehicle, after late rows are skipped, `received_at` does not decrease as `event_time` increases. Both rows in a decrease are violations

Alarms and work orders are checked for a known vehicle, site, and alarm. Zero failures produce no extra table. A failure is written to `reference_violation` and does not stop the command.

`anomalies.json` is not read. Silver and Gold are not rewritten.

## Consequences

- The same Silver run produces the same violation and passed tables.
- A row can appear more than once in `telemetry_violation` when it fails more than one rule. It is absent from `telemetry_passed` after the first failure.
- Gold activity counts still include the quarantined readings.

## Trade-offs

Leaving Gold unfiltered keeps the quality gate separate from the activity snapshot. The cost is that Gold is not yet a promoted table. Speed above 80 kph is rejected even though this simulator's clean readings stay at or below 40. The wider bound is a fleet limit, not a copy of the generator cap.
