# ADR 0006: Simulation anomaly catalog

## Status

Accepted

## Context

Later data-quality work needs a known set of bad telemetry readings so its results can be scored. If those readings are labeled on the operational row, the quality job can pass by reading the label. The label has to stay outside the source tables.

## Decision

Generate the operational story first: sites, vehicles, clean telemetry, alarms derived from that telemetry, and work orders derived from `SERVICE_DUE`. Then apply a fixed list of telemetry defects. The list does not depend on the seed. A defect whose target event is absent from the selected window is skipped.

Record each applied defect in `anomalies.json` inside the landing run:

- `kind`
- `table` (`telemetry_events`)
- `record_id`
- `field`

`lakeforge pipeline` writes that file, including an empty array when `--no-anomalies` is set. `lakeforge ingest` does not write it, because a plain extract of the database does not know the answer key.

Operational JSONL rows do not contain `is_anomaly` or any equivalent label.

The controlled kinds are duplicate, null position, null speed, null event time, null vehicle, unresolved vehicle reference, impossible speed, impossible latitude, impossible longitude, invalid engine state, late event, and out-of-order receipt. Malformed payloads, schema evolution, quarantine, and a data-quality engine are deferred.

## Consequences

- Quality processing can be evaluated against `anomalies.json` without the operational files revealing the answer.
- Alarms and work orders describe the clean story. A later defect may change the telemetry row an alarm points at. The alarm row itself is not rewritten.
- The catalog is simulation metadata. It is not a sixth operational entity.
