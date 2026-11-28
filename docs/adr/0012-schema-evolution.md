# ADR 0012: Schema evolution

## Status

Accepted

## Context

Bronze already parses a fixed set of landing columns. A later producer can add a measurement without a place to record that the old files are still valid and the new field is optional. Schema drift has to be visible before Spark starts, and it has to stay separate from the Silver value rules.

## Decision

Checked-in contracts live in `contracts/`, one JSON file per landing dataset. Each file keeps every published version. The compatibility policy is `backward`. The runtime check uses the latest version.

Version 1 matches the columns Bronze already loaded. Telemetry version 2 adds one optional double, `payload_kg`. The generator does not emit it. A current landing file still passes because the field is optional. Bronze, Silver, and the quality tables carry `payload_kg` as null until a producer sends it. Gold does not aggregate it, and quality does not add a rule for it.

`lakeforge contract` reads one landing run and writes `data/contracts/run_id=<id>/manifest.json`. A failing row is appended to `schema_violation.jsonl`. A later clean check of that run removes a stale violation file. The command returns success when violations exist so the report can be inspected.

`lakeforge bronze` runs the same check before it logs a start and before it opens a Spark session. Any violation raises and no Delta table is written.

An optional field may be added. A missing or null optional value is valid. A required field may not be removed, renamed, or made optional in the version the reader accepts. An unknown field, a missing required field, a null required field, and a value that is not the declared JSON type are violations. Timestamps and dates must match the patterns Bronze already parses. `anomalies.json` is not a contracted dataset.

## Consequences

- The same landing run produces the same contract report.
- A row can record more than one violation.
- Current generator output passes version 2 with `payload_kg` stored as null.
- An explicit check against version 1 rejects a row that contains `payload_kg`.

## Trade-offs

Keeping every version in the file makes the one compatible change reviewable. The cost is that the catalog is a set of JSON files in the repo, not a registry. Checking structure in Python, before Spark, keeps a drifted file out of Bronze. Value defects such as an impossible speed remain a Silver quality concern.
