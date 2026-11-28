# Milestone 6: streaming with Kafka

Status: implemented.

The domain products are [Milestone 7](milestone-7.md). This page records the telemetry stream.

This milestone publishes one landing telemetry file to a local Kafka broker and loads it with Structured Streaming. The batch snapshot, Silver, Gold, and the value-quality rules stay as they are. Event Hubs are not part of it.

## Flow

```
data/landing/run_id=<id>/telemetry_events.jsonl
        |
        v
lakeforge publish
        |
        v
Kafka  lakeforge.telemetry.events
        |
        v
lakeforge stream
        |
        v
data/stream/
    telemetry_events/
    checkpoint/telemetry_events/
    schema_violation.jsonl
    manifest.json
```

`schema_violation.jsonl` is written only when a Kafka record fails the telemetry contract. `line` in that file is the Kafka offset.

```powershell
uv run lakeforge publish --landing-run data/landing/run_id=<id>
uv run lakeforge stream
```

`--kafka-bootstrap` defaults to `localhost:19092`. `--stream-dir` defaults to `data/stream`. Compose publishes the broker on port 19092. The image is `apache/kafka:4.4.0`.

## Delivery

The producer can deliver a record more than once. The stream merges on `event_id`, so a second copy updates the same row. A second `lakeforge stream` on the same checkpoint reads only offsets that were not committed. The tiny feed has 97 telemetry rows, including `EVT-DUP-0001` and the late event `EVT-0003-000012`. Both stay in the table. The query reads at most 40 offsets per micro-batch and then stops.

The decision is in [ADR 0013](../adr/0013-telemetry-stream.md).

## What is deliberately absent

No Event Hubs, no second broker, no watermark, no schema registry, and no rebuild of Silver or Gold from the stream table.
