# ADR 0013: Telemetry stream

## Status

Accepted

## Context

Telemetry is an external event feed. The batch path already stores one full landing snapshot in Bronze. A streaming path has to land the same feed without replacing that snapshot, and a second delivery of the same event must not become a second row. Kafka writes are at-least-once. Spark 4.2 documents the same limit for its Kafka sink: a successful write can still deliver a record more than once, and the reader removes duplicates with its own key.

## Decision

Compose runs one KRaft broker, `apache/kafka:4.4.0`, on host port 19092. The listener block is the full single-node set from the Apache 4.4.0 Compose example, with the host listener advertised as `localhost:19092`.

`lakeforge publish` reads `telemetry_events.jsonl` from one landing run. The contract check runs first. A violation raises before any record is sent. Each remaining line becomes one Kafka record on `lakeforge.telemetry.events`. The key is `event_id`. The value is the JSON object. The producer waits for `acks=all`.

`lakeforge stream` reads that topic with Structured Streaming and `Trigger.AvailableNow`, then stops. `maxOffsetsPerTrigger` is 40. A new checkpoint starts at `earliest`. A later run of the same checkpoint continues from the offsets already committed. Each micro-batch is checked with the telemetry contract. A failing record is appended to `data/stream/schema_violation.jsonl`, where `line` is the Kafka offset. Accepted records are parsed with the Bronze telemetry parsers and merged into `data/stream/telemetry_events` on `event_id`.

The topic has one partition, so the stored order is the publish order. There is no watermark. The late reading stays in the table. `EVT-DUP-0001` has its own event id, so the merge keeps it. A redelivery of an existing event id updates that row.

The Spark connector is `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`, loaded beside the Delta jars. There is no second Kafka client library. Sites, vehicles, alarms, work orders, and the batch Bronze tables stay on the snapshot path. Silver and Gold are not rebuilt from the stream.

## Consequences

- The same landing file can be published twice and the stream table still has one row per event id.
- A checkpoint restart does not append the rows already merged.
- A contract failure in one Kafka record stays out of the Delta table.
- The batch Bronze count for a tiny run stays 2/4/97/5/1.

## Trade-offs

Stopping after the available data makes the stream testable. The cost is that this command is a catch-up, not a process that waits for the next event. Leaving Silver and Gold on the batch snapshot keeps the stream from changing activity counts. The cost is that the stream table is not yet a medallion input. One broker and one partition are enough to show ordering, replay, and idempotency. They are not a cluster.
