# ADR 0002: Milestone 0 source system and landing format

## Status

Accepted for the Milestone 0 slice.

Superseded in part by [ADR 0005](0005-operational-source-and-telemetry-feed.md): the two-table model, the telemetry foreign key, and the requirement to recreate the Compose volume before the Python loader can apply a schema change. The single schema file, PostgreSQL 17.11 simulator, full-snapshot JSONL landing, and host port 54329 remain in force. The decision text below is unchanged.

## Context

Milestone 0 needs one working path from synthetic operational data to an inspectable result. The lakehouse format is intentionally not chosen yet. The source still has to look like an operational system that later bronze ingestion can read, and the output has to be something a later Spark job can consume without rewriting the extract.

## Decision

- Simulate the operational source in PostgreSQL 17.11, image tag `postgres:17.11`.
- Keep one schema file, `src/lakeforge/source/schema.sql`. Docker Compose mounts that file into `docker-entrypoint-initdb.d`. The Python loader executes the same file.
- Model two tables in schema `source`: `vehicles` and `telemetry_events`.
- Ingest a full snapshot. There is no watermark.
- Write each run to `data/landing/run_id=<run_id>/` as JSONL plus `manifest.json`.
- The landing directory is gitignored. Tests write to a temporary directory.
- The Python process runs on the host. Compose runs only PostgreSQL, published on host port 54329.

PostgreSQL 18 was available and was not selected. Its image changes the data-directory mount path. PostgreSQL 17 is a current supported stable release and avoids that trap for a disposable local simulator.

## Alternatives

- Parquet would be closer to a columnar landing file and would require PyArrow before Spark is in the project. JSONL can be opened in an editor, and Spark can read it when the lakehouse work starts.
- A second PostgreSQL schema for "ingested" rows would make the extract easy to query and would blur the boundary between the operational source and the landing zone.
- Calling the JSONL directory `bronze` would imply a medallion layer that does not exist yet.

## Consequences

- A successful run is a directory of JSONL files and a manifest with `run_id`, row counts, and `mode: full_snapshot`.
- Re-running ingestion creates a new run directory. Re-running generation with the same seed replaces the source rows with the same records.
- Schema changes are not migrated. Recreate the Compose volume with `docker compose down -v` so the init script and the Python loader stay on one definition.
- Milestone 2 can add a Delta writer beside the JSONL writer. The extract stays a full snapshot of source columns.

## Trade-offs

JSONL is larger and untyped compared with Parquet, and it is not a table format. That is the right limitation for this milestone. A generic source/sink interface was considered and rejected. There is one reader and one writer, which is enough to replace the writer later without an unused plugin system.
