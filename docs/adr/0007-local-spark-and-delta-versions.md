# ADR 0007: Local Spark and Delta versions

## Status

Accepted

## Context

Milestone 2 stores one landing run as Delta tables. The loader runs on the developer machine and in GitHub Actions. It does not need a cluster. Python stays on 3.12, which Spark 4.2 supports.

Delta and Spark patch releases have already broken each other inside one minor line. The pair has to be pinned to versions that declare support for each other.

## Decision

- PySpark 4.2.0 and delta-spark 4.4.1, pinned exactly.
- Java 17 in GitHub Actions (Temurin). Spark 4.2 also runs on Java 21 and 25. A local Temurin 21 install is acceptable.
- One `SparkSession` at `local[1]`, with the Delta catalog, the Spark UI disabled, and the session time zone set to UTC.
- Jars come from the installed `delta-spark` package through `configure_spark_with_delta_pip`. Compose does not gain a Spark service.

On Windows, Hadoop's local filesystem calls `winutils.exe` and a native `access` method that the PySpark package does not ship. LakeForge prepares two local shims and does not commit their binaries:

- If `HADOOP_HOME\bin\winutils.exe` is absent, compile `winutils_stub.cs` with the .NET Framework compiler. The stub accepts `chmod` and reports a fixed mode for `ls`.
- Compile `BareLocalFileSystem` against the Hadoop API jar shipped with PySpark and set `fs.file.impl` to it. Listing and status use `java.io.File`.

Both outputs live under `%LOCALAPPDATA%\lakeforge`. Linux does not use them.

## Consequences

- `uv lock` records the Spark and Delta versions. A later bump is its own change.
- The first session start resolves Delta's Maven dependencies into the local Ivy cache.
- Windows needs either the .NET Framework compiler and `javac`, or a pre-existing `HADOOP_HOME` and a Hadoop build that can read the local disk.
- Silver, partitioning, and performance runs stay later milestones. This session is only the Bronze writer.

## Trade-offs

A host-local session is slower to start than a plain Python process and it needs a JDK. It is the same engine later milestones will use. A separate Spark container would hide that startup cost and add a second runtime to keep compatible.
