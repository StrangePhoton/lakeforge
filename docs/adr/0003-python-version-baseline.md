# ADR 0003: Python version baseline

## Status

Accepted

## Context

The interpreter chosen in Milestone 0 will be the baseline until a later ADR changes it. Milestone 2 adds PySpark. As of 2026-10-05:

- Python 3.12.15 is the current 3.12 security release. Security support for 3.12 continues until October 2028.
- Python 3.13 is in security-fix-only maintenance.
- Python 3.14 is the current feature release.
- PySpark 4.2 documents support for Python 3.10 and above.
- Python 3.14 support was added in Spark 4.1.

## Decision

Target CPython 3.12 only.

- `.python-version` contains `3.12`.
- `requires-python` is `>=3.12,<3.13`.
- CI and local setup install Python 3.12 through uv.
- The implementation environment resolved CPython 3.12.15.

## Alternatives

- Python 3.13 is mature and still supported. It does not buy a capability Milestone 0 or the planned Spark milestone lacks on 3.12.
- Python 3.14 has the longest remaining support and Spark 4.1+ can run it. Adopting it now would make the first lakehouse milestone depend on the newest interpreter as well as the newest Spark line.

## Consequences

- Packages and syntax stay within the 3.12 language.
- A move to 3.13 or 3.14 is a deliberate upgrade with its own ADR, not a side effect of a later milestone.
- uv installs the interpreter, so a preinstalled system Python is optional. The machine used for implementation also had Python 3.13 on `PATH`; the project does not use it.

## Trade-offs

3.12 is not the newest interpreter, and its security support ends earlier than 3.14. The gain is a boring, widely cached baseline that does not force an interpreter migration when Spark arrives. Novelty of the language runtime is not a goal of this project.
