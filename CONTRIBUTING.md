# Contributing

LakeForge is an open-source reference architecture. The current local slice is a Python operational source, a JSONL landing zone, and Bronze, Silver, and Gold Delta tables. Later milestones are documented as planned and are not implemented until their own work starts.

## Setup

Install [uv](https://docs.astral.sh/uv/), Java 17 or 21, and Docker Desktop. From the repository root:

```powershell
uv python install 3.12
uv sync
Copy-Item .env.example .env
docker compose up -d --wait
```

The Compose password is a local dummy for synthetic data. Do not reuse it, and do not commit `.env`.

## Checks

```powershell
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
```

`uv run ruff format .` applies the formatter.

Integration tests expect PostgreSQL. They skip when the server cannot be reached, with the command `docker compose up -d --wait`. Authentication and schema errors fail the test. GitHub Actions runs the integration tests against a PostgreSQL 17.11 service and must not skip them.

## Scope

Keep production logic in `src/lakeforge`. Do not commit generated landing files or large synthetic datasets. Do not introduce a later-milestone technology inside an unrelated change.

## License

This project is licensed under the MIT License. See `LICENSE`.
