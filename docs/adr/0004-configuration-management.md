# ADR 0004: Configuration management

## Status

Accepted

## Context

The local process needs a database location, a landing directory, a log level, and generator defaults. Those values differ between a workstation (PostgreSQL on port 54329) and GitHub Actions (PostgreSQL on port 5432). Secrets must not be committed. Milestone 0 has no Azure Key Vault and no application service.

## Decision

Load typed settings with `pydantic-settings` from `LAKEFORGE_*` environment variables, then an optional `.env` file, then defaults in code.

- `.env.example` is committed and documents every variable.
- `.env` is gitignored.
- The database password is a `SecretStr` and is omitted from `repr`.
- Docker Compose uses the same variable names, with the same defaults, for the PostgreSQL container.
- The local password `lakeforge` is a dummy for synthetic data. It is not a secret and must not be reused for any real system.
- No YAML hierarchy, settings module tree, or secret store is introduced.

## Alternatives

- Reading `os.environ` directly avoids a dependency. It also scatters parsing and produces worse errors when a port or a count is not an integer.
- A YAML config file is easy to read and easy to accidentally commit with a real password later. Environment variables match the GitHub Actions service and Compose without a second format.

## Consequences

- Invalid configuration fails at process start.
- Tests build `Settings` with `_env_file=None` so a developer's `.env` does not leak into unit tests. Environment variables still apply, which is how CI selects port 5432.
- Cloud secret stores remain a later milestone. This ADR does not block them. A future settings source can populate the same environment variables.

## Trade-offs

`pydantic-settings` is a real dependency for a small configuration surface. It is justified because the values are already typed for mypy and the failure mode is a validation error instead of a string passed into a driver. A hand-rolled settings object would be smaller and would reimplement that validation.
