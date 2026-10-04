"""Shared fixtures for unit and PostgreSQL integration tests."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from psycopg import Connection, OperationalError

from lakeforge.config import Settings
from lakeforge.source.postgres import apply_schema, connect, truncate_source


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(_env_file=None, landing_dir=tmp_path / "landing")


@pytest.fixture
def source_connection(settings: Settings) -> Iterator[Connection[tuple[object, ...]]]:
    try:
        connection = connect(settings)
    except OperationalError as exc:
        if exc.sqlstate is not None and not exc.sqlstate.startswith("08"):
            raise
        pytest.skip("PostgreSQL is unavailable. Start it with `docker compose up -d --wait`.")
    apply_schema(connection)
    truncate_source(connection)
    try:
        yield connection
    finally:
        truncate_source(connection)
        connection.close()
