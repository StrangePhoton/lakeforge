from pathlib import Path

from lakeforge.source.postgres import (
    SCHEMA_PATH,
    TELEMETRY_COLUMNS,
    VEHICLE_COLUMNS,
    split_sql_script,
)


def test_compose_and_loader_share_one_schema_file() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = root / "src" / "lakeforge" / "source" / "schema.sql"
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")

    assert SCHEMA_PATH.resolve() == schema.resolve()
    assert "src/lakeforge/source/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro" in compose
    text = schema.read_text(encoding="utf-8")
    for column in (*VEHICLE_COLUMNS, *TELEMETRY_COLUMNS):
        assert column in text
    assert text.count("CREATE TABLE") == 2


def test_split_sql_script_ignores_comments() -> None:
    script = "-- comment\nCREATE SCHEMA source;\n\n-- tail\nSELECT 1;\n"
    assert split_sql_script(script) == ["CREATE SCHEMA source", "SELECT 1"]
